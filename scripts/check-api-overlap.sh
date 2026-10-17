#!/usr/bin/env bash
# Checks that the crate's copy of the fork API and the API that the pinned sdk-rust vendors share
# no file path and no message, enum or service full name. A lang loads both trees into one
# descriptor pool, and a second definition of a name or a path fails that load. CI runs it.
#
#   scripts/check-api-overlap.sh
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
common=$(cd "$root" && cargo metadata --format-version 1 | python3 -c '
import json, sys
meta = json.load(sys.stdin)
print(next(p["manifest_path"] for p in meta["packages"] if p["name"] == "temporalio-common"))')
vendored="$(dirname "$common")/../protos/protos/api_upstream"
[ -d "$vendored/temporal/api" ] || { echo "no api_upstream next to $common" >&2; exit 1; }

python3 - "$vendored" "$root/crates/streams/protos/api_stream" \
  "$root/crates/streams/protos/api_nexus" <<'EOF'
import pathlib, re, sys

def names(path):
    """The full names of the top-level messages, enums and services of one proto file."""
    text = re.sub(r"//[^\n]*", "", path.read_text())
    package = re.search(r"^\s*package\s+([\w.]+)\s*;", text, re.M).group(1)
    found, depth = [], 0
    for token in re.finditer(r"\b(message|enum|service)\s+(\w+)|[{}]", text):
        if token.group(0) == "{":
            depth += 1
        elif token.group(0) == "}":
            depth -= 1
        elif depth == 0:
            found.append(f"{package}.{token.group(2)}")
    return found

vendored = pathlib.Path(sys.argv[1])
theirs = {p.relative_to(vendored).as_posix(): p for p in vendored.rglob("*.proto")}
their_names = {n: rel for rel, p in theirs.items() for n in names(p)}
failed = False
for copy in map(pathlib.Path, sys.argv[2:]):
    for path in sorted(copy.rglob("*.proto")):
        rel = path.relative_to(copy).as_posix()
        if rel in theirs:
            print(f"{copy.name}/{rel}: the path is also in sdk-rust's api_upstream")
            failed = True
        for name in names(path):
            if name in their_names:
                print(f"{copy.name}/{rel}: {name} is also in api_upstream/{their_names[name]}")
                failed = True
sys.exit(1 if failed else 0)
EOF
echo "the crate's API copy shares no path and no name with the pinned sdk-rust"
