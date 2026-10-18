#!/usr/bin/env python3
"""Checks that each optional layer's restore commit copies its lines from the stack top.

The notification log, the driver, the archive and the PostgreSQL store left the main chain and
come back in the optional layers, each with one restore commit. Its lines are copies of the
stack top before the move, so they are checked here and not read line by line. A path in a
layer's `whole` list must match the stack top at the restore commit. Every line that a restore
commit adds to any other path must be a line of that path at the stack top, or a line in
`scripts/optional-restore-joins.txt`, where a later layer's part was left out of a joined line.

    scripts/check-optional-restore.py
"""

import subprocess
import sys
from pathlib import Path

SOURCE = "4aebbd4abc292bc7536e0aadfca7fe6660403474"

# Each layer's restore commit and the paths that match the stack top there.
LAYERS = [
    (
        "so1",
        "fd9483e5398e480a2479759f382dbbe03b3d597c",
        [
            "crates/streams/docs/managed-redis.md",
            "crates/streams/scripts/redis-test-servers.sh",
            "crates/streams/src/redis/conn.rs",
            "crates/streams/src/redis/keys.rs",
            "crates/streams/src/redis/log.rs",
            "crates/streams/src/redis/slots.rs",
            "crates/streams/testdata/redis/notify_slots.json",
        ],
    ),
    (
        "so2",
        "34b84590f4102d144695020aa35fd2203de5863d",
        [
            "crates/streams/lua/advance.lua",
            "crates/streams/src/archive/format.rs",
            "crates/streams/src/archive/local.rs",
            "crates/streams/src/archive/mod.rs",
            "crates/streams/src/archive/object.rs",
            "crates/streams/src/handle.rs",
            "crates/streams/src/redis/mod.rs",
            "crates/streams/src/tiered.rs",
            "crates/streams/testdata/archive/names.json",
            "crates/streams/testdata/archive/objects.json",
        ],
    ),
    (
        "so3",
        "6d8efe363a5b71e9f1fd12e75b4c5ea5a73bdf72",
        [
            ".gitattributes",
            "crates/streams/Cargo.toml",
            "crates/streams/docs/archive.md",
            "crates/streams/docs/postgres.md",
            "crates/streams/scripts/postgres-test-server.sh",
            "crates/streams/src/lib.rs",
            "crates/streams/src/postgres/conn.rs",
            "crates/streams/src/postgres/errors.rs",
            "crates/streams/src/postgres/janitor.rs",
            "crates/streams/src/postgres/listen.rs",
            "crates/streams/src/postgres/log.rs",
            "crates/streams/src/postgres/migrations.rs",
            "crates/streams/src/postgres/migrations/0001_init.sql",
            "crates/streams/src/postgres/migrations/0002_notification_log.sql",
            "crates/streams/src/postgres/migrations/0003_archive_hold.sql",
            "crates/streams/src/postgres/migrations/0004_review_fixes.sql",
            "crates/streams/src/postgres/migrations/0005_archive_trim.sql",
            "crates/streams/src/postgres/mod.rs",
            "crates/streams/src/postgres/wait.rs",
            "crates/streams/src/service.rs",
            "crates/streams/tests/driver.rs",
            "crates/streams/tests/postgres_store.rs",
        ],
    ),
]


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True
    ).stdout


def lines_at(commit: str, path: str) -> set[str]:
    try:
        return set(git("show", f"{commit}:{path}").splitlines())
    except subprocess.CalledProcessError:
        return set()


def main() -> int:
    root = Path(__file__).resolve().parent
    joins = set((root / "optional-restore-joins.txt").read_text().splitlines())
    failed = False
    for name, restore, whole in LAYERS:
        for path in whole:
            if subprocess.run(["git", "diff", "--quiet", SOURCE, restore, "--", path]).returncode:
                print(f"{name}: {path} differs from {SOURCE[:8]} at {restore[:8]}")
                failed = True
        changed = git("diff", "--name-only", f"{restore}^", restore).splitlines()
        for path in changed:
            if path in whole:
                continue
            source = lines_at(SOURCE, path)
            diff = git("diff", "-U0", f"{restore}^", restore, "--", path)
            for line in diff.splitlines():
                if line.startswith("+") and not line.startswith("+++"):
                    added = line[1:]
                    if added not in source and added not in joins:
                        print(f"{name}: {path} adds a line {SOURCE[:8]} lacks: {added!r}")
                        failed = True
    if not failed:
        print(f"every restore commit copies {SOURCE[:8]}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
