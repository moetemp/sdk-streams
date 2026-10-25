"""Writes the shared test vectors of the Stream runtime.

This is a second implementation of the layout, in Python, so the crate's tests check the Rust
code against it. Each SDK runs the same JSON files. Run it from this directory:

    python3 gen_vectors.py
"""

import hashlib
import json
from urllib.parse import quote

SEQ_BITS = 20


def part(text):
    return quote(text, safe="")


def join(*parts):
    return ":".join(part(p) for p in parts)


def keys(prefix, namespace, stream_id):
    base = f"{part(prefix)}:{{{join(namespace, stream_id)}}}"
    return {"meta": f"{base}:meta", "log": f"{base}:log"}


def hex64(value):
    return f"{max(value, 0):016x}"


def hex32(value):
    return f"{max(value, 0):08x}"


def task_key(started_event_id=0, attempt=0, started_time=(0, 0)):
    return ".".join(
        [hex64(started_event_id), hex32(attempt), hex64(started_time[0]), hex32(started_time[1])]
    )


def epoch_key(epoch):
    return ".".join(
        [
            task_key(
                epoch.get("task_started_event_id", 0),
                epoch.get("task_attempt", 0),
                tuple(epoch.get("task_started_time", (0, 0))),
            ),
            hex64(epoch.get("attempt_scheduled_time", (0, 0))[0]),
            hex32(epoch.get("attempt_scheduled_time", (0, 0))[1]),
            hex32(epoch.get("attempt", 0)),
        ]
    )


def ids():
    cases = []
    for prefix, namespace, workflow_id, first_run_id, name in [
        ("temporal-streams", "default", "agent-1", "run-1", None),
        ("temporal-streams", "default", "agent-1", "run-1", "turn-3"),
        ("p:x", "ns{1}", "a:b/c", "r%1", "é name"),
    ]:
        if name is None:
            stream_id = join(workflow_id, first_run_id)
        else:
            stream_id = join(workflow_id, first_run_id, name)
        cases.append(
            {
                "prefix": prefix,
                "namespace": namespace,
                "workflow_id": workflow_id,
                "first_run_id": first_run_id,
                "name": name,
                "stream_id": stream_id,
                **keys(prefix, namespace, stream_id),
            }
        )
    producers = {
        "workflow": {"run_id": "run:1", "id": "wf:" + join("run:1")},
        "local_activity": {
            "run_id": "run:1",
            "call_key": "k-1",
            "id": "la:" + join("run:1", "k-1"),
        },
        "activity": {
            "run_id": "run:1",
            "activity_id": "a/1",
            "schedule_key": "s-1",
            "id": "act:" + join("run:1", "a/1", "s-1"),
        },
        "standalone_activity": {
            "activity_id": "sa 1",
            "activity_run_id": "ar-1",
            "id": "sa:" + join("sa 1", "ar-1"),
        },
        "client": {
            "identity": "1@host",
            "process_key": "p-1",
            "id": "client:" + join("1@host", "p-1"),
        },
        "named_client": {"name": "ingest:eu/1", "id": "client:" + join("ingest:eu/1")},
    }
    return {"streams": cases, "producers": producers}


def offsets():
    cases = []
    for entry_id in ["0-1", "1-0", "1700000000000-0", "1700000000000-7", "1760000000123-1048575"]:
        ms, seq = (int(x) for x in entry_id.split("-"))
        cases.append({"entry_id": entry_id, "offset": (ms << SEQ_BITS) | seq})
    return cases


def epochs():
    cases = [
        {"kind": "workflow", "epoch": {"task_started_event_id": 8, "task_attempt": 2,
                                        "task_started_time": [1760000000, 5]}},
        {"kind": "local_activity", "epoch": {"task_started_event_id": 8, "task_attempt": 1,
                                              "task_started_time": [1760000000, 5],
                                              "attempt": 3}},
        {"kind": "activity", "epoch": {"attempt_scheduled_time": [1760000001, 999999999],
                                        "attempt": 2}},
        {"kind": "client", "epoch": {"attempt": 1}},
        {"kind": "empty", "epoch": {}},
    ]
    for case in cases:
        case["key"] = epoch_key(case["epoch"])
        epoch = case["epoch"]
        case["task_key"] = task_key(
            epoch.get("task_started_event_id", 0),
            epoch.get("task_attempt", 0),
            tuple(epoch.get("task_started_time", (0, 0))),
        )
    return cases


def fields():
    run = "run:1"
    producer = "act:" + join(run, "a", "s")
    return {
        "kind": 3,
        "producer_id": producer,
        "run_id": run,
        "fence": f"e|3|{producer}",
        "dedupe": f"d|3|{producer}",
        "holds": f"h|3|{producer}",
        "run_wrote": f"run|{part(run)}",
        "withdrawn_run": f"wr|{part(run)}",
    }


def digest(records):
    """The content digest of a batch, which a dedupe entry keeps."""
    out = hashlib.sha256()

    def part(data):
        out.update(len(data).to_bytes(8, "big"))
        out.update(data)

    for record in records:
        part(record["topic"].encode())
        metadata = record.get("metadata", {})
        metadata = sorted((key.encode(), bytes.fromhex(value)) for key, value in metadata.items())
        out.update(len(metadata).to_bytes(8, "big"))
        for key, value in metadata:
            part(key)
            part(value)
        part(bytes.fromhex(record.get("data", "")))
    return out.hexdigest()


def digests():
    """Batches as hex bytes, so the server and every SDK compute the same digest."""
    cases = [
        {"name": "empty batch", "records": []},
        {"name": "one record, no body", "records": [{"topic": ""}]},
        {"name": "one record", "records": [{"topic": "t", "data": b"hello".hex()}]},
        {
            "name": "metadata in key order",
            "records": [
                {
                    "topic": "turns",
                    "metadata": {"encoding": b"json/plain".hex(), "a": "00ff"},
                    "data": b'{"x":1}'.hex(),
                }
            ],
        },
        {
            "name": "two records",
            "records": [{"topic": "a", "data": "01"}, {"topic": "", "data": "0203"}],
        },
        # The length prefixes give these two different digests, though their joined bytes are equal.
        {"name": "topic and data split one way", "records": [{"topic": "ab", "data": "63"}]},
        {"name": "topic and data split another way", "records": [{"topic": "a", "data": "6263"}]},
    ]
    for case in cases:
        case["digest"] = digest(case["records"])
    assert cases[-1]["digest"] != cases[-2]["digest"]
    return cases


def withdraws(supersession, record):
    """The reader rule of a Superseded item, as the API proposal states it."""
    producer = record["producer"]
    if supersession.get("withdraw_run") and producer.get("run_id") == supersession["withdraw_run"]:
        return True
    # A client's newer epoch only fences. Its earlier Records are real messages.
    if supersession["producer_kind"] == "client":
        return False
    # An item with no epoch, such as a create's, withdraws only a run.
    if "epoch" not in supersession or "epoch" not in producer:
        return False
    old, new = producer["epoch"], supersession["epoch"]
    if supersession["producer_kind"] != "workflow":
        return (
            producer["kind"] == supersession["producer_kind"]
            and producer["id"] == supersession["producer_id"]
            and epoch_key(old) < epoch_key(new)
        )
    if producer["kind"] == "workflow":
        if producer["id"] != supersession["producer_id"]:
            return False
    elif producer["kind"] == "local_activity":
        if producer.get("run_id", "") != supersession["run_id"]:
            return False
    else:
        return False

    def task(epoch):
        return (
            epoch.get("task_started_event_id", 0),
            task_key(
                epoch.get("task_started_event_id", 0),
                epoch.get("task_attempt", 0),
                tuple(epoch.get("task_started_time", (0, 0))),
            ),
        )

    started, old_key = task(old)
    t_started, t_key = task(new)
    c = supersession.get("previous_completed_task")
    c_started = c["started_event_id"] if c else 0
    if started in supersession.get("failed_tasks", []):
        return True
    if c_started < started < t_started:
        return True
    if started == t_started and old_key < t_key:
        return True
    if c and started == c_started:
        c_time = tuple(c.get("started_time", (0, 0)))
        if c.get("attempt", 0) == 0:
            # Core sets attempt 0 when it no longer knows c's attempt. Then the time decides.
            return tuple(old.get("task_started_time", (0, 0))) < c_time
        if old_key < task_key(c["started_event_id"], c["attempt"], c_time):
            return True
    return False


def wf(run, started, attempt=1, time=(100, 0)):
    return {"kind": "workflow", "id": "wf:" + part(run), "run_id": run,
            "epoch": {"task_started_event_id": started, "task_attempt": attempt,
                      "task_started_time": list(time)}}


def withdrawal_examples():
    """Appendix E of the API proposal, one case per Record that a Superseded item meets."""
    r1 = "r1"
    cases = []

    def case(name, supersession, producer, expected):
        record = {"producer": producer}
        assert withdraws(supersession, record) == expected, name
        cases.append({"name": name, "supersession": supersession, "record": record,
                      "withdraws": expected})

    def sup(task, c=None, failed=(), withdraw_run=None, run=r1):
        s = {"producer_kind": "workflow", "producer_id": "wf:" + part(run), "run_id": run,
             "epoch": task["epoch"], "failed_tasks": list(failed)}
        if c:
            s["previous_completed_task"] = c
        if withdraw_run:
            s["withdraw_run"] = withdraw_run
        return s

    def completed(started, attempt=1, time=(100, 0)):
        return {"started_event_id": started, "attempt": attempt, "started_time": list(time)}

    # 1. A retried task writes nothing. B at [5] failed, [8] wrote nothing, C at [11], c = [8].
    s = sup(wf(r1, 11), completed(8), failed=[5])
    case("e1 B at the failed task 5 goes", s, wf(r1, 5), True)
    case("e1 a Record at the completed task 8 stays", s, wf(r1, 8), False)
    case("e1 a Record before c stays", s, wf(r1, 3), False)
    # 2. A refused completion. B at [5] refused, B' at [8] with failed [5].
    s = sup(wf(r1, 8), completed(2), failed=[5])
    case("e2 B goes", s, wf(r1, 5), True)
    case("e2 an older completed Record stays", s, wf(r1, 2), False)
    # 3. A transient retry. D at [8] attempt 2, D' at [8] attempt 3.
    s = sup(wf(r1, 8, attempt=3), completed(5))
    case("e3 D at a lower attempt of t goes", s, wf(r1, 8, attempt=2), True)
    case("e3 a Record of t itself stays", s, wf(r1, 8, attempt=3), False)
    # 4. A dropped speculative task. U at [9] T1, the next task at [9] T2.
    s = sup(wf(r1, 9, time=(200, 0)), completed(6))
    case("e4 U at an earlier started time goes", s, wf(r1, 9, time=(150, 0)), True)
    # 5. A refused transient attempt. c = [5], D at [11] refused, D' at [14].
    s = sup(wf(r1, 14), completed(5))
    case("e5 D between c and t goes", s, wf(r1, 11, attempt=2), True)
    case("e5 a Record at c stays", s, wf(r1, 5), False)
    # With c at attempt 0, an earlier attempt at c's started id is withdrawn and c itself isn't.
    s = sup(wf(r1, 14), {"started_event_id": 8, "attempt": 0, "started_time": [300, 0]})
    case("c attempt 0, an earlier attempt at c goes", s, wf(r1, 8, attempt=1, time=(200, 0)),
         True)
    case("c attempt 0, c itself stays", s, wf(r1, 8, attempt=2, time=(300, 0)), False)
    # A Local Activity of a withdrawn task is withdrawn too.
    la = {"kind": "local_activity", "id": "la:" + join(r1, "k"), "run_id": r1,
          "epoch": {"task_started_event_id": 5, "task_attempt": 1,
                    "task_started_time": [100, 0], "attempt": 2}}
    case("e1 a Local Activity of the failed task goes", sup(wf(r1, 11), completed(8), [5]), la,
         True)
    other_run = wf("r9", 5)
    case("another run's Record stays", sup(wf(r1, 11), completed(8), [5]), other_run, False)
    # 6. A Workflow retry. r2's first batch withdraws r1 and its Activity.
    act = {"kind": "activity", "id": "act:" + join(r1, "a", "s"), "run_id": r1,
           "epoch": {"attempt_scheduled_time": [100, 0], "attempt": 1}}
    s = sup(wf("r2", 3), None, withdraw_run=r1, run="r2")
    case("e6 r1's Record goes", s, wf(r1, 20), True)
    case("e6 r1's Activity Record goes", s, act, True)
    case("e6 r2's own earlier Record of t stays", s, wf("r2", 3), False)
    # An Activity retry withdraws only its own earlier attempt.
    s = {"producer_kind": "activity", "producer_id": act["id"], "run_id": r1,
         "epoch": {"attempt_scheduled_time": [130, 0], "attempt": 2}}
    case("an Activity retry withdraws attempt 1", s, act, True)
    other = dict(act, id="act:" + join(r1, "b", "s"))
    case("an Activity retry keeps another Activity", s, other, False)
    # A client's newer epoch fences its earlier one and withdraws nothing.
    named = {"kind": "client", "id": "client:" + join("ingest"), "epoch": {"attempt": 1}}
    s = {"producer_kind": "client", "producer_id": named["id"], "epoch": {"attempt": 2}}
    case("a client's newer epoch keeps its earlier Records", s, named, False)

    def la_at(started, task_attempt=1, attempt=1, run=r1, call="k"):
        return {"kind": "local_activity", "id": "la:" + join(run, call), "run_id": run,
                "epoch": {"task_started_event_id": started, "task_attempt": task_attempt,
                          "task_started_time": [100, 0], "attempt": attempt}}

    # A retry run's create writes an item with no epoch. It withdraws only the previous run, even
    # when it names failed tasks.
    s = {"producer_kind": "workflow", "producer_id": "wf:" + part("r2"), "run_id": "r2",
         "withdraw_run": r1, "failed_tasks": [5]}
    case("no epoch, r1's Record goes", s, wf(r1, 5), True)
    case("no epoch, r1's Local Activity Record goes", s, la_at(5), True)
    case("no epoch, r2's Local Activity Record at a failed task stays", s, la_at(5, run="r2"),
         False)
    case("no epoch, r2's Record stays", s, wf("r2", 5), False)
    # A Local Activity's newer epoch withdraws only its own lower epochs.
    s = {"producer_kind": "local_activity", "producer_id": "la:" + join(r1, "k"), "run_id": r1,
         "epoch": la_at(8, attempt=3)["epoch"]}
    case("a Local Activity's newer epoch withdraws a lower attempt", s, la_at(8, attempt=2), True)
    case("a Local Activity's newer epoch withdraws an earlier task", s, la_at(5, attempt=4), True)
    case("a Local Activity's newer epoch keeps its own epoch", s, la_at(8, attempt=3), False)
    case("a Local Activity's newer epoch keeps another call", s, la_at(5, call="j"), False)
    case("a Local Activity's item keeps the run's Workflow Record", s, wf(r1, 5), False)
    # Local Activity Records under a Workflow item with c = [8], t = [14] attempt 2, failed [5].
    s = sup(wf(r1, 14, attempt=2), completed(8), failed=[5])
    case("a Local Activity before c stays", s, la_at(3), False)
    case("a Local Activity at c stays", s, la_at(8), False)
    case("a Local Activity between c and t goes", s, la_at(11), True)
    case("a Local Activity of a lower attempt of t goes", s, la_at(14, task_attempt=1), True)
    case("a Local Activity of t itself stays", s, la_at(14, task_attempt=2), False)
    case("a Local Activity of another run stays", s, la_at(11, run="r9"), False)
    no_epoch = {"kind": "local_activity", "id": "la:" + join(r1, "k"), "run_id": r1}
    case("a Local Activity Record with no epoch stays", s, no_epoch, False)
    return cases


def main():
    for name, value in [
        ("ids.json", ids()),
        ("offsets.json", offsets()),
        ("epochs.json", epochs()),
        ("fields.json", fields()),
        ("withdrawals.json", withdrawal_examples()),
        ("digests.json", digests()),
    ]:
        with open(name, "w") as out:
            json.dump(value, out, indent=2, ensure_ascii=False)
            out.write("\n")


if __name__ == "__main__":
    main()
