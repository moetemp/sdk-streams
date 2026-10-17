"""Drives the Python SDK's Redis provider for the cross-implementation test.

Run from a Python SDK checkout with ``uv run --no-sync python brook.py '<json>'``. The argument
names one operation on one stream, and the answer is one JSON line. The provider talks to Redis
as in production. A stand-in for the Temporal client describes one running Workflow, so no
server is needed.
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import temporalio.converter
from temporalio.client import WorkflowExecutionStatus
from temporalio.contrib.streams import Cursor, RecordKind, StreamError, StreamRef
from temporalio.contrib.streams._body import content_fingerprint
from temporalio.contrib.streams._wire import to_wire
from temporalio.contrib.streams.redis import RedisStreams


class Owner:
    """A Temporal client that knows one running Workflow, whose first run is ``first_run``."""

    def __init__(self, namespace: str, first_run: str) -> None:
        self.namespace = namespace
        self.data_converter = temporalio.converter.DataConverter.default
        self._first_run = first_run

    def get_workflow_handle(self, workflow_id: str, run_id: str | None = None) -> Any:
        async def describe() -> Any:
            info = SimpleNamespace(first_run_id=self._first_run)
            return SimpleNamespace(
                status=WorkflowExecutionStatus.RUNNING,
                run_id=run_id or self._first_run,
                raw_description=SimpleNamespace(workflow_execution_info=info),
            )

        return SimpleNamespace(describe=describe)


def record(item: Any) -> dict[str, Any]:
    return {
        "cursor": item.cursor.token,
        "kind": item.kind.name,
        "value": item.value,
        "producer_id": item.producer_id,
        "attempt": item.attempt,
        "sequence": item.sequence,
        "run_id": item.run_id,
        "stale": item.stale,
    }


async def run(op: dict[str, Any]) -> Any:
    provider = RedisStreams(
        op["url"],
        key_prefix=op["prefix"],
        retention=timedelta(milliseconds=op.get("retention_ms", 7 * 24 * 3600 * 1000)),
        poll_interval=timedelta(milliseconds=50),
    )
    owner = Owner(op["namespace"], op["first_run"])
    ref = StreamRef.for_workflow(op["workflow_id"])
    stream = provider.get_stream_handle(owner, ref)  # type: ignore[arg-type]
    topic = op.get("topic", "events")
    try:
        if op["op"] == "append":
            producer = stream.producer(
                topic=topic, producer_id=op["producer"], attempt=op.get("attempt", 1)
            )
            producer._sequence = op.get("sequence", 1)
            converter = owner.data_converter.payload_converter
            wires = [
                to_wire(
                    converter,
                    topic=topic,
                    kind=RecordKind.DATA,
                    value=value,
                    producer_id=op["producer"],
                    attempt=op.get("attempt", 1),
                    sequence=producer._sequence + index,
                )
                for index, value in enumerate(op["values"])
            ]
            digest = content_fingerprint(wires).hex()
            cursor = await producer.append(*op["values"])
            return {"cursor": cursor.token, "digest": digest}
        if op["op"] == "digest":
            converter = owner.data_converter.payload_converter
            wires = [
                to_wire(
                    converter,
                    topic=topic,
                    kind=RecordKind.DATA,
                    value=value,
                    producer_id=op["producer"],
                    attempt=op.get("attempt", 1),
                    sequence=op.get("sequence", 1) + index,
                )
                for index, value in enumerate(op["values"])
            ]
            return content_fingerprint(wires).hex()
        if op["op"] == "read":
            records = stream.read(topic=topic, after=Cursor(op.get("after", "")))
            out = []
            try:
                while len(out) < op["count"]:
                    out.append(record(await asyncio.wait_for(records.__anext__(), 5)))
            except asyncio.TimeoutError:
                return {
                    "error": "Timeout",
                    "message": f"got {len(out)} records",
                    "cursor": None,
                }
            finally:
                await records.aclose()
            return out
        if op["op"] == "latest":
            return (await stream.latest(topic=topic)).token
        if op["op"] == "close":
            await provider._mark_closed(
                provider._chain_keys(
                    op["namespace"], op["workflow_id"], op["first_run"]
                )
            )
            return None
        raise ValueError(f"no operation {op['op']!r}")
    except StreamError as error:
        cursor = getattr(error, "cursor", None)
        return {
            "error": type(error).__name__,
            "message": str(error),
            "cursor": cursor.token if cursor is not None else None,
        }
    finally:
        await provider.close()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run(json.loads(sys.argv[1])))))
