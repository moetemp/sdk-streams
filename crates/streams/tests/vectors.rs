//! The shared test vectors, which `testdata/gen_vectors.py` writes from a second implementation.
//! Every SDK that builds ids or reads Records runs the same files.

use prost_types::Timestamp;
use serde_json::Value;
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    digest::batch_digest,
    epoch::{epoch_key, task_key, withdraws},
    ids::{self, StreamKeys, fields},
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{
        AppendRecord, Producer, ProducerEpoch, StreamProducerKind, StreamRecord, Supersession,
        WorkflowTaskEpoch,
    },
};

fn load(name: &str) -> Value {
    let path = format!("{}/testdata/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field].as_str().unwrap()
}

fn time(value: &Value) -> Option<Timestamp> {
    let pair = value.as_array()?;
    Some(Timestamp {
        seconds: pair[0].as_i64().unwrap(),
        nanos: pair[1].as_i64().unwrap() as i32,
    })
}

fn number(value: &Value, field: &str) -> i64 {
    value[field].as_i64().unwrap_or(0)
}

fn epoch(value: &Value) -> ProducerEpoch {
    ProducerEpoch {
        task_started_event_id: number(value, "task_started_event_id"),
        task_attempt: number(value, "task_attempt") as i32,
        task_started_time: time(&value["task_started_time"]),
        attempt_scheduled_time: time(&value["attempt_scheduled_time"]),
        attempt: number(value, "attempt") as i32,
    }
}

fn kind(name: &str) -> StreamProducerKind {
    match name {
        "workflow" => StreamProducerKind::Workflow,
        "local_activity" => StreamProducerKind::LocalActivity,
        "activity" => StreamProducerKind::Activity,
        "standalone_activity" => StreamProducerKind::StandaloneActivity,
        "client" => StreamProducerKind::Client,
        other => panic!("unknown kind {other}"),
    }
}

#[test]
fn stream_ids_and_keys_match() {
    let vectors = load("ids.json");
    for case in vectors["streams"].as_array().unwrap() {
        let (workflow_id, run) = (text(case, "workflow_id"), text(case, "first_run_id"));
        let id = match case["name"].as_str() {
            Some(name) => ids::scoped_stream_id(workflow_id, run, name),
            None => ids::default_stream_id(workflow_id, run),
        };
        assert_eq!(id, text(case, "stream_id"));
        let keys = StreamKeys::new(text(case, "prefix"), text(case, "namespace"), &id);
        assert_eq!(keys.meta(), text(case, "meta"));
        assert_eq!(keys.log(), text(case, "log"));
    }
}

#[test]
fn producer_ids_match() {
    let producers = &load("ids.json")["producers"];
    let id = |name: &str| text(&producers[name], "id");
    let field = |name: &str, field: &str| text(&producers[name], field);
    assert_eq!(
        ids::workflow_producer_id(field("workflow", "run_id")),
        id("workflow")
    );
    assert_eq!(
        ids::local_activity_producer_id(
            field("local_activity", "run_id"),
            field("local_activity", "call_key")
        ),
        id("local_activity")
    );
    assert_eq!(
        ids::activity_producer_id(
            field("activity", "run_id"),
            field("activity", "activity_id"),
            field("activity", "schedule_key")
        ),
        id("activity")
    );
    assert_eq!(
        ids::standalone_activity_producer_id(
            field("standalone_activity", "activity_id"),
            field("standalone_activity", "activity_run_id")
        ),
        id("standalone_activity")
    );
    assert_eq!(
        ids::client_producer_id(field("client", "identity"), field("client", "process_key")),
        id("client")
    );
    assert_eq!(
        ids::named_client_producer_id(field("named_client", "name")),
        id("named_client")
    );
}

#[test]
fn offsets_match() {
    for case in load("offsets.json").as_array().unwrap() {
        let offset = number(case, "offset");
        assert_eq!(offset_of_entry_id(text(case, "entry_id")).unwrap(), offset);
        assert_eq!(entry_id_of_offset(offset), text(case, "entry_id"));
    }
}

#[test]
fn epoch_keys_match() {
    for case in load("epochs.json").as_array().unwrap() {
        let epoch = epoch(&case["epoch"]);
        assert_eq!(epoch_key(&epoch), text(case, "key"));
        let task = WorkflowTaskEpoch {
            started_event_id: epoch.task_started_event_id,
            attempt: epoch.task_attempt,
            started_time: epoch.task_started_time,
        };
        assert_eq!(task_key(&task), text(case, "task_key"));
    }
}

#[test]
fn meta_fields_match() {
    let case = load("fields.json");
    let (kind, producer, run) = (
        number(&case, "kind") as i32,
        text(&case, "producer_id"),
        text(&case, "run_id"),
    );
    assert_eq!(fields::fence(kind, producer), text(&case, "fence"));
    assert_eq!(fields::dedupe(kind, producer), text(&case, "dedupe"));
    assert_eq!(fields::holds(kind, producer), text(&case, "holds"));
    assert_eq!(fields::run_wrote(run), text(&case, "run_wrote"));
    assert_eq!(fields::withdrawn_run(run), text(&case, "withdrawn_run"));
}

#[test]
fn the_appendix_e_withdrawals_match() {
    let cases = load("withdrawals.json");
    assert!(cases.as_array().unwrap().len() >= 35);
    for case in cases.as_array().unwrap() {
        let s = &case["supersession"];
        let supersession = Supersession {
            producer_kind: kind(text(s, "producer_kind")) as i32,
            producer_id: text(s, "producer_id").to_string(),
            // A create's item has no epoch, and a Reader must not read it as an empty one.
            epoch: s.get("epoch").map(epoch),
            previous_completed_task: s.get("previous_completed_task").map(|c| WorkflowTaskEpoch {
                started_event_id: number(c, "started_event_id"),
                attempt: number(c, "attempt") as i32,
                started_time: time(&c["started_time"]),
            }),
            failed_tasks: s["failed_tasks"]
                .as_array()
                .map(|list| list.iter().map(|v| v.as_i64().unwrap()).collect())
                .unwrap_or_default(),
            withdraw_run: s["withdraw_run"].as_str().unwrap_or_default().to_string(),
            run_id: s["run_id"].as_str().unwrap_or_default().to_string(),
        };
        let p = &case["record"]["producer"];
        let record = StreamRecord {
            producer: Some(Producer {
                kind: kind(text(p, "kind")) as i32,
                id: text(p, "id").to_string(),
                epoch: p.get("epoch").map(epoch),
                run_id: p["run_id"].as_str().unwrap_or_default().to_string(),
            }),
            ..Default::default()
        };
        assert_eq!(
            withdraws(&supersession, &record),
            case["withdraws"].as_bool().unwrap(),
            "{}",
            text(case, "name")
        );
    }
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn batch_digests_match() {
    for case in load("digests.json").as_array().unwrap() {
        let records: Vec<_> = case["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| AppendRecord {
                topic: text(r, "topic").to_string(),
                // A record with neither metadata nor data has no body.
                body: (r.get("metadata").is_some() || r.get("data").is_some()).then(|| Payload {
                    metadata: r["metadata"]
                        .as_object()
                        .into_iter()
                        .flatten()
                        .map(|(key, value)| (key.clone(), hex(value.as_str().unwrap())))
                        .collect(),
                    data: hex(r["data"].as_str().unwrap_or_default()),
                    ..Default::default()
                }),
            })
            .collect();
        assert_eq!(
            batch_digest(&records),
            text(case, "digest"),
            "{}",
            text(case, "name")
        );
    }
}
