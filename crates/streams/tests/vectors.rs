//! Values every SDK must produce the same, so a stream one SDK wrote stays readable by another,
//! and the rules every lang must follow the same way.

use serde_json::Value;
use temporalio_streams::{cursor_position, mint_cursor, stream_hash};

#[test]
fn cursors_match_the_python_sdk() {
    // A cursor another SDK minted must resume on the same record.
    let cases: Vec<Value> = serde_json::from_str(include_str!("../testdata/cursors.json")).unwrap();
    assert!(!cases.is_empty());
    for case in &cases {
        let text = |field: &str| case[field].as_str().unwrap();
        let hash = stream_hash(
            text("namespace"),
            text("kind"),
            text("workflow_id"),
            text("topic"),
        );
        assert_eq!(hash, text("hash"));
        for (store, position, field) in [
            ("redis", "1-0", "cursor_redis_1_0"),
            ("memory", "5", "cursor_memory_5"),
        ] {
            assert_eq!(mint_cursor(store, &hash, position), text(field));
            assert_eq!(
                cursor_position(text(field), store, &hash),
                Ok(Some(position))
            );
        }
    }
}

#[test]
fn activity_attempt_keys_match_the_vectors() {
    // Every SDK reports the attempt a record carries, so each one composes it the same way.
    use temporalio_streams::{activity_attempt, proto::ActivityProducer};
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../testdata/streams/activity_attempts.json")).unwrap();
    assert!(cases.iter().any(|case| case["key"].is_null()));
    for case in &cases {
        let mut activity = ActivityProducer {
            activity_id: "act".to_string(),
            run_id: "run".to_string(),
            attempt: case["attempt"].as_i64().unwrap() as i32,
            ..Default::default()
        };
        if let Some(started) = case["started_time"].as_object() {
            let time = activity
                .attempt_started_time
                .get_or_insert_with(Default::default);
            time.seconds = started["seconds"].as_i64().unwrap();
            time.nanos = started["nanos"].as_i64().unwrap() as i32;
        }
        let name = case["name"].as_str().unwrap();
        match case["key"].as_str() {
            Some(key) => assert_eq!(
                activity_attempt(&activity).unwrap().to_string(),
                key,
                "{name}"
            ),
            None => assert!(activity_attempt(&activity).is_err(), "{name}"),
        }
    }
}

/// The wake in a `wake_folds.json` entry. `close_result` is the data of the result payload, and
/// `close_failure` the failure's message. A missing field takes its proto default.
fn vector_wake(value: &Value) -> temporalio_streams::protos::temporal::api::stream::v1::StreamWake {
    use temporalio_common::protos::temporal::api::{common::v1::Payload, failure::v1::Failure};
    let text = |field: &str| value[field].as_str().unwrap_or_default().to_string();
    let flag = |field: &str| value[field].as_bool().unwrap_or_default();
    temporalio_streams::protos::temporal::api::stream::v1::StreamWake {
        subscription_id: text("subscription_id"),
        position: text("position"),
        counter: value["counter"].as_i64().unwrap_or_default(),
        metadata: value["metadata"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
            .collect(),
        recheck: flag("recheck"),
        closed: flag("closed"),
        close_result: value["close_result"].as_str().map(|data| Payload {
            data: data.as_bytes().to_vec(),
            ..Default::default()
        }),
        close_failure: value["close_failure"].as_str().map(|message| Failure {
            message: message.to_string(),
            ..Default::default()
        }),
        resubscribe: flag("resubscribe"),
    }
}

#[test]
fn wake_folds_match_the_vectors() {
    // Each lang folds a task's wakes itself, so each one must give these results.
    use temporalio_streams::fold_stream_wake;
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../testdata/wake_folds.json")).unwrap();
    assert!(!cases.is_empty());
    for case in &cases {
        let wakes = |field: &str| -> Vec<_> {
            case[field]
                .as_array()
                .unwrap()
                .iter()
                .map(vector_wake)
                .collect()
        };
        let mut folded = vec![];
        for wake in wakes("wakes") {
            fold_stream_wake(&mut folded, &wake);
        }
        assert_eq!(folded, wakes("folded"), "{}", case["name"]);
    }
}

/// The progress in a `nexus_progress_folds.json` entry. A missing `key` leaves the operation
/// unset, and a missing field takes its proto default.
fn vector_progress(
    value: &Value,
) -> temporalio_streams::protos::temporal::api::nexus::v1::NexusOperationProgress {
    use temporalio_streams::protos::temporal::api::nexus::v1::{
        NexusOperationProgress,
        nexus_operation_progress::{Operation, OperationKey},
    };
    let text = |value: &Value, field: &str| value[field].as_str().unwrap_or_default().to_string();
    let key = &value["key"];
    NexusOperationProgress {
        operation: key.is_object().then(|| {
            Operation::OperationKey(OperationKey {
                endpoint: text(key, "endpoint"),
                service: text(key, "service"),
                operation: text(key, "operation"),
                operation_token: text(key, "operation_token"),
            })
        }),
        position: text(value, "position"),
        counter: value["counter"].as_i64().unwrap_or_default(),
        metadata: value["metadata"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
            .collect(),
    }
}

fn vector_progresses(
    value: &Value,
) -> Vec<temporalio_streams::protos::temporal::api::nexus::v1::NexusOperationProgress> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(vector_progress)
        .collect()
}

#[test]
fn nexus_progress_folds_match_the_vectors() {
    // Each lang folds a task's progress itself, so each one must give these results.
    use temporalio_streams::fold_nexus_progress;
    let vectors: Value =
        serde_json::from_str(include_str!("../testdata/nexus_progress_folds.json")).unwrap();
    let cases = vectors["folds"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let mut folded = vec![];
        for progress in vector_progresses(&case["progress"]) {
            fold_nexus_progress(&mut folded, &progress);
        }
        assert_eq!(
            folded,
            vector_progresses(&case["folded"]),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn nexus_progress_deliveries_match_the_vectors() {
    // Each lang hands progress to its operations itself, so each one must give these results.
    use temporalio_streams::{
        NexusProgressOperations, fold_nexus_progress,
        protos::temporal::api::nexus::v1::nexus_operation_progress::{Operation, OperationKey},
    };
    let vectors: Value =
        serde_json::from_str(include_str!("../testdata/nexus_progress_folds.json")).unwrap();
    let cases = vectors["deliveries"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let mut operations = NexusProgressOperations::default();
        let mut keys: std::collections::HashMap<u32, OperationKey> = Default::default();
        for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            let text = |value: &Value, field: &str| value[field].as_str().unwrap().to_string();
            let seq = |value: &Value| value["seq"].as_u64().unwrap() as u32;
            if let Some(scheduled) = step.get("schedule") {
                operations.schedule(
                    seq(scheduled),
                    &text(scheduled, "endpoint"),
                    &text(scheduled, "service"),
                    &text(scheduled, "operation"),
                );
                keys.insert(
                    seq(scheduled),
                    OperationKey {
                        endpoint: text(scheduled, "endpoint"),
                        service: text(scheduled, "service"),
                        operation: text(scheduled, "operation"),
                        operation_token: String::new(),
                    },
                );
            } else if let Some(started) = step.get("start") {
                operations.start(seq(started), &text(started, "operation_token"));
                if let Some(key) = keys.get_mut(&seq(started)) {
                    key.operation_token = text(started, "operation_token");
                }
            } else if let Some(resolved) = step.get("resolve") {
                operations.resolve(seq(resolved));
            } else {
                let mut folded = vec![];
                for progress in vector_progresses(&step["attachments"]) {
                    fold_nexus_progress(&mut folded, &progress);
                }
                // A delivery hands over the whole folded progress of the operation's key, so
                // its key and metadata reach lang as they came.
                let expected: Vec<_> = step["delivered"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|d| {
                        let operation = Some(Operation::OperationKey(keys[&seq(d)].clone()));
                        let progress = folded
                            .iter()
                            .find(|progress| progress.operation == operation)
                            .unwrap_or_else(|| panic!("{name}, step {index}: no progress"));
                        assert_eq!(
                            (progress.position.as_str(), progress.counter),
                            (
                                d["position"].as_str().unwrap(),
                                d["counter"].as_i64().unwrap()
                            ),
                            "{name}, step {index}"
                        );
                        (seq(d), progress.clone())
                    })
                    .collect();
                assert_eq!(
                    operations.deliver(&folded),
                    expected,
                    "{name}, step {index}"
                );
            }
        }
    }
}
