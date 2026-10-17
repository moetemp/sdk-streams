//! Values the Python SDK produced before the store moved into this crate, which the crate must
//! reproduce, and the rules every lang must follow the same way.

use serde_json::Value;
use temporalio_streams::{cursor_position, mint_cursor, stream_hash};

#[test]
fn cursors_match_the_python_sdk() {
    // A cursor a reader kept from before the move must resume on the same record.
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
