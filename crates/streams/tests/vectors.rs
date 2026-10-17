//! Values the Python SDK produced before the store moved into Core, which Core must reproduce.

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
