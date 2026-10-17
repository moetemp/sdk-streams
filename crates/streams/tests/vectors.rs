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
