//! The N0a contract's models in their JSON transfer form.
//!
//! Inputs reject unknown members, so a typo fails at the endpoint. Outputs skip absent members, as
//! the generated callers expect. Bytes cross as padded standard base64, and an integer may cross
//! as a whole float, as the generated models allow.

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

/// Bytes that cross as a base64 string.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Base64(pub(crate) Vec<u8>);

impl Serialize for Base64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for Base64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        base64_bytes(&text)
            .map(Base64)
            .ok_or_else(|| D::Error::custom("must be base64-encoded"))
    }
}

/// The bytes `text` holds as padded standard base64.
pub(crate) fn base64_bytes(text: &str) -> Option<Vec<u8>> {
    STANDARD.decode(text.as_bytes()).ok()
}

/// An integer as the contract's JSON carries it, where `1.0` counts and `1.5` doesn't.
fn spec_integer<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    let number = serde_json::Number::deserialize(deserializer)?;
    if let Some(value) = number.as_i64() {
        return Ok(value);
    }
    match number.as_f64() {
        Some(value) if value.fract() == 0.0 && value.abs() < 2f64.powi(53) => Ok(value as i64),
        _ => Err(D::Error::custom(format!("expected integer, got {number}"))),
    }
}

fn optional_spec_integer<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<i64>, D::Error> {
    Option::<serde_json::Value>::deserialize(deserializer)?
        .map(|value| spec_integer(value).map_err(D::Error::custom))
        .transpose()
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamRef {
    pub(crate) kind: String,
    pub(crate) workflow_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) activity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) chain_run_id: Option<String>,
    pub(crate) topic: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppendInput {
    pub(crate) stream: StreamRef,
    pub(crate) producer_id: String,
    #[serde(deserialize_with = "spec_integer")]
    pub(crate) attempt: i64,
    #[serde(deserialize_with = "spec_integer")]
    pub(crate) sequence: i64,
    #[serde(default)]
    pub(crate) payloads: Option<Vec<Base64>>,
    #[serde(default)]
    pub(crate) finish: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct AppendOutput {
    pub(crate) cursor: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadInput {
    pub(crate) stream: StreamRef,
    #[serde(default)]
    pub(crate) after_token: Option<String>,
    #[serde(default, deserialize_with = "optional_spec_integer")]
    pub(crate) max_records: Option<i64>,
    #[serde(default, deserialize_with = "optional_spec_integer")]
    pub(crate) wait_ms: Option<i64>,
    #[serde(default)]
    pub(crate) latest_only: Option<bool>,
    /// Left as base64, so an oversized state is refused before it is decoded.
    #[serde(default)]
    pub(crate) state: Option<String>,
    #[serde(default)]
    pub(crate) floor_holder: Option<String>,
    #[serde(default)]
    pub(crate) wake_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadRangeInput {
    pub(crate) stream: StreamRef,
    pub(crate) chain_first_run_id: String,
    pub(crate) after_token: String,
    pub(crate) to_token: String,
    #[serde(deserialize_with = "spec_integer")]
    pub(crate) stored_records: i64,
    /// Left as base64, so an oversized state is refused before it is decoded.
    #[serde(default)]
    pub(crate) state: Option<String>,
    pub(crate) records_digest: Base64,
    #[serde(default)]
    pub(crate) floor_holder: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReadOutput {
    pub(crate) records: Vec<RecordWire>,
    pub(crate) next_token: String,
    pub(crate) done: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) state: Option<Base64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) replay: Option<ReplayRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReplayRange {
    pub(crate) chain_first_run_id: String,
    pub(crate) after_token: String,
    pub(crate) stored_records: i64,
    pub(crate) records_digest: Base64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct RecordWire {
    pub(crate) token: String,
    pub(crate) record: Base64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stale: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) superseded: Option<Supersession>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Supersession {
    pub(crate) producer_id: String,
    pub(crate) previous_attempt: i64,
    pub(crate) attempt: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FollowInput {
    pub(crate) stream: StreamRef,
    #[serde(default)]
    pub(crate) after_token: Option<String>,
    /// Left as base64, so an oversized state is refused before it is decoded.
    #[serde(default)]
    pub(crate) state: Option<String>,
    #[serde(default, deserialize_with = "optional_spec_integer")]
    pub(crate) max_records: Option<i64>,
    #[serde(default, deserialize_with = "optional_spec_integer")]
    pub(crate) wait_ms: Option<i64>,
    #[serde(default, deserialize_with = "optional_spec_integer")]
    pub(crate) max_depth: Option<i64>,
    #[serde(default, deserialize_with = "optional_spec_integer")]
    pub(crate) max_open: Option<i64>,
    #[serde(default)]
    pub(crate) floor_holder: Option<String>,
    #[serde(default)]
    pub(crate) wake_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FollowRangeInput {
    pub(crate) stream: StreamRef,
    #[serde(default)]
    pub(crate) state: Option<String>,
    pub(crate) ranges: Vec<Base64>,
    pub(crate) follow_digest: Base64,
    #[serde(default, deserialize_with = "optional_spec_integer")]
    pub(crate) max_depth: Option<i64>,
    #[serde(default, deserialize_with = "optional_spec_integer")]
    pub(crate) max_open: Option<i64>,
    #[serde(default)]
    pub(crate) floor_holder: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FollowOutput {
    pub(crate) records: Vec<FollowedWire>,
    pub(crate) done: bool,
    pub(crate) state: Base64,
    pub(crate) sources: Vec<FollowSourceWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) replay: Option<FollowReplay>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FollowedWire {
    pub(crate) source: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) record: Option<RecordWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) link_state: Option<LinkStateWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LinkStateWire {
    pub(crate) kind: String,
    pub(crate) link: Base64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FollowSourceWire {
    pub(crate) number: u32,
    pub(crate) parent: u32,
    pub(crate) depth: u32,
    pub(crate) stream: StreamRef,
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FollowReplay {
    pub(crate) ranges: Vec<Base64>,
    pub(crate) follow_digest: Base64,
}

/// The bytes `size` record bytes take in a read answer, as base64.
pub(crate) fn crossing_size(size: usize) -> usize {
    4 * size.div_ceil(3)
}

/// The bytes `value` takes as JSON.
pub(crate) fn json_size(value: &impl Serialize) -> usize {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).expect("a contract model always serializes");
    count.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_integer_may_cross_as_a_whole_float() {
        let input: AppendInput = serde_json::from_str(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},
                "producer_id":"p","attempt":1.0,"sequence":2,"payloads":["AQI="]}"#,
        )
        .unwrap();
        assert_eq!((input.attempt, input.sequence), (1, 2));
        assert_eq!(input.payloads.unwrap()[0].0, [1, 2]);
        let error = serde_json::from_str::<AppendInput>(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},
                "producer_id":"p","attempt":1.5,"sequence":2}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("expected integer"), "{error}");
    }

    #[test]
    fn an_input_refuses_an_unknown_member_and_bad_base64() {
        let unknown = serde_json::from_str::<ReadInput>(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},"wait":5}"#,
        )
        .unwrap_err();
        assert!(unknown.to_string().contains("unknown field"), "{unknown}");
        let bad = serde_json::from_str::<AppendInput>(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},
                "producer_id":"p","attempt":1,"sequence":1,"payloads":["!!"]}"#,
        )
        .unwrap_err();
        assert!(bad.to_string().contains("base64"), "{bad}");
        assert!(!bad.to_string().contains("!!"), "{bad}");
        // Null is what a generated caller sends for an absent optional member.
        let null: ReadInput = serde_json::from_str(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","chain_run_id":null,"topic":"t"},
                "after_token":null,"max_records":null}"#,
        )
        .unwrap();
        assert_eq!(null.stream.chain_run_id, None);
        assert_eq!(null.max_records, None);
    }

    #[test]
    fn an_output_leaves_absent_members_out() {
        let wire = RecordWire {
            token: "c".to_string(),
            record: Base64(vec![1]),
            error: None,
            stale: None,
            superseded: None,
        };
        assert_eq!(
            serde_json::to_string(&wire).unwrap(),
            r#"{"token":"c","record":"AQ=="}"#
        );
    }

    #[test]
    fn a_read_range_input_takes_its_members_and_refuses_others() {
        let input: ReadRangeInput = serde_json::from_str(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},
                "chain_first_run_id":"run-1","after_token":"","to_token":"c","stored_records":2.0,
                "state":null,"records_digest":"AQI=","floor_holder":"ns/run/r"}"#,
        )
        .unwrap();
        assert_eq!(input.stored_records, 2);
        assert_eq!(input.records_digest.0, [1, 2]);
        assert_eq!(input.state, None);
        assert_eq!(input.floor_holder.as_deref(), Some("ns/run/r"));
        let unknown = serde_json::from_str::<ReadRangeInput>(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},
                "chain_first_run_id":"run-1","after_token":"","to_token":"c","stored_records":0,
                "records_digest":"","wait_ms":5}"#,
        )
        .unwrap_err();
        assert!(unknown.to_string().contains("unknown field"), "{unknown}");
        let missing = serde_json::from_str::<ReadRangeInput>(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},
                "chain_first_run_id":"run-1","after_token":"","stored_records":0,
                "records_digest":""}"#,
        )
        .unwrap_err();
        assert!(missing.to_string().contains("to_token"), "{missing}");
    }

    #[test]
    fn a_replay_range_crosses_with_its_digest_as_base64() {
        let output = ReadOutput {
            records: Vec::new(),
            next_token: "c".to_string(),
            done: false,
            state: None,
            replay: Some(ReplayRange {
                chain_first_run_id: "run-1".to_string(),
                after_token: String::new(),
                stored_records: 0,
                records_digest: Base64(vec![1, 2]),
            }),
        };
        assert_eq!(
            serde_json::to_value(&output).unwrap(),
            serde_json::json!({"records": [], "next_token": "c", "done": false,
                "replay": {"chain_first_run_id": "run-1", "after_token": "", "stored_records": 0,
                    "records_digest": "AQI="}})
        );
    }

    #[test]
    fn a_record_crosses_at_four_thirds_of_its_bytes() {
        assert_eq!(crossing_size(0), 0);
        assert_eq!(crossing_size(1), 4);
        assert_eq!(crossing_size(3), 4);
        assert_eq!(crossing_size(4), 8);
    }
}
