//! The archive's object names and object format.
//!
//! ```text
//! v1/<namespace>/w/<workflow id>/<first run id>/<topic>/seg-<last position>.tsr
//! v1/<namespace>/a/<activity id>/<run id>/<topic>/seg-<last position>.tsr
//! v1/<namespace>/wa/<workflow id>/<run id>/<activity id>/<topic>/seg-<last position>.tsr
//! .../<topic>/close.pb
//! ```
//!
//! Each part is percent-encoded as Redis keys are, with `.` encoded too, so no part is a path
//! step such as `..`. A position in a name is its two numbers, each zero-padded to 20 digits, so
//! names sort in log order. The first segment after a position is the first name after
//! `seg-<that position>.tsr`.
//!
//! A segment is `TSR1`, a flags byte, the records as length-delimited `StoredRecord`s, a
//! [SegmentTrailer], and the trailer's length as 4 big-endian bytes. A segment holds every record
//! of its topic after the trailer's `after_position` up to its `last_position`. `close.pb` is an
//! [ArchivedClose].

use crate::{
    StreamError, StreamResult,
    proto::{ChainId, DeleteOwnerRequest, StoredClose, StoredRecord, StreamOwnerKind},
    record::part,
};
use prost::Message;
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 4] = b"TSR1";

/// No compression. Other flags are kept for later formats, and a reader refuses them.
const FLAGS_NONE: u8 = 0;

/// The name of a closed topic's close, under the topic's prefix.
pub(crate) const CLOSE_NAME: &str = "close.pb";

/// What ends a segment.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct SegmentTrailer {
    #[prost(uint64, tag = "1")]
    pub(crate) count: u64,
    /// The segment holds every record after this position. Empty for the topic's start. When the
    /// archive found records lost before it, this is the segment's first position.
    #[prost(string, tag = "2")]
    pub(crate) after_position: String,
    #[prost(string, tag = "3")]
    pub(crate) first_position: String,
    #[prost(string, tag = "4")]
    pub(crate) last_position: String,
    /// SHA-256 over the length-delimited records.
    #[prost(bytes = "vec", tag = "5")]
    pub(crate) sha256: Vec<u8>,
}

/// A closed topic that the archive holds to its end.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct ArchivedClose {
    /// The topic's first close. Unset for a chain close, which keeps no outcome.
    #[prost(message, optional, tag = "1")]
    pub(crate) close: Option<StoredClose>,
    /// The position of the topic's last record, or empty when it took none.
    #[prost(string, tag = "2")]
    pub(crate) last_position: String,
    /// The newest position the topic ever took.
    #[prost(string, tag = "3")]
    pub(crate) newest_ever: String,
}

/// One decoded segment.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Segment {
    pub(crate) after_position: String,
    pub(crate) records: Vec<StoredRecord>,
}

impl Segment {
    pub(crate) fn last_position(&self) -> &str {
        self.records
            .last()
            .map_or("", |record| record.position.as_str())
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        for record in &self.records {
            record
                .encode_length_delimited(&mut body)
                .expect("a Vec grows");
        }
        let trailer = SegmentTrailer {
            count: self.records.len() as u64,
            after_position: self.after_position.clone(),
            first_position: self
                .records
                .first()
                .map(|record| record.position.clone())
                .unwrap_or_default(),
            last_position: self.last_position().to_string(),
            sha256: Sha256::digest(&body).to_vec(),
        }
        .encode_to_vec();
        let mut out = Vec::with_capacity(MAGIC.len() + 1 + body.len() + trailer.len() + 4);
        out.extend_from_slice(MAGIC);
        out.push(FLAGS_NONE);
        out.extend_from_slice(&body);
        out.extend_from_slice(&trailer);
        out.extend_from_slice(&(trailer.len() as u32).to_be_bytes());
        out
    }

    pub(crate) fn decode(name: &str, bytes: &[u8]) -> StreamResult<Self> {
        let bad = |why: &str| StreamError::storage(format!("archive object {name} {why}"));
        let rest = bytes
            .strip_prefix(MAGIC.as_slice())
            .ok_or_else(|| bad("is not a stream segment"))?;
        let (&flags, rest) = rest.split_first().ok_or_else(|| bad("ends early"))?;
        if flags != FLAGS_NONE {
            return Err(bad(&format!(
                "has flags {flags}, which this release can't read"
            )));
        }
        let (rest, length) = rest
            .split_last_chunk::<4>()
            .ok_or_else(|| bad("ends early"))?;
        let body_length = rest
            .len()
            .checked_sub(u32::from_be_bytes(*length) as usize)
            .ok_or_else(|| bad("ends early"))?;
        let (mut body, trailer) = rest.split_at(body_length);
        let trailer = SegmentTrailer::decode(trailer).map_err(|_| bad("has a bad trailer"))?;
        if Sha256::digest(body).as_slice() != trailer.sha256 {
            return Err(bad("does not match its checksum"));
        }
        let mut records = Vec::with_capacity(trailer.count as usize);
        while !body.is_empty() {
            records.push(
                StoredRecord::decode_length_delimited(&mut body)
                    .map_err(|_| bad("has a bad record"))?,
            );
        }
        if records.len() as u64 != trailer.count {
            return Err(bad("holds another count of records than its trailer"));
        }
        Ok(Self {
            after_position: trailer.after_position,
            records,
        })
    }
}

impl ArchivedClose {
    pub(crate) fn decode_object(name: &str, bytes: &[u8]) -> StreamResult<Self> {
        Self::decode(bytes)
            .map_err(|_| StreamError::storage(format!("archive object {name} is not a close")))
    }
}

/// A position as two zero-padded numbers, so positions of one store sort as text in log order.
/// Takes a Redis entry id, `<ms>-<seq>`, and the memory store's offsets.
pub(crate) fn sort_key(position: &str) -> StreamResult<String> {
    let number = |text: &str| {
        (!text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| text.parse::<u64>().ok())
            .flatten()
    };
    let (high, low) = position.split_once('-').unwrap_or((position, "0"));
    match (number(high), number(low)) {
        (Some(high), Some(low)) => Ok(format!("{high:020}-{low:020}")),
        _ => Err(StreamError::unsupported(format!(
            "position {position:?} has no order the archive knows"
        ))),
    }
}

/// The name of the segment whose last record is at `position`.
pub(crate) fn segment_name(prefix: &str, position: &str) -> StreamResult<String> {
    Ok(format!("{prefix}seg-{}.tsr", sort_key(position)?))
}

/// The start of every segment name. It sorts after [CLOSE_NAME].
pub(crate) fn segments_start(prefix: &str) -> String {
    format!("{prefix}seg-")
}

/// The name that every segment holding `position` or a later one sorts after.
pub(crate) fn holding_start(prefix: &str, position: &str) -> StreamResult<String> {
    Ok(format!("{prefix}seg-{}", sort_key(position)?))
}

fn name_part(text: &str) -> String {
    part(text).replace('.', "%2E")
}

/// The name prefix of every object of one topic, ending with `/`.
pub(crate) fn topic_prefix(chain: &ChainId, topic: &str) -> String {
    let namespace = name_part(&chain.namespace);
    let run = name_part(&chain.first_run_id);
    let owner = match StreamOwnerKind::try_from(chain.owner_kind) {
        Ok(StreamOwnerKind::Activity) => format!("a/{}/{run}", name_part(&chain.activity_id)),
        Ok(StreamOwnerKind::WorkflowActivity) => format!(
            "wa/{}/{run}/{}",
            name_part(&chain.workflow_id),
            name_part(&chain.activity_id)
        ),
        _ => format!("w/{}/{run}", name_part(&chain.workflow_id)),
    };
    format!("v1/{namespace}/{owner}/{}/", name_part(topic))
}

/// The name prefixes of every object that an owner delete removes: all the owner's chains, and a
/// Workflow's Workflow Activities.
pub(crate) fn owner_prefixes(request: &DeleteOwnerRequest) -> Vec<String> {
    let namespace = name_part(&request.namespace);
    if request.owner_kind == StreamOwnerKind::Activity as i32 {
        return vec![format!(
            "v1/{namespace}/a/{}/",
            name_part(&request.activity_id)
        )];
    }
    let workflow = name_part(&request.workflow_id);
    vec![
        format!("v1/{namespace}/w/{workflow}/"),
        format!("v1/{namespace}/wa/{workflow}/"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn chain_of(case: &Value) -> ChainId {
        let text = |key: &str| case[key].as_str().unwrap_or_default().to_string();
        ChainId {
            namespace: text("namespace"),
            workflow_id: text("workflow_id"),
            first_run_id: text("first_run_id"),
            owner_kind: case["owner_kind"].as_i64().unwrap_or_default() as i32,
            activity_id: text("activity_id"),
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn names_match_the_vectors() {
        let vectors: Value =
            serde_json::from_str(include_str!("../../testdata/archive/names.json")).unwrap();
        for case in vectors["topics"].as_array().unwrap() {
            let chain = chain_of(case);
            let prefix = topic_prefix(&chain, case["topic"].as_str().unwrap());
            assert_eq!(prefix, case["prefix"].as_str().unwrap(), "{case}");
        }
        for case in vectors["positions"].as_array().unwrap() {
            let name = segment_name("p/", case["position"].as_str().unwrap()).unwrap();
            assert_eq!(name, case["segment"].as_str().unwrap(), "{case}");
        }
        for case in vectors["deletes"].as_array().unwrap() {
            let request = DeleteOwnerRequest {
                namespace: case["namespace"].as_str().unwrap().to_string(),
                owner_kind: case["owner_kind"].as_i64().unwrap() as i32,
                workflow_id: case["workflow_id"].as_str().unwrap_or_default().to_string(),
                activity_id: case["activity_id"].as_str().unwrap_or_default().to_string(),
            };
            let expected: Vec<String> = serde_json::from_value(case["prefixes"].clone()).unwrap();
            assert_eq!(owner_prefixes(&request), expected, "{case}");
        }
    }

    #[test]
    fn positions_sort_in_log_order() {
        let positions = [
            "0",
            "9",
            "10",
            "1-0",
            "1-1",
            "1-10",
            "2-0",
            "18446744073709551615-3",
        ];
        // The memory store's offsets and Redis entry ids, each in log order.
        for positions in [&positions[..3], &positions[3..]] {
            let keys: Vec<String> = positions.iter().map(|p| sort_key(p).unwrap()).collect();
            assert!(keys.is_sorted(), "{keys:?}");
        }
        for bad in ["", "-1", "1-", "a-1", "1-2-3", "99999999999999999999"] {
            assert!(sort_key(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_segment_and_a_close_match_the_vectors() {
        let vectors: Value =
            serde_json::from_str(include_str!("../../testdata/archive/objects.json")).unwrap();
        let segment = &vectors["segment"];
        let records: Vec<StoredRecord> = segment["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|record| StoredRecord {
                position: record["position"].as_str().unwrap().to_string(),
                record: record["record"].as_str().unwrap().as_bytes().to_vec(),
            })
            .collect();
        let decoded = Segment {
            after_position: segment["after_position"].as_str().unwrap().to_string(),
            records,
        };
        let bytes = decoded.encode();
        assert_eq!(hex(&bytes), segment["hex"].as_str().unwrap());
        assert_eq!(Segment::decode("s", &bytes).unwrap(), decoded);

        let close = &vectors["close"];
        let archived = ArchivedClose {
            close: Some(StoredClose {
                closed_by: close["closed_by"].as_str().unwrap().to_string(),
                ..Default::default()
            }),
            last_position: close["last_position"].as_str().unwrap().to_string(),
            newest_ever: close["newest_ever"].as_str().unwrap().to_string(),
        };
        assert_eq!(
            hex(&archived.encode_to_vec()),
            close["hex"].as_str().unwrap()
        );
    }

    #[test]
    fn a_damaged_segment_is_refused() {
        let segment = Segment {
            after_position: String::new(),
            records: vec![StoredRecord {
                position: "1-0".to_string(),
                record: b"r".to_vec(),
            }],
        };
        let bytes = segment.encode();
        let mut flipped = bytes.clone();
        flipped[6] ^= 1;
        assert!(Segment::decode("s", &flipped).is_err());
        assert!(Segment::decode("s", &bytes[..bytes.len() - 1]).is_err());
        assert!(Segment::decode("s", b"TSR1\x01").is_err());
        assert!(Segment::decode("s", b"").is_err());
    }
}
