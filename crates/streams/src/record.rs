//! Building the records a store keeps, and naming who wrote them.

use crate::{
    CONTENT_HASH_KEY, Capabilities, METADATA_ENCODING, StreamError, StreamResult,
    content_hash_text,
    proto::{ActivityProducer, AppendRecord, StreamLink, StreamRecord, StreamRecordKind},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;

/// The owner kind a Workflow's streams hash under in their cursors.
pub const WORKFLOW_OWNER_KIND: &str = "workflow";

const HASH_LENGTH: usize = 32;

/// The producer id an Activity attempt writes as.
///
/// The scheduling run is part of it, so the same Activity id in another run of the chain is a
/// different producer, and a retry of the Activity keeps its id and raises the attempt.
pub fn activity_producer_id(activity: &ActivityProducer) -> String {
    format!("{}@{}", activity.activity_id, activity.run_id)
}

/// How a Local Activity call's producer id starts, `__temporal_local_<n>@<run id>`.
pub(crate) const LOCAL_PRODUCER_PREFIX: &str = "__temporal_local_";

/// How many completed tasks a record of a Workflow run names. A reader learns from them whether
/// an earlier attempt it delivered completed, as long as fewer tasks completed since.
pub const STREAM_COMPLETED_TASKS: usize = 8;

/// The completed tasks a record names, from an activation's list, with a trailing zero when the
/// list goes back to the run's start.
pub(crate) fn completed_tasks(completed_started_event_ids: &[i64]) -> Vec<i64> {
    let mut ids = completed_started_event_ids.to_vec();
    if ids.len() < STREAM_COMPLETED_TASKS {
        ids.push(0);
    }
    ids
}

/// The producer id a Workflow run writes its own records as.
///
/// It is an Activity's id form with no Activity id. An Activity id is never empty, so no Activity
/// writes as a run. Each run of a chain is a producer of its own, so Continue-as-New and a reset
/// start a new producer rather than superseding the earlier run.
pub fn workflow_producer_id(run_id: &str) -> String {
    format!("@{run_id}")
}

/// The digest of a batch of a Workflow's records on `topic`, from each record's kind and content
/// hash, so it never depends on the codec. A Workflow's records reach the service after the codec,
/// and a task that runs again must repeat the digest of the same records.
///
/// It is the SHA-256 over each record as its length in 8 bytes big-endian, then the record as its
/// topic's length in 8 bytes big-endian, the topic's UTF-8 bytes, its kind in 4 bytes big-endian,
/// its content hash's length in 8 bytes big-endian and the content hash. A LINK record then adds
/// each field of its link as its length in 8 bytes big-endian and its bytes: the event in 4 bytes
/// big-endian, the stream's owner kind in 4 bytes big-endian, its Workflow id, run id, topic and
/// Activity id, then the name, `after`, `until`, the namespace and the endpoint. A record with no
/// link adds nothing, so its digest is as before links.
pub fn workflow_batch_digest<'a>(
    topic: &str,
    records: impl IntoIterator<Item = (i32, &'a [u8], Option<&'a StreamLink>)>,
) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for (kind, content_hash, link) in records {
        let fields = link.map(link_fields).unwrap_or_default();
        let length = 8
            + topic.len()
            + 4
            + 8
            + content_hash.len()
            + fields.iter().map(|field| 8 + field.len()).sum::<usize>();
        digest.update((length as u64).to_be_bytes());
        digest.update((topic.len() as u64).to_be_bytes());
        digest.update(topic.as_bytes());
        digest.update(kind.to_be_bytes());
        digest.update((content_hash.len() as u64).to_be_bytes());
        digest.update(content_hash);
        for field in fields {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field);
        }
    }
    digest.finalize().to_vec()
}

/// Refuses an append batch digest that isn't the 32-byte SHA-256 lang takes before the codec.
///
/// Stores compare it unchanged when a producer repeats its newest batch, so a digest the service
/// made another way would turn a retry into a divergent write.
pub fn check_append_digest(digest: &[u8]) -> StreamResult<()> {
    if digest.len() != HASH_LENGTH {
        return Err(StreamError::invalid_argument(format!(
            "an append needs the {HASH_LENGTH}-byte digest lang takes over the batch before the \
             codec, not {} bytes",
            digest.len()
        )));
    }
    Ok(())
}

/// The record a store keeps for one appended record of a producer.
///
/// Fails as an invalid argument when the record is one no writer stores: a kind other than DATA,
/// FINISH or LINK, a DATA record without a body or a 32-byte content hash, a FINISH record with a
/// body, a LINK record without a valid link, or a link on another kind.
pub fn stored_append_record(
    topic: &str,
    producer_id: &str,
    attempt: i64,
    sequence: i64,
    record: &AppendRecord,
) -> StreamResult<StreamRecord> {
    let mut stored = stored_record(
        topic,
        record.kind,
        record.body.clone(),
        &record.content_hash,
        record.link.as_ref(),
    )?;
    stored.producer_id = producer_id.to_string();
    stored.attempt = attempt;
    stored.sequence = sequence;
    Ok(stored)
}

fn stored_record(
    topic: &str,
    kind: i32,
    body: Option<Payload>,
    content_hash: &[u8],
    link: Option<&StreamLink>,
) -> StreamResult<StreamRecord> {
    if topic.is_empty() {
        return Err(StreamError::invalid_argument(
            "a stream record needs a topic",
        ));
    }
    let is_link = kind == StreamRecordKind::Link as i32;
    if link.is_some() != is_link {
        return Err(StreamError::invalid_argument(
            "a LINK record needs its link, and no other kind carries one",
        ));
    }
    let mut stored = StreamRecord {
        topic: topic.to_string(),
        kind,
        ..Default::default()
    };
    match StreamRecordKind::try_from(kind) {
        Ok(StreamRecordKind::Data) => {
            let Some(body) = body else {
                return Err(StreamError::invalid_argument("a DATA record needs a body"));
            };
            if content_hash.len() != HASH_LENGTH {
                return Err(StreamError::invalid_argument(format!(
                    "a DATA record needs the {HASH_LENGTH}-byte SHA-256 of its plaintext body, \
                     not {} bytes",
                    content_hash.len()
                )));
            }
            stored.metadata.insert(
                CONTENT_HASH_KEY.to_string(),
                metadata_payload(content_hash_text(content_hash).as_bytes()),
            );
            stored.body = Some(body);
        }
        Ok(StreamRecordKind::Finish) => {
            if body.is_some() || !content_hash.is_empty() {
                return Err(StreamError::invalid_argument(
                    "a FINISH record carries no body",
                ));
            }
        }
        Ok(StreamRecordKind::Link) => {
            let link = link.expect("checked above");
            crate::conventions::check_link(link).map_err(StreamError::invalid_argument)?;
            match (body, content_hash.len()) {
                (None, 0) => {}
                (Some(body), HASH_LENGTH) => {
                    stored.metadata.insert(
                        CONTENT_HASH_KEY.to_string(),
                        metadata_payload(content_hash_text(content_hash).as_bytes()),
                    );
                    stored.body = Some(body);
                }
                (_, length) => {
                    return Err(StreamError::invalid_argument(format!(
                        "a LINK record's body needs the {HASH_LENGTH}-byte SHA-256 of its \
                         plaintext, and no body takes no hash, not {length} bytes"
                    )));
                }
            }
            stored.link = Some(link.clone());
        }
        _ => {
            return Err(StreamError::invalid_argument(format!(
                "a writer stores DATA, FINISH or LINK records, not kind {kind}"
            )));
        }
    }
    Ok(stored)
}

/// Each field of a link, as the digest takes them, every one length-prefixed by its caller.
pub(crate) fn link_fields(link: &StreamLink) -> Vec<Vec<u8>> {
    let stream = link.stream.clone().unwrap_or_default();
    vec![
        link.event.to_be_bytes().to_vec(),
        stream.owner_kind.to_be_bytes().to_vec(),
        stream.workflow_id.into_bytes(),
        stream.run_id.into_bytes(),
        stream.topic.into_bytes(),
        stream.activity_id.into_bytes(),
        link.name.as_bytes().to_vec(),
        link.after.as_bytes().to_vec(),
        link.until.as_bytes().to_vec(),
        link.namespace.as_bytes().to_vec(),
        link.endpoint.as_bytes().to_vec(),
    ]
}

/// Refuses a batch of serialized records with a record above the store's record limit.
pub fn check_record_limit(records: &[Vec<u8>], capabilities: &Capabilities) -> StreamResult<()> {
    let limit = capabilities.max_record_bytes;
    if let Some((index, record)) = records.iter().enumerate().find(|(_, r)| r.len() > limit) {
        return Err(StreamError::invalid_argument(format!(
            "record {index} takes {} bytes in the store, above the store's record limit of \
             {limit} bytes",
            record.len()
        )));
    }
    Ok(())
}

/// Refuses a batch above the store's record or batch limit. The service doesn't split a producer's
/// batch, since lang numbered its records as one sequence range.
pub fn check_store_limits(records: &[Vec<u8>], capabilities: &Capabilities) -> StreamResult<()> {
    check_record_limit(records, capabilities)?;
    let bytes: usize = records.iter().map(Vec::len).sum();
    let limit = capabilities.max_batch_bytes;
    if bytes > limit {
        return Err(StreamError::invalid_argument(format!(
            "the batch takes {bytes} bytes in the store, above the store's batch limit of {limit} \
             bytes"
        )));
    }
    Ok(())
}

/// A `CLOSE` record that `producer_id` at `attempt` writes to end `topic` for every reader.
pub fn close_record(topic: &str, producer_id: &str, attempt: i64) -> StreamRecord {
    StreamRecord {
        topic: topic.to_string(),
        kind: StreamRecordKind::Close as i32,
        producer_id: producer_id.to_string(),
        attempt,
        ..Default::default()
    }
}

/// A `TOPIC` record that `producer_id` at `attempt` writes to the chain's control log before its
/// first record on `topic`.
pub fn topic_record(topic: &str, producer_id: &str, attempt: i64) -> StreamRecord {
    StreamRecord {
        topic: topic.to_string(),
        kind: StreamRecordKind::Topic as i32,
        producer_id: producer_id.to_string(),
        attempt,
        ..Default::default()
    }
}

fn metadata_payload(data: &[u8]) -> Payload {
    Payload {
        metadata: [("encoding".to_string(), METADATA_ENCODING.to_vec())].into(),
        data: data.to_vec(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::StreamFailureKind;

    fn body() -> Payload {
        Payload {
            metadata: [("encoding".to_string(), b"binary/encrypted".to_vec())].into(),
            data: b"ciphertext".to_vec(),
            ..Default::default()
        }
    }

    fn data(hash: u8) -> AppendRecord {
        AppendRecord {
            kind: StreamRecordKind::Data as i32,
            body: Some(body()),
            content_hash: vec![hash; HASH_LENGTH],
            ..Default::default()
        }
    }

    fn finish() -> AppendRecord {
        AppendRecord {
            kind: StreamRecordKind::Finish as i32,
            ..Default::default()
        }
    }

    #[test]
    fn an_activity_writes_as_its_id_at_its_run() {
        let activity = ActivityProducer {
            workflow_id: "wf".to_string(),
            run_id: "run-1".to_string(),
            activity_id: "fetch".to_string(),
            attempt: 3,
        };
        assert_eq!(activity_producer_id(&activity), "fetch@run-1");
    }

    #[test]
    fn a_workflow_run_writes_as_no_activity_can() {
        let activity = ActivityProducer {
            activity_id: "x".to_string(),
            run_id: "run-1".to_string(),
            ..Default::default()
        };
        assert_eq!(workflow_producer_id("run-1"), "@run-1");
        assert_ne!(
            workflow_producer_id("run-1"),
            activity_producer_id(&activity)
        );
    }

    #[test]
    fn a_workflow_batch_digest_sees_order_kind_and_hash() {
        let data = StreamRecordKind::Data as i32;
        let finish = StreamRecordKind::Finish as i32;
        let base =
            workflow_batch_digest("t", [(data, &[1u8; 32][..], None), (finish, &[][..], None)]);
        assert_eq!(base.len(), HASH_LENGTH);
        assert_eq!(check_append_digest(&base), Ok(()));
        for other in [
            workflow_batch_digest("t", [(finish, &[][..], None), (data, &[1u8; 32][..], None)]),
            workflow_batch_digest("t", [(data, &[2u8; 32][..], None), (finish, &[][..], None)]),
            workflow_batch_digest("u", [(data, &[1u8; 32][..], None), (finish, &[][..], None)]),
            workflow_batch_digest("t", [(data, &[1u8; 32][..], None)]),
        ] {
            assert_ne!(other, base);
        }
        assert_eq!(
            workflow_batch_digest("t", [(data, &[1u8; 32][..], None), (finish, &[][..], None)]),
            base
        );
    }

    #[test]
    fn a_stored_data_record_carries_its_plaintext_hash() {
        let stored = stored_append_record("out", "p", 2, 5, &data(0xab)).unwrap();
        assert_eq!(stored.body, Some(body()));
        assert_eq!(
            stored.metadata[CONTENT_HASH_KEY].data,
            "ab".repeat(HASH_LENGTH).into_bytes()
        );
        assert_eq!(
            stored.metadata[CONTENT_HASH_KEY].metadata["encoding"],
            b"binary/plain"
        );
        assert_eq!(
            (stored.producer_id.as_str(), stored.attempt, stored.sequence),
            ("p", 2, 5)
        );
    }

    #[test]
    fn records_no_writer_stores_are_refused() {
        let mut no_body = data(1);
        no_body.body = None;
        let mut short_hash = data(1);
        short_hash.content_hash.truncate(4);
        let mut finish_with_body = finish();
        finish_with_body.body = Some(body());
        let unspecified = AppendRecord {
            kind: StreamRecordKind::Unspecified as i32,
            ..data(1)
        };
        for (record, expected) in [
            (no_body, "needs a body"),
            (short_hash, "not 4 bytes"),
            (finish_with_body, "carries no body"),
            (unspecified, "not kind 0"),
        ] {
            let error = stored_append_record("out", "p", 1, 1, &record).unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument);
            assert!(error.message.contains(expected), "{expected}: {error}");
        }
        let error = stored_append_record("", "p", 1, 1, &data(1)).unwrap_err();
        assert!(error.message.contains("needs a topic"));
    }

    #[test]
    fn an_append_needs_a_full_digest() {
        assert_eq!(check_append_digest(&[9; HASH_LENGTH]), Ok(()));
        for digest in [vec![], vec![9; 16]] {
            let error = check_append_digest(&digest).unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument);
            assert!(error.message.contains("32-byte digest"), "{error}");
        }
    }

    fn open_link() -> StreamLink {
        use crate::protos::temporal::api::{
            enums::v1::StreamOwnerKind as Kind, stream::v1::StreamReference,
        };
        StreamLink {
            event: crate::proto::stream_link::Event::Open as i32,
            stream: Some(StreamReference {
                owner_kind: Kind::Workflow as i32,
                workflow_id: "agent-child".to_string(),
                topic: "events".to_string(),
                ..Default::default()
            }),
            name: "child#1".to_string(),
            ..Default::default()
        }
    }

    fn link(link: StreamLink) -> AppendRecord {
        AppendRecord {
            kind: StreamRecordKind::Link as i32,
            link: Some(link),
            ..Default::default()
        }
    }

    #[test]
    fn a_link_is_stored_with_its_reference_in_plaintext() {
        let stored = stored_append_record("events", "p", 1, 1, &link(open_link())).unwrap();
        assert_eq!(stored.kind, StreamRecordKind::Link as i32);
        assert_eq!(stored.link, Some(open_link()));
        assert!(stored.body.is_none() && stored.metadata.is_empty());
        // A label may ride in the body, through the codec like any value.
        let labeled = AppendRecord {
            body: Some(body()),
            content_hash: vec![7; HASH_LENGTH],
            ..link(open_link())
        };
        let stored = stored_append_record("events", "p", 1, 2, &labeled).unwrap();
        assert_eq!(stored.body, Some(body()));
        assert!(stored.metadata.contains_key(CONTENT_HASH_KEY));
        let join = StreamLink {
            event: crate::proto::stream_link::Event::Join as i32,
            name: "child#1".to_string(),
            until: "cursor".to_string(),
            ..Default::default()
        };
        assert!(stored_append_record("events", "p", 1, 3, &link(join)).is_ok());
    }

    #[test]
    fn a_link_no_follower_can_use_is_refused() {
        use crate::proto::stream_link::Event;
        use crate::protos::temporal::api::enums::v1::StreamOwnerKind as Kind;
        let with = |change: fn(&mut StreamLink)| {
            let mut link = open_link();
            change(&mut link);
            link
        };
        let cases: Vec<(AppendRecord, &str)> = vec![
            (link(with(|l| l.name.clear())), "needs a name"),
            (link(with(|l| l.name = "n".repeat(257))), "needs a name"),
            (
                link(with(|l| l.event = Event::Unspecified as i32)),
                "OPEN or JOIN",
            ),
            (link(with(|l| l.stream = None)), "needs the stream"),
            (link(with(|l| l.until = "c".to_string())), "names no until"),
            (
                link(with(|l| l.stream.as_mut().unwrap().workflow_id.clear())),
                "needs the Workflow id",
            ),
            (
                link(with(|l| {
                    l.stream.as_mut().unwrap().activity_id = "tool-1".to_string();
                })),
                "no Activity id",
            ),
            (
                link(with(|l| {
                    l.stream.as_mut().unwrap().owner_kind = Kind::WorkflowActivity as i32;
                })),
                "not one of owner kind 3",
            ),
            (
                link(with(|l| {
                    l.stream.as_mut().unwrap().owner_kind = Kind::Activity as i32;
                })),
                "not one of owner kind 2",
            ),
            (
                link(with(|l| l.stream.as_mut().unwrap().topic.clear())),
                "needs a topic",
            ),
            (
                link(with(|l| l.after = "c".repeat(1025))),
                "after is at most",
            ),
            (
                link(with(|l| {
                    l.event = Event::Join as i32;
                })),
                "only the OPEN it joins",
            ),
            (
                AppendRecord {
                    link: Some(open_link()),
                    ..data(1)
                },
                "no other kind carries one",
            ),
            (
                AppendRecord {
                    link: None,
                    ..link(open_link())
                },
                "needs its link",
            ),
            (
                AppendRecord {
                    body: Some(body()),
                    ..link(open_link())
                },
                "not 0 bytes",
            ),
        ];
        for (record, expected) in cases {
            let error = stored_append_record("events", "p", 1, 1, &record).unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument);
            assert!(error.message.contains(expected), "{expected}: {error}");
        }
    }

    #[test]
    fn a_workflow_batch_digest_sees_every_link_field() {
        let kind = StreamRecordKind::Link as i32;
        let base_link = open_link();
        let digest = |link: &StreamLink| workflow_batch_digest("t", [(kind, &[][..], Some(link))]);
        let base = digest(&base_link);
        // A record with no link keeps the digest it had before links.
        assert_ne!(base, workflow_batch_digest("t", [(kind, &[][..], None)]));
        let changes: [fn(&mut StreamLink); 11] = [
            |l| l.event = crate::proto::stream_link::Event::Join as i32,
            |l| l.stream.as_mut().unwrap().owner_kind = 3,
            |l| l.stream.as_mut().unwrap().workflow_id.push('x'),
            |l| l.stream.as_mut().unwrap().run_id.push('x'),
            |l| l.stream.as_mut().unwrap().topic.push('x'),
            |l| l.stream.as_mut().unwrap().activity_id.push('x'),
            |l| l.name.push('x'),
            |l| l.after.push('x'),
            |l| l.until.push('x'),
            |l| l.namespace.push('x'),
            |l| l.endpoint.push('x'),
        ];
        for change in changes {
            let mut other = base_link.clone();
            change(&mut other);
            assert_ne!(digest(&other), base, "{other:?}");
        }
        // Length prefixes keep a byte moved from one field to the next apart.
        let mut moved = base_link.clone();
        moved.name = "child#".to_string();
        moved.after = "1".to_string();
        assert_ne!(digest(&moved), base);
    }

    #[test]
    fn the_batch_digests_match_the_shared_vectors() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../testdata/workflow_attempts.json")).unwrap();
        let cases = vectors["batch_digests"].as_array().unwrap();
        assert!(!cases.is_empty());
        let hex = |value: &serde_json::Value| -> Vec<u8> {
            let text = value.as_str().unwrap_or("");
            (0..text.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
                .collect()
        };
        for case in cases {
            let records: Vec<(i32, Vec<u8>, Option<StreamLink>)> = case["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|record| {
                    let link = record.get("link").map(|link| {
                        let text = |field: &str| link[field].as_str().unwrap_or("").to_string();
                        let stream = link.get("stream").map(|stream| {
                            let text =
                                |field: &str| stream[field].as_str().unwrap_or("").to_string();
                            crate::protos::temporal::api::stream::v1::StreamReference {
                                owner_kind: stream["owner_kind"].as_i64().unwrap() as i32,
                                workflow_id: text("workflow_id"),
                                run_id: text("run_id"),
                                topic: text("topic"),
                                activity_id: text("activity_id"),
                            }
                        });
                        StreamLink {
                            event: link["event"].as_i64().unwrap() as i32,
                            stream,
                            name: text("name"),
                            after: text("after"),
                            until: text("until"),
                            namespace: text("namespace"),
                            endpoint: text("endpoint"),
                        }
                    });
                    (
                        record["kind"].as_i64().unwrap() as i32,
                        hex(&record["content_hash"]),
                        link,
                    )
                })
                .collect();
            let digest = workflow_batch_digest(
                case["topic"].as_str().unwrap(),
                records
                    .iter()
                    .map(|(kind, hash, link)| (*kind, hash.as_slice(), link.as_ref())),
            );
            assert_eq!(
                content_hash_text(&digest),
                case["digest"].as_str().unwrap(),
                "{case}"
            );
        }
    }
}
