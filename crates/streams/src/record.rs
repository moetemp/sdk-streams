//! Building the records a store keeps, and naming who wrote them.

use crate::{
    Capabilities, StreamError, StreamResult,
    proto::{ActivityProducer, AppendRecord, StreamRecord, StreamRecordKind},
};
use temporalio_common::{
    protos::temporal::api::common::v1::Payload,
    streams::{CONTENT_HASH_KEY, METADATA_ENCODING, content_hash_text},
};

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

/// The producer id a Workflow run writes its own records as.
///
/// It is an Activity's id form with no Activity id. An Activity id is never empty, so no Activity
/// writes as a run. Each run of a chain is a producer of its own, so Continue-as-New and a reset
/// start a new producer rather than superseding the earlier run.
pub fn workflow_producer_id(run_id: &str) -> String {
    format!("@{run_id}")
}

/// The digest of a batch of a Workflow's records on `topic`, from each record's kind and content
/// hash, so it never depends on the codec. A Workflow's records reach Core after the codec, and a
/// task that runs again must repeat the digest of the same records.
///
/// It is the SHA-256 over each record as its length in 8 bytes big-endian, then the record as its
/// topic's length in 8 bytes big-endian, the topic's UTF-8 bytes, its kind in 4 bytes big-endian,
/// its content hash's length in 8 bytes big-endian and the content hash.
pub fn workflow_batch_digest<'a>(
    topic: &str,
    records: impl IntoIterator<Item = (i32, &'a [u8])>,
) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for (kind, content_hash) in records {
        let length = 8 + topic.len() + 4 + 8 + content_hash.len();
        digest.update((length as u64).to_be_bytes());
        digest.update((topic.len() as u64).to_be_bytes());
        digest.update(topic.as_bytes());
        digest.update(kind.to_be_bytes());
        digest.update((content_hash.len() as u64).to_be_bytes());
        digest.update(content_hash);
    }
    digest.finalize().to_vec()
}

/// Refuses an append batch digest that isn't the 32-byte SHA-256 lang takes before the codec.
///
/// Stores compare it unchanged when a producer repeats its newest batch, so a digest Core made
/// another way would turn a retry into a divergent write.
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
/// Fails as an invalid argument when the record is one no writer stores: a kind other than DATA
/// or FINISH, a DATA record without a body or a 32-byte content hash, or a FINISH record with a
/// body.
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
) -> StreamResult<StreamRecord> {
    if topic.is_empty() {
        return Err(StreamError::invalid_argument(
            "a stream record needs a topic",
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
        _ => {
            return Err(StreamError::invalid_argument(format!(
                "a writer stores DATA or FINISH records, not kind {kind}"
            )));
        }
    }
    Ok(stored)
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

/// Refuses a batch above the store's record or batch limit. Core doesn't split a producer's
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
        let base = workflow_batch_digest("t", [(data, &[1u8; 32][..]), (finish, &[][..])]);
        assert_eq!(base.len(), HASH_LENGTH);
        assert_eq!(check_append_digest(&base), Ok(()));
        for other in [
            workflow_batch_digest("t", [(finish, &[][..]), (data, &[1u8; 32][..])]),
            workflow_batch_digest("t", [(data, &[2u8; 32][..]), (finish, &[][..])]),
            workflow_batch_digest("u", [(data, &[1u8; 32][..]), (finish, &[][..])]),
            workflow_batch_digest("t", [(data, &[1u8; 32][..])]),
        ] {
            assert_ne!(other, base);
        }
        assert_eq!(
            workflow_batch_digest("t", [(data, &[1u8; 32][..]), (finish, &[][..])]),
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
}
