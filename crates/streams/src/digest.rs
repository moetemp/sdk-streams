//! The content digest of a batch. Each dedupe entry stores it, so that a resend with the same
//! `sequence` and other content fails instead of being dropped without an error. The server
//! computes the same digest, and `testdata/digests.json` has the expected values.

use crate::proto::AppendRecord;
use sha2::{Digest, Sha256};

/// SHA-256 over the Records in order, as 64 lowercase hex characters. Each part has its length
/// before it, so that two different batches never hash the same bytes. The metadata is hashed in
/// key order, since a map has no order on the wire.
pub fn batch_digest(records: &[AppendRecord]) -> String {
    let mut hasher = Sha256::new();
    for record in records {
        part(&mut hasher, record.topic.as_bytes());
        let mut metadata: Vec<_> = record
            .body
            .iter()
            .flat_map(|body| body.metadata.iter())
            .collect();
        metadata.sort();
        hasher.update((metadata.len() as u64).to_be_bytes());
        for (key, value) in metadata {
            part(&mut hasher, key.as_bytes());
            part(&mut hasher, value);
        }
        part(
            &mut hasher,
            record.body.as_ref().map_or(&[], |body| &body.data),
        );
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn part(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}
