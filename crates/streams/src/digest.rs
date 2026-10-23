//! The content digest of a batch. Each dedupe entry keeps it, so a resend with the same
//! `sequence` and other content fails instead of going missing. The server computes the same
//! bytes, and `testdata/digests.json` pins them for every SDK.

use crate::proto::AppendRecord;
use sha2::{Digest, Sha256};

/// SHA-256 over the Records in order, as 64 lowercase hex characters. Each part has its length
/// before it, so two batches never give the same bytes. The metadata goes in key order, since a
/// map has no order on the wire.
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
