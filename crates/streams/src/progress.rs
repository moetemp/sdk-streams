//! Progress of a Workflow's Nexus operations. The server puts each progress on a Workflow Task's
//! scheduled event as an attachment, keyed by the operation's endpoint, service, operation name
//! and token. Lang folds the progress of one task and hands it to its started operations. This is
//! the reference fold and delivery. Every lang must give the results of
//! `testdata/nexus_progress_folds.json`.

use crate::protos::temporal::api::nexus::v1::{
    NexusOperationProgress,
    nexus_operation_progress::{Operation, OperationKey},
};
use std::collections::{BTreeMap, HashMap};

/// The type URL of an attachment that holds a [NexusOperationProgress].
pub const NEXUS_OPERATION_PROGRESS_TYPE_URL: &str =
    "type.googleapis.com/temporal.api.nexus.v1.NexusOperationProgress";

/// The key of an operation, as a value a map can hold. A token is unique only within one handler,
/// so the key holds all four parts.
type Key = (String, String, String, String);

fn key_of(key: &OperationKey) -> Key {
    (
        key.endpoint.clone(),
        key.service.clone(),
        key.operation.clone(),
        key.operation_token.clone(),
    )
}

/// The key `progress` names, if it names one.
fn progress_key(progress: &NexusOperationProgress) -> Option<Key> {
    match &progress.operation {
        Some(Operation::OperationKey(key)) => Some(key_of(key)),
        None => None,
    }
}

/// Folds `progress` into `folded`, which keeps one progress per operation key, in order of first
/// appearance. A later progress replaces the held one only when its counter is higher.
pub fn fold_nexus_progress(
    folded: &mut Vec<NexusOperationProgress>,
    progress: &NexusOperationProgress,
) {
    match folded
        .iter_mut()
        .find(|held| held.operation == progress.operation)
    {
        Some(held) if progress.counter > held.counter => *held = progress.clone(),
        Some(_) => {}
        None => folded.push(progress.clone()),
    }
}

/// The folded progress of a Workflow Task's attachments, in event order. Other attachments are
/// skipped. An attachment with the progress type URL that does not decode is an error, since the
/// server wrote it.
pub fn fold_nexus_progresses(
    attachments: &[prost_types::Any],
) -> Result<Vec<NexusOperationProgress>, prost::DecodeError> {
    let mut folded = vec![];
    for attachment in attachments {
        if attachment.type_url != NEXUS_OPERATION_PROGRESS_TYPE_URL {
            continue;
        }
        let progress =
            <NexusOperationProgress as prost::Message>::decode(attachment.value.as_slice())?;
        fold_nexus_progress(&mut folded, &progress);
    }
    Ok(folded)
}

/// One Nexus operation that a Workflow scheduled and that has not resolved.
#[derive(Debug, Clone)]
struct Scheduled {
    endpoint: String,
    service: String,
    operation: String,
    /// Set once the operation starts asynchronously.
    token: Option<String>,
}

/// What lang knows of a Workflow's Nexus operations, so that it can hand progress to them.
///
/// Lang feeds it in activation order: each `ScheduleNexusOperation` command it sends, each
/// `ResolveNexusOperationStart` and `ResolveNexusOperation` job, and the folded progress of each
/// `WorkflowTaskAttachments` job. The attachments job comes after the task's resolutions, so an
/// operation that resolves in the same task gets no progress, the same way live and on replay.
#[derive(Debug, Clone, Default)]
pub struct NexusProgressOperations {
    /// The unresolved operations by the sequence number of their schedule command.
    scheduled: BTreeMap<u32, Scheduled>,
    /// The highest counter handed out per operation key, so a later task never moves it back.
    delivered: HashMap<Key, i64>,
}

impl NexusProgressOperations {
    /// Records the operation lang scheduled as `seq`.
    pub fn schedule(&mut self, seq: u32, endpoint: &str, service: &str, operation: &str) {
        self.scheduled.insert(
            seq,
            Scheduled {
                endpoint: endpoint.to_string(),
                service: service.to_string(),
                operation: operation.to_string(),
                token: None,
            },
        );
    }

    /// Records that the operation `seq` started asynchronously with `operation_token`.
    pub fn start(&mut self, seq: u32, operation_token: &str) {
        if let Some(scheduled) = self.scheduled.get_mut(&seq) {
            scheduled.token = Some(operation_token.to_string());
        }
    }

    /// Records that the operation `seq` resolved. It gets no progress after this.
    pub fn resolve(&mut self, seq: u32) {
        self.scheduled.remove(&seq);
    }

    /// Hands each of `folded` to every started, unresolved operation with its key, when its
    /// counter is higher than any handed out for that key before. Answers the sequence number
    /// and the progress of each delivery, in the order of `folded` and then of the sequence
    /// numbers. Two operations with one key are one handler operation, so both get it.
    pub fn deliver(
        &mut self,
        folded: &[NexusOperationProgress],
    ) -> Vec<(u32, NexusOperationProgress)> {
        let mut deliveries = vec![];
        for progress in folded {
            let Some(key) = progress_key(progress) else {
                continue;
            };
            if self
                .delivered
                .get(&key)
                .is_some_and(|&held| progress.counter <= held)
            {
                continue;
            }
            let seqs: Vec<u32> = self
                .scheduled
                .iter()
                .filter(|(_, scheduled)| {
                    scheduled.token.as_deref() == Some(key.3.as_str())
                        && scheduled.endpoint == key.0
                        && scheduled.service == key.1
                        && scheduled.operation == key.2
                })
                .map(|(&seq, _)| seq)
                .collect();
            if seqs.is_empty() {
                continue;
            }
            self.delivered.insert(key, progress.counter);
            deliveries.extend(seqs.into_iter().map(|seq| (seq, progress.clone())));
        }
        deliveries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn progress(token: &str, counter: i64) -> NexusOperationProgress {
        NexusOperationProgress {
            operation: Some(Operation::OperationKey(OperationKey {
                endpoint: "endpoint".to_string(),
                service: "service".to_string(),
                operation: "operation".to_string(),
                operation_token: token.to_string(),
            })),
            position: format!("position-{counter}"),
            counter,
            ..Default::default()
        }
    }

    fn attachment(type_url: &str, progress: &NexusOperationProgress) -> prost_types::Any {
        prost_types::Any {
            type_url: type_url.to_string(),
            value: progress.encode_to_vec(),
        }
    }

    #[test]
    fn attachments_fold_in_event_order_and_others_are_skipped() {
        let attachments = [
            attachment(NEXUS_OPERATION_PROGRESS_TYPE_URL, &progress("a", 5)),
            attachment(
                "type.googleapis.com/temporal.api.stream.v1.StreamWake",
                &progress("b", 9),
            ),
            attachment(NEXUS_OPERATION_PROGRESS_TYPE_URL, &progress("b", 1)),
            attachment(NEXUS_OPERATION_PROGRESS_TYPE_URL, &progress("a", 7)),
        ];
        assert_eq!(
            fold_nexus_progresses(&attachments).unwrap(),
            vec![progress("a", 7), progress("b", 1)]
        );
    }

    #[test]
    fn a_progress_that_does_not_decode_is_an_error() {
        let broken = prost_types::Any {
            type_url: NEXUS_OPERATION_PROGRESS_TYPE_URL.to_string(),
            value: vec![0xff],
        };
        assert!(fold_nexus_progresses(&[broken]).is_err());
    }

    // The server reads the key by its field numbers, and stored History holds these bytes, so a
    // renumbered field would key progress to no operation on replay.
    #[test]
    fn the_operation_key_keeps_its_field_numbers() {
        let progress = NexusOperationProgress {
            operation: Some(Operation::OperationKey(OperationKey {
                endpoint: "e".to_string(),
                service: "s".to_string(),
                operation: "o".to_string(),
                operation_token: "t".to_string(),
            })),
            counter: 1,
            ..Default::default()
        };
        assert_eq!(
            progress.encode_to_vec(),
            vec![
                0x18, 0x01, 0x2a, 0x0c, 0x0a, 0x01, b'e', 0x12, 0x01, b's', 0x1a, 0x01, b'o', 0x22,
                0x01, b't'
            ]
        );
    }
}
