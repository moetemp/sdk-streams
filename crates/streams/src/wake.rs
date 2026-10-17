//! Stream wakes for a Workflow's readers. The server puts each wake on a Workflow Task's scheduled
//! event as an attachment, and lang folds the wakes of one task per subscription. This is the
//! reference fold. Every lang's fold must give the results of `testdata/wake_folds.json`.

use crate::protos::temporal::api::stream::v1::StreamWake;

/// The type URL of an attachment that holds a [StreamWake].
pub const STREAM_WAKE_TYPE_URL: &str = "type.googleapis.com/temporal.api.stream.v1.StreamWake";

/// Folds `wake` into `folded`, which keeps one wake per subscription, in order of first
/// appearance. The highest counter keeps its position and metadata. A recheck, a close or a
/// resubscribe on any of the folded wakes stays, since each one asks the reader to act whatever
/// the counter. The first close keeps its outcome, since a stream closes once.
pub fn fold_stream_wake(folded: &mut Vec<StreamWake>, wake: &StreamWake) {
    let Some(held) = folded
        .iter_mut()
        .find(|held| held.subscription_id == wake.subscription_id)
    else {
        folded.push(wake.clone());
        return;
    };
    if wake.counter > held.counter {
        held.counter = wake.counter;
        held.position = wake.position.clone();
        held.metadata = wake.metadata.clone();
    }
    held.recheck |= wake.recheck;
    held.resubscribe |= wake.resubscribe;
    if wake.closed && !held.closed {
        held.closed = true;
        held.close_result = wake.close_result.clone();
        held.close_failure = wake.close_failure.clone();
    }
}

/// The folded wakes of a Workflow Task's attachments, in event order. Other attachments are
/// skipped. An attachment with the wake type URL that does not decode is an error, since the
/// server wrote it.
pub fn fold_stream_wakes(
    attachments: &[prost_types::Any],
) -> Result<Vec<StreamWake>, prost::DecodeError> {
    let mut folded = vec![];
    for attachment in attachments {
        if attachment.type_url != STREAM_WAKE_TYPE_URL {
            continue;
        }
        let wake = <StreamWake as prost::Message>::decode(attachment.value.as_slice())?;
        fold_stream_wake(&mut folded, &wake);
    }
    Ok(folded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn wake(subscription_id: &str, counter: i64) -> StreamWake {
        StreamWake {
            subscription_id: subscription_id.to_string(),
            counter,
            ..Default::default()
        }
    }

    fn attachment(type_url: &str, wake: &StreamWake) -> prost_types::Any {
        prost_types::Any {
            type_url: type_url.to_string(),
            value: wake.encode_to_vec(),
        }
    }

    #[test]
    fn attachments_fold_in_event_order_and_others_are_skipped() {
        let attachments = [
            attachment(STREAM_WAKE_TYPE_URL, &wake("a", 5)),
            attachment(
                "type.googleapis.com/temporal.api.nexus.v1.Other",
                &wake("b", 9),
            ),
            attachment(STREAM_WAKE_TYPE_URL, &wake("b", 1)),
            attachment(STREAM_WAKE_TYPE_URL, &wake("a", 7)),
        ];
        assert_eq!(
            fold_stream_wakes(&attachments).unwrap(),
            vec![wake("a", 7), wake("b", 1)]
        );
    }

    #[test]
    fn a_wake_that_does_not_decode_is_an_error() {
        let broken = prost_types::Any {
            type_url: STREAM_WAKE_TYPE_URL.to_string(),
            value: vec![0xff],
        };
        assert!(fold_stream_wakes(&[broken]).is_err());
    }
}
