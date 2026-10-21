//! Producer epochs, their order, and the withdrawal rule of a `Superseded` item.
//!
//! The Lua scripts compare epochs as strings, so an epoch goes to Redis as a key of fixed-width
//! hex fields in the order of comparison. A plain string comparison of two keys then orders the
//! epochs field by field. The shared vectors in `testdata/epochs.json` pin the keys.

use crate::proto::{
    Producer, ProducerEpoch, ProducerKind, StreamRecord, Supersession, WorkflowTaskEpoch,
};
use prost_types::Timestamp;
use std::{
    cmp::Ordering,
    time::{SystemTime, UNIX_EPOCH},
};

fn hex64(value: i64) -> String {
    format!("{:016x}", value.max(0))
}

fn hex32(value: i32) -> String {
    format!("{:08x}", value.max(0))
}

fn time_fields(time: Option<&Timestamp>) -> [String; 2] {
    let (seconds, nanos) = time.map_or((0, 0), |time| (time.seconds, time.nanos));
    [hex64(seconds), hex32(nanos)]
}

/// The key of a Workflow Task attempt: started event id, attempt, started time. It is the start
/// of the epoch key of the run and of the Local Activities that the task dispatched.
pub fn task_key(task: &WorkflowTaskEpoch) -> String {
    let [seconds, nanos] = time_fields(task.started_time.as_ref());
    [
        hex64(task.started_event_id),
        hex32(task.attempt),
        seconds,
        nanos,
    ]
    .join(".")
}

/// The key of an epoch. Fields a kind doesn't use compare as zero.
pub fn epoch_key(epoch: &ProducerEpoch) -> String {
    let [scheduled_seconds, scheduled_nanos] = time_fields(epoch.attempt_scheduled_time.as_ref());
    [
        task_key(&task_of(epoch)),
        scheduled_seconds,
        scheduled_nanos,
        hex32(epoch.attempt),
    ]
    .join(".")
}

/// The Workflow Task fields of an epoch.
pub fn task_of(epoch: &ProducerEpoch) -> WorkflowTaskEpoch {
    WorkflowTaskEpoch {
        started_event_id: epoch.task_started_event_id,
        attempt: epoch.task_attempt,
        started_time: epoch.task_started_time,
    }
}

/// Orders two epochs field by field.
pub fn compare_epochs(a: &ProducerEpoch, b: &ProducerEpoch) -> Ordering {
    epoch_key(a).cmp(&epoch_key(b))
}

/// The started-time part of a task key.
fn time_key(task: &WorkflowTaskEpoch) -> [String; 2] {
    time_fields(task.started_time.as_ref())
}

fn compare_tasks(a: &WorkflowTaskEpoch, b: &WorkflowTaskEpoch) -> Ordering {
    task_key(a).cmp(&task_key(b))
}

/// A proto timestamp of a system time.
pub fn timestamp(time: SystemTime) -> Timestamp {
    let since = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    Timestamp {
        seconds: since.as_secs() as i64,
        nanos: since.subsec_nanos() as i32,
    }
}

/// Whether a `Superseded` item withdraws an earlier Record.
///
/// For an Activity, a Local Activity or a client, it withdraws that Producer's Records with a
/// lower epoch. For a Workflow task t with the previous completed task c, it withdraws the
/// Records of the run and of its Local Activities whose task is in `failed_tasks` or strictly
/// between c and t, and those at c or at t with a lower epoch. A retry run withdraws every
/// Record whose Producer has the previous run.
pub fn withdraws(supersession: &Supersession, record: &StreamRecord) -> bool {
    let Some(producer) = record.producer.as_ref() else {
        return false;
    };
    if !supersession.withdraw_run.is_empty() && producer.run_id == supersession.withdraw_run {
        return true;
    }
    let kind = ProducerKind::try_from(supersession.producer_kind).unwrap_or_default();
    let (Some(epoch), Some(old)) = (supersession.epoch.as_ref(), producer.epoch.as_ref()) else {
        return false;
    };
    if kind != ProducerKind::Workflow {
        return producer.kind == supersession.producer_kind
            && producer.id == supersession.producer_id
            && compare_epochs(old, epoch) == Ordering::Less;
    }
    if !of_run(producer, supersession) {
        return false;
    }
    let (task, old) = (task_of(epoch), task_of(old));
    let started = old.started_event_id;
    if supersession.failed_tasks.contains(&started) {
        return true;
    }
    let completed = supersession.previous_completed_task.as_ref();
    let after = completed.map_or(0, |c| c.started_event_id);
    if after < started && started < task.started_event_id {
        return true;
    }
    let lower_at = |at: &WorkflowTaskEpoch| {
        started == at.started_event_id && compare_tasks(&old, at) == Ordering::Less
    };
    // Core gives c with attempt 0 when it no longer has c's attempt. The attempts of one task
    // start in time order, so the started time still tells the lower ones.
    let lower_at_completed = |c: &WorkflowTaskEpoch| {
        if c.attempt > 0 {
            return lower_at(c);
        }
        started == c.started_event_id && time_key(&old) < time_key(c)
    };
    lower_at(&task) || completed.is_some_and(lower_at_completed)
}

/// Whether a Producer is the Workflow run of a `Superseded` item, or one of its Local
/// Activities.
fn of_run(producer: &Producer, supersession: &Supersession) -> bool {
    let kind = ProducerKind::try_from(producer.kind).unwrap_or_default();
    match kind {
        ProducerKind::Workflow => producer.id == supersession.producer_id,
        ProducerKind::LocalActivity => producer.run_id == supersession.run_id,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(seconds: i64, nanos: i32) -> Option<Timestamp> {
        Some(Timestamp { seconds, nanos })
    }

    #[test]
    fn keys_order_epochs_field_by_field() {
        let epochs = [
            ProducerEpoch::default(),
            ProducerEpoch {
                attempt: 2,
                ..Default::default()
            },
            ProducerEpoch {
                attempt_scheduled_time: time(1, 0),
                attempt: 1,
                ..Default::default()
            },
            ProducerEpoch {
                attempt_scheduled_time: time(1, 5),
                ..Default::default()
            },
            ProducerEpoch {
                task_started_time: time(0, 1),
                ..Default::default()
            },
            ProducerEpoch {
                task_attempt: 1,
                ..Default::default()
            },
            ProducerEpoch {
                task_started_event_id: 3,
                ..Default::default()
            },
            ProducerEpoch {
                task_started_event_id: 3,
                task_attempt: 1,
                attempt: 9,
                ..Default::default()
            },
            ProducerEpoch {
                task_started_event_id: 16,
                ..Default::default()
            },
        ];
        for pair in epochs.windows(2) {
            assert_eq!(
                compare_epochs(&pair[0], &pair[1]),
                Ordering::Less,
                "{pair:?}"
            );
        }
        // A negative value compares as zero.
        assert_eq!(
            compare_epochs(
                &ProducerEpoch {
                    attempt: -1,
                    ..Default::default()
                },
                &ProducerEpoch::default()
            ),
            Ordering::Equal
        );
        assert!(epoch_key(&epochs[6]).starts_with(&task_key(&task_of(&epochs[6]))));
    }
}
