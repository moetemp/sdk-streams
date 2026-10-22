//! The Store rules, on a real Redis, standalone and as a cluster, and on the memory Store. One
//! suite runs on all of them, so they can't drift. Start the Redis servers with
//! `scripts/redis-test-servers.sh` and set the two URLs it prints.

mod support;

use prost::Message;
use prost_types::Timestamp;
use std::time::Duration;
use support::{Run, Target, record, records_of, stores, supersession};
use temporalio_streams::{
    Reason,
    ids::{activity_producer_id, client_producer_id},
    proto::{Producer, ProducerEpoch, ProducerKind},
    store::{Batch, Capabilities, CreateOutcome, Owner, Withdrawal},
};

fn activity(run: &str, attempt: i32, scheduled: i64) -> Producer {
    Producer {
        kind: ProducerKind::Activity as i32,
        id: activity_producer_id(run, "a", "s"),
        epoch: Some(ProducerEpoch {
            attempt_scheduled_time: Some(Timestamp {
                seconds: scheduled,
                nanos: 0,
            }),
            attempt,
            ..Default::default()
        }),
        run_id: run.to_string(),
    }
}

fn client(attempt: i32) -> Producer {
    Producer {
        kind: ProducerKind::Client as i32,
        id: client_producer_id("me", "p1"),
        epoch: Some(ProducerEpoch {
            attempt,
            ..Default::default()
        }),
        run_id: String::new(),
    }
}

async fn append(
    target: &Target,
    producer: &Producer,
    sequence: i64,
    texts: &[&str],
) -> Result<temporalio_streams::redis::Appended, temporalio_streams::StreamError> {
    let records: Vec<_> = texts.iter().map(|text| record(producer, text)).collect();
    let mut withdrawal = Withdrawal::default();
    let bytes = supersession(producer, &withdrawal);
    withdrawal.supersession = &bytes;
    target
        .store
        .append(
            &target.namespace,
            &target.stream_ref(),
            &Batch {
                producer,
                sequence,
                records: &records,
                withdrawal,
            },
        )
        .await
}

#[tokio::test]
async fn a_create_writes_the_key_once_and_keeps_its_retention() {
    for target in stores().await {
        let none = Withdrawal::default();
        assert_eq!(
            append(&target, &client(1), 1, &["x"])
                .await
                .unwrap_err()
                .reason,
            Reason::NotFound
        );
        let day = Some(Duration::from_secs(86_400));
        let created = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                day,
                None,
                Owner::Client,
                &none,
            )
            .await
            .unwrap();
        assert_eq!(created.outcome, CreateOutcome::Created);
        let again = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                day,
                None,
                Owner::Client,
                &none,
            )
            .await
            .unwrap();
        assert_eq!(again.outcome, CreateOutcome::Exists);
        let other = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                None,
                Owner::Client,
                &none,
            )
            .await
            .unwrap();
        assert_eq!(other.outcome, CreateOutcome::Conflict);
        assert_eq!(other.retention, Duration::from_secs(86_400));
    }
}

#[tokio::test]
async fn a_resend_gets_its_first_positions_and_a_lower_sequence_is_refused() {
    for target in stores().await {
        target.create(Owner::Client).await;
        let p = client(1);
        let first = append(&target, &p, 1, &["p1", "p2"]).await.unwrap();
        assert!(!first.resend);
        // Another Producer writes between the batch and its resend.
        append(&target, &activity("r", 1, 10), 1, &["q1"])
            .await
            .unwrap();
        let resend = append(&target, &p, 1, &["p1", "p2"]).await.unwrap();
        assert!(resend.resend);
        assert_eq!(resend.offsets, first.offsets);
        append(&target, &p, 2, &["p3"]).await.unwrap();
        assert_eq!(
            append(&target, &p, 1, &["p1"]).await.unwrap_err().reason,
            Reason::Sequence
        );
        let texts = target.texts().await;
        assert_eq!(texts, ["p1", "p2", "q1", "p3"]);
    }
}

#[tokio::test]
async fn a_lower_epoch_is_fenced_for_each_producer_kind() {
    for target in stores().await {
        target.create(Owner::Workflow(None)).await;
        let run = Run::new("r1");
        let la = run.local_activity("k", 8, 1, 2);
        target.batch(&la, 1, &["la2"]).await.unwrap();
        let stale = target
            .batch(&run.local_activity("k", 8, 1, 1), 1, &["la1"])
            .await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
        append(&target, &activity("r1", 2, 20), 1, &["a2"])
            .await
            .unwrap();
        let stale = append(&target, &activity("r1", 3, 15), 1, &["a3"]).await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
        append(&target, &client(2), 1, &["c2"]).await.unwrap();
        let stale = append(&target, &client(1), 1, &["c1"]).await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
        assert_eq!(target.texts().await, ["la2", "a2", "c2"]);
    }
}

#[tokio::test]
async fn a_close_stops_appends_but_not_a_resend() {
    for target in stores().await {
        target.create(Owner::Client).await;
        let p = client(1);
        let first = append(&target, &p, 1, &["a"]).await.unwrap();
        let closed = target.close(&p).await.unwrap();
        assert!(!closed.already);
        assert_eq!(
            append(&target, &p, 2, &["b"]).await.unwrap_err().reason,
            Reason::Closed
        );
        assert_eq!(
            append(&target, &p, 1, &["a"]).await.unwrap().offsets,
            first.offsets
        );
        let again = target.close(&p).await.unwrap();
        assert!(again.already);
        assert_eq!(again.offset, closed.offset);
        let created = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                None,
                Owner::Client,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
        assert_eq!(created.outcome, CreateOutcome::Closed);
    }
}

#[tokio::test]
async fn a_newer_format_refuses_every_write() {
    for target in stores().await {
        target.create(Owner::Client).await;
        target.set_meta("v", "2").await;
        let error = append(&target, &client(1), 1, &["a"]).await.unwrap_err();
        assert_eq!(error.reason, Reason::FormatTooNew);
        assert_eq!(
            target.close(&client(1)).await.unwrap_err().reason,
            Reason::FormatTooNew
        );
        let create = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                None,
                Owner::Client,
                &Withdrawal::default(),
            )
            .await;
        assert_eq!(create.unwrap_err().reason, Reason::FormatTooNew);
    }
}

#[tokio::test]
async fn an_expired_stream_refuses_writes_and_its_create() {
    for target in stores().await {
        target.create(Owner::Client).await;
        target.set_meta("exp", "1").await;
        let error = append(&target, &client(1), 1, &["a"]).await.unwrap_err();
        assert_eq!(error.reason, Reason::Expired);
        let created = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                None,
                Owner::Client,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
        assert_eq!(created.outcome, CreateOutcome::Expired);
    }
}

#[tokio::test]
async fn an_activity_retry_flags_its_first_batch_only() {
    for target in stores().await {
        target.create(Owner::Workflow(None)).await;
        let (a1, a2) = (activity("r1", 1, 10), activity("r1", 2, 20));
        let other = Producer {
            id: activity_producer_id("r1", "b", "s"),
            ..activity("r1", 1, 10)
        };
        append(&target, &a1, 1, &["x1"]).await.unwrap();
        append(&target, &other, 1, &["y1"]).await.unwrap();
        assert!(append(&target, &a2, 1, &["x2"]).await.unwrap().flagged);
        assert!(!append(&target, &a2, 2, &["x3"]).await.unwrap().flagged);
        assert_eq!(target.visible().await, ["y1", "x2", "x3"]);
    }
}

#[tokio::test]
async fn an_empty_batch_of_a_retry_withdraws_the_earlier_attempt() {
    for target in stores().await {
        target.create(Owner::Workflow(None)).await;
        append(&target, &activity("r1", 1, 10), 1, &["x1"])
            .await
            .unwrap();
        let empty = append(&target, &activity("r1", 2, 20), 1, &[])
            .await
            .unwrap();
        assert!(empty.flagged);
        assert!(empty.offsets.is_some());
        assert!(target.visible().await.is_empty());
        // The withdrawal is done, so another empty batch writes nothing.
        let again = append(&target, &activity("r1", 3, 30), 1, &[])
            .await
            .unwrap();
        assert!(!again.flagged);
        assert_eq!(again.offsets, None);
        // An empty batch still moves the fence.
        let stale = append(&target, &activity("r1", 2, 20), 2, &["late"]).await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
    }
}

#[tokio::test]
async fn a_retry_run_withdraws_and_fences_the_previous_run() {
    for target in stores().await {
        target.create(Owner::Workflow(None)).await;
        let (r1, r2) = (Run::new("r1"), Run::new("r2"));
        target
            .batch(&r1.local_activity("k", 3, 1, 1), 1, &["A"])
            .await
            .unwrap();
        append(&target, &activity("r1", 1, 10), 1, &["A'"])
            .await
            .unwrap();
        let b = target
            .batch_retry(&r2.local_activity("k", 3, 1, 1), "r1", &["B"])
            .await
            .unwrap();
        assert!(b.flagged);
        assert_eq!(target.visible().await, ["B"]);
        let late = append(&target, &activity("r1", 1, 10), 2, &["A''"]).await;
        assert_eq!(late.unwrap_err().reason, Reason::Fenced);
        let late = target
            .batch(&r1.local_activity("j", 6, 1, 1), 1, &["A3"])
            .await;
        assert_eq!(late.unwrap_err().reason, Reason::Fenced);
        // A second withdrawal of the same run withdraws nothing more.
        let again = target
            .batch_retry(&r2.local_activity("j", 6, 1, 1), "r1", &["B2"])
            .await
            .unwrap();
        assert!(!again.flagged);
    }
}

#[tokio::test]
async fn the_ttl_follows_the_owner_and_the_close() {
    for target in stores().await {
        target.create(Owner::Workflow(None)).await;
        let run = Run::new("r1");
        target
            .batch(&run.local_activity("k", 3, 1, 1), 1, &["a"])
            .await
            .unwrap();
        let (ttl, _) = target.ttls().await;
        assert!(
            ttl > Duration::from_secs(80 * 86_400).as_millis() as i64,
            "{ttl}"
        );
        target
            .close(&run.local_activity("k", 6, 1, 1))
            .await
            .unwrap();
        let (ttl, meta) = target.ttls().await;
        assert!(
            ttl <= Duration::from_secs(7 * 86_400).as_millis() as i64,
            "{ttl}"
        );
        assert!(
            meta > Duration::from_secs(36 * 86_400).as_millis() as i64,
            "{meta}"
        );
    }
}

#[tokio::test]
async fn an_append_trims_by_retention_and_keeps_the_watermark() {
    for target in stores().await {
        let none = Withdrawal::default();
        target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                Some(Duration::from_millis(200)),
                None,
                // An open Workflow Stream keeps its key, so only the Retention trims.
                Owner::Workflow(None),
                &none,
            )
            .await
            .unwrap();
        let old = append(&target, &client(1), 1, &["old"]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        // The read hides the old Record before any append trims it.
        let (state, entries) = target
            .store
            .read(&target.namespace, &target.stream, 0, 10)
            .await
            .unwrap();
        assert!(state.retained_from() > old.offsets.unwrap().1);
        assert_eq!(entries.len(), 1);
        append(&target, &client(1), 2, &["new"]).await.unwrap();
        let (state, entries) = target
            .store
            .read(&target.namespace, &target.stream, 0, 10)
            .await
            .unwrap();
        assert_eq!(state.trimmed, old.offsets.unwrap().1);
        assert_eq!(records_of(&entries), ["new"]);
    }
}

#[tokio::test]
async fn a_blocked_read_wakes_on_an_append_and_a_wait_for_replicas_ends() {
    for target in stores().await {
        target.create(Owner::Client).await;
        let first = append(&target, &client(1), 1, &["a"]).await.unwrap();
        let newest = first.offsets.unwrap().1;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let woke = target
            .store
            .wait_after(&target.namespace, &target.stream, newest, deadline)
            .await
            .unwrap();
        assert!(!woke);
        let store = target.store.clone();
        let (namespace, stream) = (target.namespace.clone(), target.stream.clone());
        let waiter = tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            store
                .wait_after(&namespace, &stream, newest, deadline)
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        append(&target, &client(1), 2, &["b"]).await.unwrap();
        assert!(waiter.await.unwrap().unwrap());
    }
}

#[tokio::test]
async fn a_sequence_below_one_is_refused() {
    for target in stores().await {
        target.create(Owner::Client).await;
        for sequence in [0, -1] {
            let error = append(&target, &client(1), sequence, &["x"])
                .await
                .unwrap_err();
            assert_eq!(error.reason, Reason::Invalid);
        }
    }
}

#[tokio::test]
async fn a_workflow_create_takes_over_a_stream_an_activity_created() {
    for target in stores().await {
        target.create(Owner::Client).await;
        target.create(Owner::Workflow(None)).await;
        assert_eq!(target.meta("own").await.as_deref(), Some("wf"));
        let (_, ttl) = target.ttls().await;
        assert!(
            ttl > Duration::from_secs(100 * 86_400).as_millis() as i64,
            "{ttl}"
        );
    }
}

#[tokio::test]
async fn a_stale_producer_cant_close_the_stream() {
    for target in stores().await {
        target.create(Owner::Client).await;
        append(&target, &client(2), 1, &["a"]).await.unwrap();
        assert_eq!(
            target.close(&client(1)).await.unwrap_err().reason,
            Reason::Fenced
        );
        target.close(&client(2)).await.unwrap();
    }
}

#[tokio::test]
async fn an_append_whose_ids_would_not_fit_an_offset_is_refused_before_it_writes() {
    for target in stores().await {
        target.create(Owner::Client).await;
        // An entry a minute ahead of the clock, with nearly the last 20-bit sequence.
        let ahead = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 60_000;
        target.add_raw_entry(&format!("{ahead}-1048570")).await;
        let error = append(&target, &client(1), 1, &["a"; 10])
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::Storage);
        assert_eq!(target.log_len().await, 1);
    }
}

#[tokio::test]
async fn every_store_keeps_the_meta_fields_of_the_shared_vectors() {
    use temporalio_streams::ids::fields;
    let vectors: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{}/testdata/fields.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap(),
    )
    .unwrap();
    for target in stores().await {
        target.create(Owner::Workflow(None)).await;
        let producer = activity("run:1", 1, 10);
        append(&target, &producer, 1, &["a"]).await.unwrap();
        let kind = ProducerKind::Activity as i32;
        // The vector's Producer is this one, so its field names are the ones the Store keeps.
        assert_eq!(producer.id, vectors["producer_id"].as_str().unwrap());
        let fence = fields::fence(kind, &producer.id);
        assert_eq!(fence, vectors["fence"].as_str().unwrap());
        for field in [
            fence,
            fields::dedupe(kind, &producer.id),
            fields::holds(kind, &producer.id),
            vectors["run_wrote"].as_str().unwrap().to_string(),
            fields::VERSION.to_string(),
        ] {
            assert!(target.meta(&field).await.is_some(), "{field}");
        }
    }
}

#[tokio::test]
async fn an_auto_close_closes_a_quiet_stream_on_its_next_write() {
    for target in stores().await {
        let none = Withdrawal::default();
        let period = Some(Duration::from_millis(300));
        let stream = target.stream_ref();
        let create = || {
            target.store.create(
                &target.namespace,
                &stream,
                None,
                period,
                Owner::Client,
                &none,
            )
        };
        let created = create().await.unwrap();
        assert_eq!(created.outcome, CreateOutcome::Created);
        assert_eq!(created.auto_close, period);
        assert_eq!(create().await.unwrap().outcome, CreateOutcome::Exists);
        let other = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                None,
                Owner::Client,
                &none,
            )
            .await
            .unwrap();
        assert_eq!(other.outcome, CreateOutcome::Conflict);
        assert_eq!(other.auto_close, period);

        // Each append holds the close off for one more period.
        let p = client(1);
        append(&target, &p, 1, &["a"]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        append(&target, &p, 2, &["b"]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let last = append(&target, &p, 3, &["c"]).await.unwrap();
        let (state, _) = target
            .store
            .read(&target.namespace, &target.stream, 0, 0)
            .await
            .unwrap();
        assert_eq!(state.auto_close, period);
        assert!(!state.auto_close_due());

        tokio::time::sleep(Duration::from_millis(400)).await;
        let (state, _) = target
            .store
            .read(&target.namespace, &target.stream, 0, 0)
            .await
            .unwrap();
        assert!(state.auto_close_due());
        assert_eq!(state.closed_at, None);
        // The append that finds the period passed writes the close Record and is refused.
        assert_eq!(
            append(&target, &p, 4, &["d"]).await.unwrap_err().reason,
            Reason::Closed
        );
        // A resend still gets its first answer.
        assert_eq!(
            append(&target, &p, 3, &["c"]).await.unwrap().offsets,
            last.offsets
        );
        let entries = target.entries().await;
        let close = entries.last().unwrap();
        let record =
            temporalio_streams::proto::StreamRecord::decode(close.record.as_deref().unwrap())
                .unwrap();
        assert_eq!(
            record.kind,
            temporalio_streams::proto::StreamRecordKind::Close as i32
        );
        assert!(record.producer.is_none());
        assert!(record.close_result.is_none() && record.close_failure.is_none());
        assert_eq!(target.texts().await, ["a", "b", "c"]);
        let closed = target.close(&p).await.unwrap();
        assert!(closed.already);
        assert_eq!(closed.offset, close.offset);
        assert_eq!(create().await.unwrap().outcome, CreateOutcome::Closed);
    }
}

#[tokio::test]
async fn an_auto_close_counts_from_the_create_and_each_step_applies_it_once() {
    for target in stores().await {
        let none = Withdrawal::default();
        target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                Some(Duration::from_millis(200)),
                Owner::Workflow(None),
                &none,
            )
            .await
            .unwrap();
        assert_eq!(
            target
                .store
                .auto_close(&target.namespace, &target.stream)
                .await
                .unwrap(),
            None
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        let first = target
            .store
            .auto_close(&target.namespace, &target.stream)
            .await
            .unwrap();
        let again = target
            .store
            .auto_close(&target.namespace, &target.stream)
            .await
            .unwrap();
        assert!(first.is_some());
        assert_eq!(first, again);
        assert_eq!(target.log_len().await, 1);
        let (state, _) = target
            .store
            .read(&target.namespace, &target.stream, 0, 0)
            .await
            .unwrap();
        assert_eq!(state.closed_at, first);
        // A closed Stream keeps its Retention, not the safety TTL.
        let (log, _) = target.ttls().await;
        assert!(
            log <= Duration::from_secs(7 * 86_400).as_millis() as i64,
            "{log}"
        );
        assert!(
            target
                .store
                .capabilities()
                .contains(Capabilities::AUTO_CLOSE)
        );
    }
}

#[tokio::test]
async fn an_auto_close_period_at_the_edges_neither_overflows_nor_rounds_to_zero() {
    for target in stores().await {
        let none = Withdrawal::default();
        let created = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                Some(Duration::from_micros(500)),
                Owner::Client,
                &none,
            )
            .await
            .unwrap();
        assert_eq!(created.auto_close, Some(Duration::from_millis(1)));
    }
    for target in stores().await {
        let none = Withdrawal::default();
        target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                Some(Duration::from_secs(u64::MAX / 4)),
                Owner::Client,
                &none,
            )
            .await
            .unwrap();
        append(&target, &client(1), 1, &["a"]).await.unwrap();
        append(&target, &client(1), 2, &["b"]).await.unwrap();
        assert_eq!(target.texts().await, ["a", "b"]);
    }
}
