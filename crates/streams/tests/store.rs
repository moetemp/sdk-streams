//! The Store rules, on a real Redis, standalone and as a cluster, and on the memory Store. One
//! suite runs on all of them, so they can't drift. Start the Redis servers with
//! `scripts/redis-test-servers.sh` and set the two URLs it prints.

mod support;

use prost_types::Timestamp;
use std::time::Duration;
use support::{Needs, Run, Target, digest, record, records_of, stores_for, supersession};
use temporalio_streams::{
    Reason,
    ids::{
        activity_producer_id, client_producer_id, named_client_producer_id, workflow_producer_id,
    },
    proto::{Producer, ProducerEpoch, ProducerKind, StreamRef},
    store::{Batch, CreateOutcome, Owner, Withdrawal},
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

/// A second client Producer, which every backend takes as sent.
fn other_client() -> Producer {
    Producer {
        id: named_client_producer_id("other"),
        ..client(1)
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
                digest: &digest(texts),
                withdrawal,
            },
        )
        .await
}

/// A create writes the key once, and another Retention is a conflict that keeps the stored one.
#[tokio::test]
async fn a_create_writes_the_key_once_and_keeps_its_retention() {
    for target in stores_for(Needs::STORE_ANSWERS).await {
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
                Owner::Client,
                &none,
            )
            .await
            .unwrap();
        assert_eq!(other.outcome, CreateOutcome::Conflict);
        assert_eq!(other.retention, Duration::from_secs(86_400));
    }
}

/// A resend with the last sequence gets the offsets of its first answer, also after another
/// Producer wrote, and a lower sequence is refused.
#[tokio::test]
async fn a_resend_gets_its_first_positions_and_a_lower_sequence_is_refused() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        let p = client(1);
        let first = append(&target, &p, 1, &["p1", "p2"]).await.unwrap();
        // Another Producer writes between the batch and its resend.
        append(&target, &other_client(), 1, &["q1"]).await.unwrap();
        let resend = append(&target, &p, 1, &["p1", "p2"]).await.unwrap();
        assert_eq!(resend.offsets, first.offsets);
        if target.gives.gives(Needs::STORE_ANSWERS) {
            assert!(!first.resend && resend.resend);
        }
        append(&target, &p, 2, &["p3"]).await.unwrap();
        assert_eq!(
            append(&target, &p, 1, &["p1"]).await.unwrap_err().reason,
            Reason::Sequence
        );
        let texts = target.texts().await;
        assert_eq!(texts, ["p1", "p2", "q1", "p3"]);
    }
}

/// A resend with the last sequence and other content fails as `seq_divergent` and writes
/// nothing. With `RAW_KEYS`, a dedupe entry from before the digest takes any resend.
#[tokio::test]
async fn a_resend_with_other_content_fails_as_divergent() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        let p = client(1);
        let first = append(&target, &p, 1, &["p1"]).await.unwrap();
        let divergent = append(&target, &p, 1, &["other"]).await.unwrap_err();
        assert_eq!(divergent.reason, Reason::SeqDivergent);
        assert_eq!(target.texts().await, ["p1"]);
        if !target.gives.gives(Needs::RAW_KEYS) {
            continue;
        }
        // A dedupe entry written before the digest has none, so it takes any resend.
        let field = temporalio_streams::ids::fields::dedupe(ProducerKind::Client as i32, &p.id);
        let entry = target.meta(&field).await.unwrap();
        target
            .set_meta(&field, &entry[..entry.rfind('|').unwrap()])
            .await;
        let resend = append(&target, &p, 1, &["other"]).await.unwrap();
        assert_eq!((resend.resend, resend.offsets), (true, first.offsets));
    }
}

/// A lower epoch is fenced for each Producer kind.
#[tokio::test]
async fn a_lower_epoch_is_fenced_for_each_producer_kind() {
    for target in stores_for(Needs::UNCHECKED).await {
        target.create(Owner::Workflow).await;
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

/// A close refuses later appends but not a resend of an earlier batch, and a create finds the
/// Stream closed. With `STORE_ANSWERS`, a second close answers `already` with the same offset.
#[tokio::test]
async fn a_close_stops_appends_but_not_a_resend() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        let p = client(1);
        let first = append(&target, &p, 1, &["a"]).await.unwrap();
        let closed = target.close(&p).await.unwrap();
        assert_eq!(
            append(&target, &p, 2, &["b"]).await.unwrap_err().reason,
            Reason::Closed
        );
        assert_eq!(
            append(&target, &p, 1, &["a"]).await.unwrap().offsets,
            first.offsets
        );
        let again = target.close(&p).await.unwrap();
        if target.gives.gives(Needs::STORE_ANSWERS) {
            assert!(!closed.already && again.already);
            assert_eq!(again.offset, closed.offset);
        }
        let created = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                Owner::Client,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
        assert_eq!(created.outcome, CreateOutcome::Closed);
    }
}

/// A Stream with a newer format version refuses every write.
#[tokio::test]
async fn a_newer_format_refuses_every_write() {
    for target in stores_for(Needs::RAW_KEYS).await {
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
                Owner::Client,
                &Withdrawal::default(),
            )
            .await;
        assert_eq!(create.unwrap_err().reason, Reason::FormatTooNew);
    }
}

/// An expired Stream refuses writes, and its create finds it expired.
#[tokio::test]
async fn an_expired_stream_refuses_writes_and_its_create() {
    for target in stores_for(Needs::RAW_KEYS).await {
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
                Owner::Client,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
        assert_eq!(created.outcome, CreateOutcome::Expired);
    }
}

/// An Activity retry flags only its first batch, which withdraws the earlier attempt.
#[tokio::test]
async fn an_activity_retry_flags_its_first_batch_only() {
    for target in stores_for(Needs::UNCHECKED.and(Needs::STORE_ANSWERS)).await {
        target.create(Owner::Workflow).await;
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

/// An empty batch of an Activity retry withdraws the earlier attempt's Records.
#[tokio::test]
async fn an_empty_batch_of_a_retry_withdraws_the_earlier_attempt() {
    for target in stores_for(Needs::UNCHECKED).await {
        target.create(Owner::Workflow).await;
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

/// A retry run's first write withdraws the previous run and fences its Producers.
#[tokio::test]
async fn a_retry_run_withdraws_and_fences_the_previous_run() {
    for target in stores_for(Needs::UNCHECKED).await {
        target.create(Owner::Workflow).await;
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

/// An open Workflow Stream keeps the safety TTL, and its close gives it the Retention.
#[tokio::test]
async fn the_ttl_follows_the_owner_and_the_close() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create(Owner::Workflow).await;
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
        target.close(&workflow("r1")).await.unwrap();
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

/// A read hides Records past the Retention, and the next append trims them and keeps the
/// newest trimmed offset.
#[tokio::test]
async fn an_append_trims_by_retention_and_keeps_the_watermark() {
    for target in stores_for(Needs::RAW_KEYS.and(Needs::SHORT_TIMES)).await {
        let none = Withdrawal::default();
        target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                Some(Duration::from_millis(200)),
                // An open Workflow Stream keeps its key, so only the Retention trims.
                Owner::Workflow,
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

/// A wait for the next entry wakes on an append and ends at its deadline.
#[tokio::test]
async fn a_blocked_read_wakes_on_an_append_and_a_wait_for_replicas_ends() {
    for target in stores_for(Needs::RAW_KEYS).await {
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

/// A sequence below one is refused.
#[tokio::test]
async fn a_sequence_below_one_is_refused() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        for sequence in [0, -1] {
            let error = append(&target, &client(1), sequence, &["x"])
                .await
                .unwrap_err();
            assert_eq!(error.reason, Reason::Invalid);
        }
    }
}

/// A Workflow's create makes a Stream that an Activity created Workflow-owned.
#[tokio::test]
async fn a_workflow_create_takes_over_a_stream_an_activity_created() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create(Owner::Client).await;
        target.create(Owner::Workflow).await;
        assert_eq!(target.meta("own").await.as_deref(), Some("wf"));
        let (_, ttl) = target.ttls().await;
        assert!(
            ttl > Duration::from_secs(100 * 86_400).as_millis() as i64,
            "{ttl}"
        );
    }
}

/// A Producer with a lower epoch can't close the Stream.
#[tokio::test]
async fn a_stale_producer_cant_close_the_stream() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        append(&target, &client(2), 1, &["a"]).await.unwrap();
        assert_eq!(
            target.close(&client(1)).await.unwrap_err().reason,
            Reason::Fenced
        );
        target.close(&client(2)).await.unwrap();
    }
}

/// The close Activity's Producer of run `run`.
fn workflow(run: &str) -> Producer {
    Producer {
        kind: ProducerKind::Workflow as i32,
        id: workflow_producer_id(run),
        run_id: run.to_string(),
        epoch: None,
    }
}

/// Only the owner closes a Stream: the close Activity of its run chain for a Workflow-owned one,
/// a client for a client's. A Local Activity of the chain can't close it.
#[tokio::test]
async fn only_the_owner_closes_a_stream() {
    for target in stores_for(Needs::UNCHECKED.and(Needs::STORE_ANSWERS)).await {
        let run = Run::new("r1");
        let la = run.local_activity("k", 3, 1, 1);
        target.create(Owner::Client).await;
        for producer in [&la, &activity("r1", 1, 10)] {
            let refused = target.close(producer).await.unwrap_err();
            assert_eq!(refused.reason, Reason::NotOwner);
        }
        target.close(&client(1)).await.unwrap();

        let owned = |workflow_id: &str| StreamRef {
            id: format!("{}-owned", target.stream),
            workflow_id: workflow_id.to_string(),
            ..Default::default()
        };
        target
            .store
            .create(
                &target.namespace,
                &owned("agent"),
                None,
                Owner::Workflow,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
        for (stream, producer) in [
            (owned("agent"), client(1)),
            (owned("agent"), activity("r1", 1, 10)),
            (owned("agent"), la.clone()),
            (owned("another-agent"), workflow("r1")),
        ] {
            let refused = target.close_at(&stream, &producer).await.unwrap_err();
            assert_eq!(refused.reason, Reason::NotOwner);
        }
        let closed = target.close_at(&owned("agent"), &workflow("r1")).await;
        assert!(!closed.unwrap().already);
        // The owner check comes first, also on a closed Stream.
        let refused = target.close_at(&owned("agent"), &la).await.unwrap_err();
        assert_eq!(refused.reason, Reason::NotOwner);
    }
}

/// An append whose entry ids would not fit an offset is refused before it writes.
#[tokio::test]
async fn an_append_whose_ids_would_not_fit_an_offset_is_refused_before_it_writes() {
    for target in stores_for(Needs::RAW_KEYS).await {
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

/// Every Store keeps the meta fields that the shared vectors name.
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
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create(Owner::Workflow).await;
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
