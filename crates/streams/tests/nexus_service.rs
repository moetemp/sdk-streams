//! The stream service's Nexus operations as a caller sees them: what lands, what a read answers,
//! the limits and the failure classes.
//!
//! These are the store cases the Python handler's tests held before Core served the calls. They
//! run on memory, and on Redis when `STREAMS_REDIS_URL` is set.

mod nexus_support;

use nexus_support::*;
use prost::Message;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::{
    common::v1::Payload, enums::v1::WorkflowExecutionStatus,
};
use temporalio_streams::{
    MemoryStore, NotifierClient, NotifyError, OwnerClient, OwnerDescription, OwnerError,
    StreamNotification, StreamStore, Streams, StreamsOptions,
    nexus::{NexusCalls, NexusOptions, OperationToken},
    proto::{
        ChainId, NexusFailure, ReadRangeRequest, ReadRequest, StoreAppendRequest, StreamAddress,
        StreamOwnerKind, StreamRecord, StreamRecordKind,
    },
};

static IDS: AtomicUsize = AtomicUsize::new(0);

fn workflow() -> String {
    format!(
        "service-{}-{}",
        std::process::id(),
        IDS.fetch_add(1, Ordering::Relaxed)
    )
}

const TOPIC: &str = "out";

fn append_input(wf: &str, producer: &str, attempt: i64, sequence: i64, texts: &[&str]) -> Value {
    json!({
        "stream": stream(wf, TOPIC),
        "producer_id": producer,
        "attempt": attempt,
        "sequence": sequence,
        "payloads": texts.iter().map(|text| body(text)).collect::<Vec<_>>(),
    })
}

/// Appends `texts` as producer `p`, attempt one, and answers with the cursor.
async fn write(calls: &NexusCalls, wf: &str, sequence: i64, texts: &[&str]) -> String {
    let answer = append(calls, append_input(wf, "p", 1, sequence, texts))
        .await
        .unwrap();
    answer["cursor"].as_str().unwrap().to_string()
}

fn read_input(wf: &str, after: &str) -> Value {
    json!({"stream": stream(wf, TOPIC), "after_token": after})
}

async fn read_after(calls: &NexusCalls, wf: &str, after: &str) -> Value {
    read(calls, read_input(wf, after)).await.unwrap()
}

/// The stored records of a read answer, without its supersessions and errors.
fn stored(answer: &Value) -> Vec<StreamRecord> {
    use base64::Engine;
    answer["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|wire| wire.get("superseded").is_none() && wire.get("error").is_none())
        .map(|wire| {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(wire["record"].as_str().unwrap())
                .unwrap();
            StreamRecord::decode(bytes.as_slice()).unwrap()
        })
        .collect()
}

fn texts(answer: &Value) -> Vec<String> {
    stored(answer)
        .into_iter()
        .map(|record| serde_json::from_slice(&record.body.unwrap().data).unwrap())
        .collect()
}

fn records(answer: &Value) -> &Vec<Value> {
    answer["records"].as_array().unwrap()
}

/// The Nexus error type, whether a retry may help, and the class the caller raises.
fn kind_of(failure: &NexusFailure) -> (&str, bool, &str) {
    let class = failure.message.split(':').next().unwrap();
    (
        failure.handler_error_type.as_str(),
        failure.retryable,
        class,
    )
}

fn token(answer: &Value) -> &str {
    answer["next_token"].as_str().unwrap()
}

/// The server's side of a caught-up reader's wait: each wait it took, and a record that lands
/// as the wait goes out, as one that the notifier would not have seen.
#[derive(Default)]
struct Waker {
    waits: std::sync::Mutex<Vec<(ChainId, String, String, i64)>>,
    late: std::sync::Mutex<Option<(Arc<dyn StreamStore>, StoreAppendRequest)>>,
    /// Refuses every wait, as a notifier that lost the callback does.
    refuse: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl NotifierClient for Waker {
    async fn notify(&self, _: StreamNotification) -> Result<(), NotifyError> {
        Ok(())
    }

    async fn wait_callback(
        &self,
        chain: ChainId,
        topic: String,
        request_id: String,
        after_counter: i64,
    ) -> Result<(), NotifyError> {
        self.waits
            .lock()
            .unwrap()
            .push((chain, topic, request_id, after_counter));
        if self.refuse.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(NotifyError::Refused("no such callback".to_string()));
        }
        let late = self.late.lock().unwrap().take();
        if let Some((store, request)) = late {
            store.append(request).await.unwrap();
        }
        Ok(())
    }
}

/// The token of `wf`'s stream operation, attached as `attach-1` on the chain of `run-1`.
fn operation_token(wf: &str) -> String {
    OperationToken {
        stream: StreamAddress {
            namespace: NAMESPACE.to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: wf.to_string(),
            run_id: String::new(),
            topic: TOPIC.to_string(),
            ..Default::default()
        },
        attach_request_id: "attach-1".to_string(),
        chain_first_run_id: "run-1".to_string(),
    }
    .encode()
}

/// The token of Activity `id`'s stream operation, attached as `attach-1` on its run `run-1`.
fn activity_operation_token(id: &str) -> String {
    OperationToken {
        stream: StreamAddress {
            namespace: NAMESPACE.to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: id.to_string(),
            topic: TOPIC.to_string(),
            ..Default::default()
        },
        attach_request_id: "attach-1".to_string(),
        chain_first_run_id: "run-1".to_string(),
    }
    .encode()
}

fn waking(wf: &str, after: &str) -> Value {
    let mut input = read_input(wf, after);
    input["wake_token"] = json!(operation_token(wf));
    input
}

/// Temporal where every Workflow's first run `run-1` has completed.
struct Ended;

#[async_trait::async_trait]
impl OwnerClient for Ended {
    async fn describe(
        &self,
        _: &str,
        _: temporalio_streams::OwnerId<'_>,
        _: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        Ok(OwnerDescription {
            run_id: "run-1".to_string(),
            first_run_id: "run-1".to_string(),
            status: temporalio_streams::OwnerStatus::of_workflow(
                WorkflowExecutionStatus::Completed,
            ),
        })
    }
}

/// Temporal where run `run-1` continued as new into `run-2`, which still runs.
struct Continued;

#[async_trait::async_trait]
impl OwnerClient for Continued {
    async fn describe(
        &self,
        _: &str,
        _: temporalio_streams::OwnerId<'_>,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        Ok(if run_id == "run-1" {
            OwnerDescription {
                run_id: "run-1".to_string(),
                first_run_id: "run-1".to_string(),
                status: temporalio_streams::OwnerStatus::of_workflow(
                    WorkflowExecutionStatus::ContinuedAsNew,
                ),
            }
        } else {
            OwnerDescription {
                run_id: "run-2".to_string(),
                first_run_id: "run-1".to_string(),
                status: temporalio_streams::OwnerStatus::of_workflow(
                    WorkflowExecutionStatus::Running,
                ),
            }
        })
    }
}

mod cases {
    use super::*;

    pub(crate) async fn an_append_reads_back_as_the_stored_record(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let landed = write(&calls, &wf, 1, &["one", "two"]).await;
        let answer = read_after(&calls, &wf, "").await;
        let found = stored(&answer);
        assert_eq!(
            found
                .iter()
                .map(|r| (
                    r.topic.as_str(),
                    r.kind,
                    r.producer_id.as_str(),
                    r.attempt,
                    r.sequence
                ))
                .collect::<Vec<_>>(),
            [
                (TOPIC, StreamRecordKind::Data as i32, "p", 1, 1),
                (TOPIC, StreamRecordKind::Data as i32, "p", 1, 2),
            ]
        );
        // The handler's codec decodes the body back to what the writer sent.
        let sent = {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(body("one"))
                .unwrap();
            Payload::decode(bytes.as_slice()).unwrap()
        };
        assert_eq!(found[0].body.as_ref(), Some(&sent));
        assert_eq!(records(&answer).last().unwrap()["token"], landed);
        assert_eq!(answer["next_token"], landed);
        assert_eq!(answer["done"], false);
    }

    pub(crate) async fn a_batch_lands_in_order_and_a_cursor_resumes_strictly_after(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let first = write(&calls, &wf, 1, &["one"]).await;
        write(&calls, &wf, 2, &["two", "three"]).await;
        assert_eq!(
            texts(&read_after(&calls, &wf, &first).await),
            ["two", "three"]
        );
        let answer = read_after(&calls, &wf, "").await;
        // A cursor a reader saw resumes the same way as one an append answered with.
        let seen = records(&answer)[1]["token"].as_str().unwrap().to_string();
        assert_eq!(texts(&read_after(&calls, &wf, &seen).await), ["three"]);
        assert_eq!(
            stored(&answer)
                .iter()
                .map(|r| r.sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    pub(crate) async fn max_records_caps_an_answer_and_the_next_call_continues(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, 1, &["a", "b", "c", "d", "e"]).await;
        let mut after = String::new();
        let mut pages = vec![];
        for _ in 0..3 {
            let mut input = read_input(&wf, &after);
            input["max_records"] = json!(2);
            let answer = read(&calls, input).await.unwrap();
            pages.push(texts(&answer));
            after = token(&answer).to_string();
        }
        assert_eq!(pages, [vec!["a", "b"], vec!["c", "d"], vec!["e"]]);
    }

    pub(crate) async fn a_read_that_collects_nothing_answers_with_the_callers_cursor(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let landed = write(&calls, &wf, 1, &["one"]).await;
        let mut input = read_input(&wf, &landed);
        input["wait_ms"] = json!(200);
        let answer = read(&calls, input).await.unwrap();
        assert!(records(&answer).is_empty());
        assert_eq!(answer["next_token"], landed);
        assert_eq!(answer["done"], false);
    }

    pub(crate) async fn a_parked_read_answers_when_a_record_arrives(store: Arc<dyn StreamStore>) {
        let calls = Arc::new(service(store, NexusOptions::default()));
        let wf = workflow();
        let landed = write(&calls, &wf, 1, &["one"]).await;
        let mut input = read_input(&wf, &landed);
        input["wait_ms"] = json!(10_000);
        let parked = tokio::spawn({
            let calls = calls.clone();
            async move { read(&calls, input).await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!parked.is_finished());
        write(&calls, &wf, 2, &["two"]).await;
        let answer = tokio::time::timeout(Duration::from_secs(5), parked)
            .await
            .expect("the parked read never answered")
            .unwrap();
        assert_eq!(texts(&answer), ["two"]);
    }

    pub(crate) async fn latest_only_positions_a_reader_at_the_end(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let latest = json!({"stream": stream(&wf, TOPIC), "latest_only": true});
        let empty = read(&calls, latest.clone()).await.unwrap();
        assert!(records(&empty).is_empty());
        assert_eq!(empty["next_token"], "");
        let landed = write(&calls, &wf, 1, &["one"]).await;
        let at_end = read(&calls, latest.clone()).await.unwrap();
        assert_eq!(at_end["next_token"], landed);
        write(&calls, &wf, 2, &["two"]).await;
        assert_eq!(
            texts(&read_after(&calls, &wf, token(&at_end)).await),
            ["two"]
        );
        let mut with_cursor = latest;
        with_cursor["after_token"] = json!(landed);
        let refused = read(&calls, with_cursor).await.unwrap_err();
        assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
    }

    pub(crate) async fn a_retried_append_is_written_once(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let input = append_input(&wf, "p", 1, 1, &["one", "two"]);
        let first = append(&calls, input.clone()).await.unwrap();
        // The caller lost the answer and sends the same call again.
        assert_eq!(append(&calls, input).await.unwrap(), first);
        assert_eq!(texts(&read_after(&calls, &wf, "").await), ["one", "two"]);
        let mut after = read_input(&wf, first["cursor"].as_str().unwrap());
        after["wait_ms"] = json!(200);
        assert!(texts(&read(&calls, after).await.unwrap()).is_empty());
    }

    pub(crate) async fn a_divergent_or_stale_retry_is_refused(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, 1, &["one"]).await;
        let divergent = append(&calls, append_input(&wf, "p", 1, 1, &["different"]))
            .await
            .unwrap_err();
        assert_eq!(
            kind_of(&divergent),
            ("BAD_REQUEST", false, "StreamProducerError")
        );
        write(&calls, &wf, 2, &["two"]).await;
        let stale = append(&calls, append_input(&wf, "p", 1, 1, &["one"]))
            .await
            .unwrap_err();
        assert_eq!(kind_of(&stale).2, "StreamProducerError");
        assert_eq!(texts(&read_after(&calls, &wf, "").await), ["one", "two"]);
    }

    pub(crate) async fn a_new_attempt_crosses_as_a_supersession_at_its_cursor(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let old = append(&calls, append_input(&wf, "model", 1, 1, &["old"]))
            .await
            .unwrap();
        append(&calls, append_input(&wf, "model", 2, 1, &["new"]))
            .await
            .unwrap();
        let answer = read_after(&calls, &wf, "").await;
        assert_eq!(
            values(&answer),
            ["model@1 old", "SUPERSEDED model 1->2", "model@2 new"]
        );
        // A read that resumes after the old attempt's record has seen no attempt of its own, so
        // only the store's reader knows the new one supersedes it.
        let old = old["cursor"].as_str().unwrap();
        let resumed = read_after(&calls, &wf, old).await;
        assert_eq!(values(&resumed), ["SUPERSEDED model 1->2", "model@2 new"]);
        assert_eq!(records(&resumed)[0]["token"], old);
    }

    pub(crate) async fn an_append_counts_the_producer_id_toward_the_record_limit(
        store: Arc<dyn StreamStore>,
    ) {
        // The stored record holds the topic and the producer id beside the body, and a record no
        // read can carry would be lost to every reader.
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let big = "x".repeat(2600);
        let limited = |producer: &str| {
            call_with(
                append_input(&wf, producer, 1, 1, &[&big]),
                0,
                4096,
                Duration::from_secs(60),
            )
        };
        let refused = drive(
            &calls,
            calls.append(limited(&"p".repeat(600))).await,
            Codec::Sealing,
        )
        .await
        .unwrap_err();
        assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
        assert!(refused.message.contains("limit"), "{}", refused.message);
        drive(&calls, calls.append(limited("p")).await, Codec::Sealing)
            .await
            .unwrap();
    }

    pub(crate) async fn finish_is_a_call_of_its_own(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, 1, &["one"]).await;
        let finished = append(
            &calls,
            json!({
                "stream": stream(&wf, TOPIC),
                "producer_id": "p",
                "attempt": 1,
                "sequence": 2,
                "finish": true,
            }),
        )
        .await
        .unwrap();
        let found = stored(&read_after(&calls, &wf, "").await);
        assert_eq!(
            found
                .iter()
                .map(|r| (r.kind, r.sequence))
                .collect::<Vec<_>>(),
            [
                (StreamRecordKind::Data as i32, 1),
                (StreamRecordKind::Finish as i32, 2),
            ]
        );
        assert!(found[1].body.is_none());
        for (payloads, finish) in [(vec![body("x")], true), (vec![], false)] {
            let refused = append(
                &calls,
                json!({
                    "stream": stream(&wf, TOPIC),
                    "producer_id": "p",
                    "attempt": 1,
                    "sequence": 3,
                    "payloads": payloads,
                    "finish": finish,
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
        }
        assert_eq!(
            finished["cursor"],
            read_after(&calls, &wf, "").await["next_token"]
        );
    }

    pub(crate) async fn a_foreign_cursor_is_refused(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let other = workflow();
        write(&calls, &wf, 1, &["one"]).await;
        let theirs = write(&calls, &other, 1, &["one"]).await;
        for cursor in [theirs.as_str(), "elsewhere:0000abcd:1", "not a cursor"] {
            let refused = read(&calls, read_input(&wf, cursor)).await.unwrap_err();
            assert_eq!(
                kind_of(&refused),
                ("BAD_REQUEST", false, "StreamCursorError"),
                "{cursor}"
            );
        }
        // Another topic of the same owner is another stream too.
        let mine = write(&calls, &wf, 2, &["two"]).await;
        let refused = read(
            &calls,
            json!({"stream": stream(&wf, "other"), "after_token": mine}),
        )
        .await
        .unwrap_err();
        assert_eq!(kind_of(&refused).2, "StreamCursorError");
    }

    pub(crate) async fn argument_mistakes_are_bad_requests(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let no_producer = append(&calls, append_input(&wf, "", 1, 1, &["one"]))
            .await
            .unwrap_err();
        assert_eq!(kind_of(&no_producer), ("BAD_REQUEST", false, "ValueError"));
        let mut not_a_payload = append_input(&wf, "p", 1, 1, &[]);
        not_a_payload["payloads"] = json!(["/w=="]);
        let refused = append(&calls, not_a_payload).await.unwrap_err();
        assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
    }

    pub(crate) async fn a_reference_this_release_cannot_open_is_refused(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let refused = read(
            &calls,
            json!({"stream": {"kind": "nexus", "workflow_id": "wf", "topic": "t"}}),
        )
        .await
        .unwrap_err();
        // The reference decodes, and the call refuses it where it opens the stream, as not
        // implemented in this release.
        assert_eq!(
            kind_of(&refused),
            ("NOT_IMPLEMENTED", false, "StreamUnsupportedError")
        );
        assert!(refused.message.contains("nexus"), "{}", refused.message);
        // An Activity's ref names the Activity and an empty Workflow id, and a Workflow's ref no
        // Activity.
        for stream in [
            json!({"kind": "activity", "workflow_id": "wf", "activity_id": "a", "topic": "t"}),
            json!({"kind": "activity", "workflow_id": "", "topic": "t"}),
            json!({"kind": "activity", "workflow_id": "", "activity_id": "", "topic": "t"}),
            json!({"kind": "workflow", "workflow_id": "wf", "activity_id": "a", "topic": "t"}),
        ] {
            let refused = read(&calls, json!({ "stream": stream.clone() }))
                .await
                .unwrap_err();
            assert_eq!(kind_of(&refused).0, "BAD_REQUEST", "{stream}");
        }
    }

    pub(crate) async fn an_activity_stream_appends_and_reads_apart_from_a_workflow(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let id = workflow();
        let activity = |texts: &[&str], sequence: i64| {
            let mut input = append_input(&id, "p", 1, sequence, texts);
            input["stream"] = activity_stream(&id, TOPIC);
            input
        };
        append(&calls, activity(&["one", "two"], 1)).await.unwrap();
        // A Workflow with the same id owns another stream.
        write(&calls, &id, 1, &["workflow"]).await;
        let answer = read(&calls, json!({"stream": activity_stream(&id, TOPIC)}))
            .await
            .unwrap();
        assert_eq!(texts(&answer), ["one", "two"]);
        assert_eq!(texts(&read_after(&calls, &id, "").await), ["workflow"]);
        let latest = read(
            &calls,
            json!({"stream": activity_stream(&id, TOPIC), "latest_only": true}),
        )
        .await
        .unwrap();
        assert_eq!(latest["next_token"], answer["next_token"]);
        // A cursor of the Activity's stream is not one of the Workflow's.
        let refused = read(&calls, read_input(&id, token(&answer)))
            .await
            .unwrap_err();
        assert_eq!(kind_of(&refused).2, "StreamCursorError");
    }

    pub(crate) async fn an_activity_read_says_done_once_the_run_ends(store: Arc<dyn StreamStore>) {
        let id = workflow();
        let mut input = append_input(&id, "p", 1, 1, &["one"]);
        input["stream"] = activity_stream(&id, TOPIC);
        append(&service(store.clone(), NexusOptions::default()), input)
            .await
            .unwrap();
        let ended = NexusCalls::new(
            Arc::new(Streams::new(
                store,
                Arc::new(Ended),
                StreamsOptions::default(),
            )),
            NexusOptions::default(),
        );
        let mut input = json!({"stream": activity_stream(&id, TOPIC), "wait_ms": 2000});
        let mut seen = vec![];
        for _ in 0..10 {
            let answer = read(&ended, input.clone()).await.unwrap();
            seen.extend(texts(&answer));
            if answer["done"] == true {
                assert_eq!(seen, ["one"]);
                return;
            }
            input["after_token"] = answer["next_token"].clone();
            input["state"] = answer["state"].clone();
        }
        panic!("the read never ended");
    }

    pub(crate) async fn a_wake_token_of_another_activity_is_refused(store: Arc<dyn StreamStore>) {
        let waker = Arc::new(Waker::default());
        let calls = service(store, NexusOptions::default()).with_callers(waker.clone());
        let id = workflow();
        // Every Activity's ref has the same empty Workflow id, so only the Activity id tells
        // the two streams apart.
        for (stream, token) in [
            (
                activity_stream(&id, TOPIC),
                activity_operation_token("other"),
            ),
            (activity_stream(&id, TOPIC), operation_token("")),
            (stream(&id, TOPIC), activity_operation_token(&id)),
        ] {
            let input = json!({"stream": stream, "wake_token": token});
            let refused = read(&calls, input).await.unwrap_err();
            assert_eq!(kind_of(&refused).0, "BAD_REQUEST", "{stream}");
        }
        assert!(waker.waits.lock().unwrap().is_empty());
    }

    pub(crate) async fn an_empty_activity_read_wakes_its_operation_on_the_activity_run(
        store: Arc<dyn StreamStore>,
    ) {
        let waker = Arc::new(Waker::default());
        let calls = service(store, NexusOptions::default()).with_callers(waker.clone());
        let id = workflow();
        let mut input = append_input(&id, "p", 1, 1, &["one"]);
        input["stream"] = activity_stream(&id, TOPIC);
        let last = append(&calls, input).await.unwrap()["cursor"].clone();
        let input = json!({
            "stream": activity_stream(&id, TOPIC),
            "after_token": last,
            "wake_token": activity_operation_token(&id),
        });
        let caught_up = read(&calls, input).await.unwrap();
        assert!(records(&caught_up).is_empty());
        let waits = waker.waits.lock().unwrap().clone();
        let [(chain, topic, request_id, after)] = waits.as_slice() else {
            panic!("one wait, not {waits:?}");
        };
        assert_eq!(
            chain,
            &ChainId {
                namespace: NAMESPACE.to_string(),
                first_run_id: "run-1".to_string(),
                owner_kind: StreamOwnerKind::Activity as i32,
                activity_id: id.clone(),
                ..Default::default()
            }
        );
        assert_eq!((topic.as_str(), request_id.as_str()), (TOPIC, "attach-1"));
        assert!(*after > 0);
    }

    pub(crate) async fn a_workflow_activity_stream_reads_apart_from_its_workflow_and_other_runs(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let id = workflow();
        let tool = |run: &str| workflow_activity_stream(&id, run, "1", TOPIC);
        let mut input = append_input(&id, "p", 1, 1, &["one", "two"]);
        input["stream"] = tool("run-1");
        append(&calls, input).await.unwrap();
        write(&calls, &id, 1, &["workflow"]).await;
        let answer = read(&calls, json!({"stream": tool("run-1")}))
            .await
            .unwrap();
        assert_eq!(texts(&answer), ["one", "two"]);
        assert_eq!(texts(&read_after(&calls, &id, "").await), ["workflow"]);
        // Activity `1` of the next run is another stream, and its cursors are its own.
        let next = read(&calls, json!({"stream": tool("run-2")}))
            .await
            .unwrap();
        assert!(texts(&next).is_empty());
        let refused = read(
            &calls,
            json!({"stream": tool("run-2"), "after_token": token(&answer)}),
        )
        .await
        .unwrap_err();
        assert_eq!(kind_of(&refused).2, "StreamCursorError");
        // The run is part of the owner, so another run's token wakes nothing here.
        let other_run = OperationToken {
            stream: StreamAddress {
                namespace: NAMESPACE.to_string(),
                owner_kind: StreamOwnerKind::WorkflowActivity as i32,
                workflow_id: id.clone(),
                run_id: "run-2".to_string(),
                topic: TOPIC.to_string(),
                activity_id: "1".to_string(),
            },
            attach_request_id: "attach-1".to_string(),
            chain_first_run_id: "run-2".to_string(),
        }
        .encode();
        let refused = read(
            &calls,
            json!({"stream": tool("run-1"), "wake_token": other_run}),
        )
        .await
        .unwrap_err();
        assert_eq!(kind_of(&refused).0, "BAD_REQUEST");
        // A Workflow Activity's ref names its Workflow, its run and its Activity.
        for stream in [
            json!({"kind": "workflow_activity", "workflow_id": id, "activity_id": "1", "topic": "t"}),
            json!({"kind": "workflow_activity", "workflow_id": "", "activity_id": "1",
                   "chain_run_id": "run-1", "topic": "t"}),
            json!({"kind": "workflow_activity", "workflow_id": id, "chain_run_id": "run-1",
                   "topic": "t"}),
        ] {
            let refused = read(&calls, json!({ "stream": stream.clone() }))
                .await
                .unwrap_err();
            assert_eq!(kind_of(&refused).0, "BAD_REQUEST", "{stream}");
        }
    }

    pub(crate) async fn a_read_says_done_once_the_owner_ends(store: Arc<dyn StreamStore>) {
        let wf = workflow();
        write(
            &service(store.clone(), NexusOptions::default()),
            &wf,
            1,
            &["one"],
        )
        .await;
        let ended = NexusCalls::new(
            Arc::new(Streams::new(
                store,
                Arc::new(Ended),
                StreamsOptions::default(),
            )),
            NexusOptions::default(),
        );
        let mut collected = vec![];
        let mut after = String::new();
        for _ in 0..10 {
            let mut input = read_input(&wf, &after);
            input["wait_ms"] = json!(5000);
            let answer = read(&ended, input).await.unwrap();
            collected.extend(texts(&answer));
            if answer["done"] == true {
                assert_eq!(collected, ["one"]);
                return;
            }
            after = token(&answer).to_string();
        }
        panic!("the read never said done, after {collected:?}");
    }

    pub(crate) async fn a_refs_run_picks_the_chain_and_never_ends_a_read_there(
        store: Arc<dyn StreamStore>,
    ) {
        let wf = workflow();
        let calls = NexusCalls::new(
            Arc::new(Streams::new(
                store,
                Arc::new(Continued),
                StreamsOptions {
                    owner_check_min: std::time::Duration::from_millis(10),
                    ..StreamsOptions::default()
                },
            )),
            NexusOptions::default(),
        );
        write(&calls, &wf, 1, &["one"]).await;
        let mut input = read_input(&wf, "");
        input["stream"]["chain_run_id"] = json!("run-1");
        input["wait_ms"] = json!(300);
        let answer = read(&calls, input.clone()).await.unwrap();
        assert_eq!(texts(&answer), ["one"]);
        // `run-1` continued as new, so a read pinned to that run would end here.
        input["after_token"] = answer["next_token"].clone();
        input["state"] = answer["state"].clone();
        let answer = read(&calls, input).await.unwrap();
        assert_eq!(answer["done"], false, "{answer}");
    }

    pub(crate) async fn two_readers_on_one_stream_keep_their_own_places(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = Arc::new(service(store, NexusOptions::default()));
        let wf = workflow();
        write(&calls, &wf, 1, &["a", "b", "c", "d"]).await;
        let page = |after: String, max: i64| {
            let mut input = read_input(&wf, &after);
            input["max_records"] = json!(max);
            input
        };
        // The readers interleave their calls, and each resumes where it left off, so neither
        // takes over the other's read.
        let one = read(&calls, page(String::new(), 1)).await.unwrap();
        let two = read(&calls, page(String::new(), 3)).await.unwrap();
        let one = read(&calls, page(token(&one).to_string(), 1))
            .await
            .unwrap();
        let two = read(&calls, page(token(&two).to_string(), 3))
            .await
            .unwrap();
        assert_eq!(texts(&one), ["b"]);
        assert_eq!(texts(&two), ["d"]);
        // Both park on the tail, at the same place, and both are answered.
        let parked: Vec<_> = (0..2)
            .map(|_| {
                let calls = calls.clone();
                let mut input = read_input(&wf, token(&two));
                input["wait_ms"] = json!(10_000);
                tokio::spawn(async move { read(&calls, input).await.unwrap() })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(300)).await;
        write(&calls, &wf, 5, &["e"]).await;
        for answer in parked {
            let answer = tokio::time::timeout(Duration::from_secs(5), answer)
                .await
                .expect("a parked read never answered")
                .unwrap();
            assert_eq!(texts(&answer), ["e"]);
        }
    }

    pub(crate) async fn a_read_that_does_not_wait_answers_with_what_the_store_holds(
        store: Arc<dyn StreamStore>,
    ) {
        // A zero wait means "do not wait for new records", not "skip a fetch that needs I/O".
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        for start in (0..250).step_by(50) {
            let texts: Vec<String> = (start..start + 50).map(|n| format!("r{n}")).collect();
            let texts: Vec<&str> = texts.iter().map(String::as_str).collect();
            write(&calls, &wf, start + 1, &texts).await;
        }
        let mut sizes = vec![];
        let mut after = String::new();
        loop {
            let mut input = read_input(&wf, &after);
            input["wait_ms"] = json!(0);
            input["max_records"] = json!(100);
            let answer = read(&calls, input).await.unwrap();
            if records(&answer).is_empty() {
                break;
            }
            sizes.push(records(&answer).len());
            after = token(&answer).to_string();
        }
        assert_eq!(sizes, [100, 100, 50]);
    }

    pub(crate) async fn a_read_answer_stays_within_its_byte_budget(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let big = "x".repeat(300 * 1024);
        for n in 0..5 {
            write(&calls, &wf, n + 1, &[&format!("{n}{big}")]).await;
        }
        let mut sizes = vec![];
        let mut after = String::new();
        loop {
            let answer = read_after(&calls, &wf, &after).await;
            if records(&answer).is_empty() {
                break;
            }
            // The budget counts what crosses, the base64 of each record.
            let crossing: usize = records(&answer)
                .iter()
                .map(|wire| wire["record"].as_str().unwrap().len())
                .sum();
            assert!(crossing <= 1 << 20, "{crossing}");
            sizes.push(records(&answer).len());
            after = token(&answer).to_string();
        }
        assert_eq!(sizes, [2, 2, 1]);
    }

    pub(crate) async fn a_record_above_the_budget_still_crosses_alone(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let huge = format!("{}z", "y".repeat(1 << 20));
        write(&calls, &wf, 1, &[&huge, "small"]).await;
        let first = read_after(&calls, &wf, "").await;
        assert_eq!(records(&first).len(), 1);
        assert_eq!(texts(&first)[0].len(), huge.len());
        assert_eq!(
            texts(&read_after(&calls, &wf, token(&first)).await),
            ["small"]
        );
    }

    pub(crate) async fn an_append_above_the_record_limit_is_refused(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let big = "x".repeat(5000);
        let limited = call_with(
            append_input(&wf, "p", 1, 1, &[&big]),
            0,
            4096,
            Duration::from_secs(60),
        );
        let refused = drive(&calls, calls.append(limited).await, Codec::Sealing)
            .await
            .unwrap_err();
        assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
        assert!(refused.message.contains("limit"), "{}", refused.message);
        assert!(records(&read_after(&calls, &wf, "").await).is_empty());
    }

    pub(crate) async fn a_stored_record_above_the_limit_crosses_as_an_error(
        store: Arc<dyn StreamStore>,
    ) {
        // Written under the default limit, as another handler or an Activity may, then read by a
        // handler with a smaller one.
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, 1, &[&"x".repeat(5000), "small"]).await;
        let limited = call_with(read_input(&wf, ""), 0, 4096, Duration::from_secs(60));
        let answer = drive(&calls, calls.read(limited).await, Codec::Sealing)
            .await
            .unwrap();
        let [large, small] = records(&answer).as_slice() else {
            panic!("expected two records, got {answer}");
        };
        // The reader is told why, at the cursor it goes on past.
        assert!(
            large["error"].as_str().unwrap().contains("limit"),
            "{large}"
        );
        assert_eq!(large["record"], "");
        assert!(small.get("error").is_none());
        assert_eq!(texts(&answer), ["small"]);
    }
    pub(crate) async fn a_read_that_asks_to_arm_is_refused(store: Arc<dyn StreamStore>) {
        // The contract has no arm, and a read refuses members it doesn't know.
        let calls = service(store, NexusOptions::default());
        let mut input = read_input(&workflow(), "");
        input["arm"] = json!(true);
        let refused = read(&calls, input).await.unwrap_err();
        assert_eq!(kind_of(&refused).0, "BAD_REQUEST");
    }

    pub(crate) async fn a_busy_reader_never_waits(store: Arc<dyn StreamStore>) {
        let waker = Arc::new(Waker::default());
        let calls = service(store, NexusOptions::default()).with_callers(waker.clone());
        let wf = workflow();
        write(&calls, &wf, 1, &["one", "two"]).await;
        let answer = read(&calls, waking(&wf, "")).await.unwrap();
        assert_eq!(texts(&answer), ["one", "two"]);
        assert!(
            waker.waits.lock().unwrap().is_empty(),
            "a reader with records needs no wake"
        );
    }

    pub(crate) async fn an_empty_read_wakes_its_operation_after_its_position(
        store: Arc<dyn StreamStore>,
    ) {
        let waker = Arc::new(Waker::default());
        let calls = service(store, NexusOptions::default()).with_callers(waker.clone());
        let wf = workflow();
        let last = write(&calls, &wf, 1, &["one", "two"]).await;
        let caught_up = read(&calls, waking(&wf, &last)).await.unwrap();
        assert!(records(&caught_up).is_empty());
        assert_eq!(caught_up["done"], false);
        let waits = waker.waits.lock().unwrap().clone();
        let [(chain, topic, request_id, after)] = waits.as_slice() else {
            panic!("one wait, not {waits:?}");
        };
        assert_eq!(
            (chain.workflow_id.as_str(), chain.first_run_id.as_str()),
            (wf.as_str(), "run-1")
        );
        assert_eq!((topic.as_str(), request_id.as_str()), (TOPIC, "attach-1"));
        assert!(*after > 0, "the counter of the read's position");
    }

    pub(crate) async fn a_record_that_lands_before_the_wait_is_in_the_answer(
        store: Arc<dyn StreamStore>,
    ) {
        let waker = Arc::new(Waker::default());
        let calls = service(store.clone(), NexusOptions::default()).with_callers(waker.clone());
        let wf = workflow();
        let last = write(&calls, &wf, 1, &["one"]).await;
        let finish = temporalio_streams::stored_append_record(
            TOPIC,
            "late",
            1,
            1,
            &temporalio_streams::proto::AppendRecord {
                kind: StreamRecordKind::Finish as i32,
                ..Default::default()
            },
        )
        .unwrap();
        *waker.late.lock().unwrap() = Some((
            store,
            StoreAppendRequest {
                chain: Some(ChainId {
                    namespace: NAMESPACE.to_string(),
                    workflow_id: wf.clone(),
                    first_run_id: "run-1".to_string(),
                    ..Default::default()
                }),
                topic: TOPIC.to_string(),
                producer_id: "late".to_string(),
                attempt: 1,
                sequence: 1,
                digest: vec![7; 32],
                records: vec![finish.encode_to_vec()],
            },
        ));
        let answer = read(&calls, waking(&wf, &last)).await.unwrap();
        let stored = stored(&answer);
        assert_eq!(stored.len(), 1, "the reread found the record: {answer}");
        assert_eq!(stored[0].kind, StreamRecordKind::Finish as i32);
        assert_eq!(waker.waits.lock().unwrap().len(), 1);
    }

    pub(crate) async fn a_positions_reader_waits_and_rereads_too(store: Arc<dyn StreamStore>) {
        // The direct read path: a Workflow reader that keeps positions and reads through a
        // transport.
        let waker = Arc::new(Waker::default());
        let calls = service(store, NexusOptions::default()).with_callers(waker.clone());
        let wf = workflow();
        let last = write(&calls, &wf, 1, &["one"]).await;
        let mut input = waking(&wf, &last);
        input["floor_holder"] = json!("ns/run-9/reader");
        let caught_up = read(&calls, input).await.unwrap();
        assert!(records(&caught_up).is_empty());
        assert!(caught_up.get("replay").is_some(), "{caught_up}");
        assert_eq!(waker.waits.lock().unwrap().len(), 1);
    }

    pub(crate) async fn a_wake_token_of_another_chain_is_refused(store: Arc<dyn StreamStore>) {
        let waker = Arc::new(Waker::default());
        let calls = service(store, NexusOptions::default()).with_callers(waker.clone());
        let wf = workflow();
        let last = write(&calls, &wf, 1, &["one"]).await;
        let mut token = OperationToken::decode(NAMESPACE, &operation_token(&wf)).unwrap();
        token.chain_first_run_id = "run-0".to_string();
        let mut input = read_input(&wf, &last);
        input["wake_token"] = json!(token.encode());
        let refused = read(&calls, input).await.unwrap_err();
        assert_eq!(kind_of(&refused).0, "BAD_REQUEST");
        assert!(waker.waits.lock().unwrap().is_empty());
    }

    pub(crate) async fn a_refused_wait_answers_empty(store: Arc<dyn StreamStore>) {
        let waker = Arc::new(Waker::default());
        waker
            .refuse
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let calls = service(store, NexusOptions::default()).with_callers(waker.clone());
        let wf = workflow();
        let last = write(&calls, &wf, 1, &["one"]).await;
        let caught_up = read(&calls, waking(&wf, &last)).await.unwrap();
        assert!(records(&caught_up).is_empty());
        assert_eq!(waker.waits.lock().unwrap().len(), 1);
    }

    pub(crate) async fn a_wake_token_of_another_stream_is_refused(store: Arc<dyn StreamStore>) {
        let waker = Arc::new(Waker::default());
        let calls = service(store, NexusOptions::default()).with_callers(waker.clone());
        let wf = workflow();
        let mut input = read_input(&wf, "");
        input["wake_token"] = json!(operation_token("other-workflow"));
        let refused = read(&calls, input).await.unwrap_err();
        assert_eq!(kind_of(&refused).0, "BAD_REQUEST");
        assert!(waker.waits.lock().unwrap().is_empty());
    }
}

macro_rules! lanes {
    ($store:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(store) = $store.await else { return };
                super::cases::$name(store).await;
            }
        )*
    };
}

macro_rules! all_cases {
    ($store:expr) => {
        lanes!($store;
            an_append_reads_back_as_the_stored_record,
            a_batch_lands_in_order_and_a_cursor_resumes_strictly_after,
            max_records_caps_an_answer_and_the_next_call_continues,
            a_read_that_collects_nothing_answers_with_the_callers_cursor,
            a_parked_read_answers_when_a_record_arrives,
            latest_only_positions_a_reader_at_the_end,
            a_retried_append_is_written_once,
            a_divergent_or_stale_retry_is_refused,
            a_new_attempt_crosses_as_a_supersession_at_its_cursor,
            an_append_counts_the_producer_id_toward_the_record_limit,
            finish_is_a_call_of_its_own,
            a_foreign_cursor_is_refused,
            argument_mistakes_are_bad_requests,
            a_reference_this_release_cannot_open_is_refused,
            a_read_says_done_once_the_owner_ends,
            a_refs_run_picks_the_chain_and_never_ends_a_read_there,
            two_readers_on_one_stream_keep_their_own_places,
            a_read_that_does_not_wait_answers_with_what_the_store_holds,
            a_read_answer_stays_within_its_byte_budget,
            a_record_above_the_budget_still_crosses_alone,
            an_append_above_the_record_limit_is_refused,
            a_stored_record_above_the_limit_crosses_as_an_error,
            a_read_that_asks_to_arm_is_refused,
            a_busy_reader_never_waits,
            an_empty_read_wakes_its_operation_after_its_position,
            a_record_that_lands_before_the_wait_is_in_the_answer,
            a_positions_reader_waits_and_rereads_too,
            a_wake_token_of_another_stream_is_refused,
            an_activity_stream_appends_and_reads_apart_from_a_workflow,
            an_activity_read_says_done_once_the_run_ends,
            a_wake_token_of_another_activity_is_refused,
            an_empty_activity_read_wakes_its_operation_on_the_activity_run,
            a_workflow_activity_stream_reads_apart_from_its_workflow_and_other_runs,
            a_wake_token_of_another_chain_is_refused,
            a_refused_wait_answers_empty,
        );
    };
}

async fn on_memory() -> Option<Arc<dyn StreamStore>> {
    Some(memory())
}

mod memory_lane {
    all_cases!(super::on_memory());
}

#[cfg(feature = "redis")]
async fn on_redis() -> Option<Arc<dyn StreamStore>> {
    let Ok(url) = std::env::var("STREAMS_REDIS_URL") else {
        eprintln!("set STREAMS_REDIS_URL to run the Redis cases");
        return None;
    };
    let mut options = temporalio_streams::RedisStoreOptions::new(url);
    options.key_prefix = workflow();
    Some(Arc::new(
        temporalio_streams::RedisStore::connect(options)
            .await
            .unwrap(),
    ))
}

#[cfg(feature = "redis")]
mod redis_lane {
    all_cases!(super::on_redis());
}

/// Each store's own tests cover when a cursor expires. What this pins is how the answer crosses,
/// so it runs on memory, where a test can drop records.
#[tokio::test]
async fn an_expired_cursor_is_told_apart() {
    let memory = Arc::new(MemoryStore::new());
    let calls = service(memory.clone(), NexusOptions::default());
    let wf = workflow();
    let old = write(&calls, &wf, 1, &["one"]).await;
    write(&calls, &wf, 2, &["two"]).await;
    let chain = ChainId {
        namespace: NAMESPACE.to_string(),
        workflow_id: wf.clone(),
        first_run_id: "run-1".to_string(),
        ..Default::default()
    };
    memory.truncate(&chain, TOPIC, 0);
    let refused = read(&calls, read_input(&wf, &old)).await.unwrap_err();
    assert_eq!(
        kind_of(&refused),
        ("BAD_REQUEST", false, "StreamExpiredError")
    );
}

const HOLDER: &str = "ns/reader-run/1";

fn holding(wf: &str, after: &str) -> Value {
    let mut input = read_input(wf, after);
    input["floor_holder"] = json!(HOLDER);
    input
}

fn chain_of(wf: &str) -> ChainId {
    ChainId {
        namespace: NAMESPACE.to_string(),
        workflow_id: wf.to_string(),
        first_run_id: "run-1".to_string(),
        ..Default::default()
    }
}

/// The Nexus calls over `store`, and the stream layer they share.
fn layered(store: Arc<dyn StreamStore>) -> (Arc<Streams>, NexusCalls) {
    let streams = Arc::new(Streams::new(
        store,
        Arc::new(Running),
        StreamsOptions::default(),
    ));
    let calls = NexusCalls::new(streams.clone(), NexusOptions::default());
    (streams, calls)
}

/// Core's replay read of the range `answer` carries, for the read that sent `state`.
fn replay_of(wf: &str, answer: &Value, state: &[u8]) -> ReadRangeRequest {
    use base64::Engine;
    let replay = &answer["replay"];
    ReadRangeRequest {
        stream: Some(StreamAddress {
            namespace: NAMESPACE.to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: wf.to_string(),
            run_id: String::new(),
            topic: TOPIC.to_string(),
            ..Default::default()
        }),
        chain_first_run_id: replay["chain_first_run_id"].as_str().unwrap().to_string(),
        after: replay["after_token"].as_str().unwrap().to_string(),
        to: token(answer).to_string(),
        stored_records: replay["stored_records"].as_u64().unwrap() as u32,
        state: state.to_vec(),
        records_digest: base64::engine::general_purpose::STANDARD
            .decode(replay["records_digest"].as_str().unwrap())
            .unwrap(),
        floor_holder: String::new(),
        start_from_end: false,
    }
}

#[tokio::test]
async fn a_read_without_a_floor_holder_answers_no_range() {
    let calls = service(memory(), NexusOptions::default());
    let wf = workflow();
    write(&calls, &wf, 1, &["one"]).await;
    let answer = read_after(&calls, &wf, "").await;
    assert!(answer.get("replay").is_none(), "{answer}");
}

#[tokio::test]
async fn a_read_with_a_floor_holder_holds_what_it_read_against_retention() {
    let memory = Arc::new(MemoryStore::new());
    let calls = service(memory.clone(), NexusOptions::default());
    let held = workflow();
    let free = workflow();
    let mut starts = Vec::new();
    for wf in [&held, &free] {
        let first = write(&calls, wf, 1, &["one"]).await;
        write(&calls, wf, 2, &["two", "three"]).await;
        starts.push(first);
    }
    let answer = read(&calls, holding(&held, &starts[0])).await.unwrap();
    assert_eq!(texts(&answer), ["two", "three"]);
    let replay = &answer["replay"];
    assert_eq!(replay["chain_first_run_id"], "run-1");
    assert_eq!(replay["after_token"], starts[0].as_str());
    assert_eq!(replay["stored_records"], 2);
    read_after(&calls, &free, &starts[1]).await;
    for wf in [&held, &free] {
        memory.truncate(&chain_of(wf), TOPIC, 0);
    }
    assert_eq!(
        texts(&read_after(&calls, &held, &starts[0]).await),
        ["two", "three"],
        "the floor kept the records after the read's start"
    );
    let refused = read(&calls, read_input(&free, &starts[1]))
        .await
        .unwrap_err();
    assert_eq!(
        kind_of(&refused),
        ("BAD_REQUEST", false, "StreamExpiredError")
    );
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn a_read_with_a_floor_holder_holds_what_it_read_on_redis() {
    let Ok(url) = std::env::var("STREAMS_REDIS_URL") else {
        eprintln!("set STREAMS_REDIS_URL to run the Redis cases");
        return;
    };
    let mut options = temporalio_streams::RedisStoreOptions::new(url);
    options.key_prefix = workflow();
    options.retention = Duration::from_millis(200);
    options.floor_lease = Some(Duration::from_secs(60));
    let store = Arc::new(
        temporalio_streams::RedisStore::connect(options)
            .await
            .unwrap(),
    );
    let calls = service(store, NexusOptions::default());
    let wf = workflow();
    let first = write(&calls, &wf, 1, &["one"]).await;
    write(&calls, &wf, 2, &["two", "three"]).await;
    let answer = read(&calls, holding(&wf, &first)).await.unwrap();
    assert_eq!(answer["replay"]["stored_records"], 2);
    // Past the retention, an append's trim would drop the records without the floor.
    tokio::time::sleep(Duration::from_millis(400)).await;
    write(&calls, &wf, 4, &["four"]).await;
    assert_eq!(
        texts(&read_after(&calls, &wf, &first).await),
        ["two", "three", "four"]
    );
}

#[tokio::test]
async fn a_cut_answer_ranges_over_the_records_that_crossed() {
    let (streams, calls) = layered(memory());
    let wf = workflow();
    let big = "x".repeat(300 * 1024);
    write(&calls, &wf, 1, &[&format!("1{big}"), &format!("2{big}")]).await;
    // The newer attempt's supersession fits the budget, but its record doesn't, so the cut
    // leaves the supersession for the next answer.
    append(&calls, append_input(&wf, "p", 2, 1, &[&format!("3{big}")]))
        .await
        .unwrap();
    let answer = read(&calls, holding(&wf, "")).await.unwrap();
    assert_eq!(records(&answer).len(), 2, "the budget cut the answer");
    assert!(
        records(&answer)
            .iter()
            .all(|r| r.get("superseded").is_none())
    );
    let replay = &answer["replay"];
    assert_eq!(replay["after_token"], "");
    assert_eq!(replay["stored_records"], 2);
    let replayed = streams
        .read_range(replay_of(&wf, &answer, &[]))
        .await
        .unwrap();
    assert_eq!(replayed.records.len(), 2);
    assert_eq!(replayed.cursor, token(&answer));
    // Core's page went on past the cut, so its digest is not the answer's.
    let page = streams
        .read(ReadRequest {
            stream: replay_of(&wf, &answer, &[]).stream,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.records.len(), 4);
    assert_ne!(
        json!(base64_of(&page.records_digest)),
        replay["records_digest"]
    );
}

fn base64_of(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[tokio::test]
async fn a_holding_read_from_the_end_starts_at_latest_only_and_replays() {
    let (streams, calls) = layered(memory());
    let wf = workflow();
    write(&calls, &wf, 1, &["one"]).await;
    let mut latest = read_input(&wf, "");
    latest.as_object_mut().unwrap().remove("after_token");
    latest["latest_only"] = json!(true);
    let end = read(&calls, latest).await.unwrap();
    append(&calls, append_input(&wf, "p", 2, 1, &["two"]))
        .await
        .unwrap();
    let live = read(&calls, holding(&wf, token(&end))).await.unwrap();
    assert_eq!(values(&live), ["SUPERSEDED p 1->2", "p@2 two"]);
    let replayed = streams
        .read_range(replay_of(&wf, &live, &[]))
        .await
        .unwrap();
    assert_eq!(
        replayed.records.len(),
        2,
        "the replay sees what the live read saw"
    );
}

#[tokio::test]
async fn a_range_replays_with_the_state_the_read_sent() {
    let (streams, calls) = layered(memory());
    let wf = workflow();
    write(&calls, &wf, 1, &["one"]).await;
    let first = read(&calls, holding(&wf, "")).await.unwrap();
    append(&calls, append_input(&wf, "p", 2, 1, &["two"]))
        .await
        .unwrap();
    let mut input = holding(&wf, token(&first));
    input["state"] = first["state"].clone();
    let second = read(&calls, input).await.unwrap();
    assert_eq!(values(&second), ["SUPERSEDED p 1->2", "p@2 two"]);
    let state = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD
            .decode(first["state"].as_str().unwrap())
            .unwrap()
    };
    let replayed = streams
        .read_range(replay_of(&wf, &second, &state))
        .await
        .unwrap();
    assert_eq!(
        replayed.records.len(),
        2,
        "the supersession comes out again"
    );
}

#[tokio::test]
async fn a_record_the_reader_cannot_parse_has_an_empty_range_at_its_cursor() {
    let memory = Arc::new(MemoryStore::new());
    let (streams, calls) = layered(memory.clone());
    let wf = workflow();
    memory
        .append(StoreAppendRequest {
            chain: Some(chain_of(&wf)),
            topic: TOPIC.to_string(),
            producer_id: "raw".to_string(),
            attempt: 1,
            sequence: 1,
            digest: vec![1; 32],
            records: vec![b"\xff".to_vec()],
        })
        .await
        .unwrap();
    let answer = read(&calls, holding(&wf, "")).await.unwrap();
    let [wire] = records(&answer).as_slice() else {
        panic!("expected one record, got {answer}");
    };
    assert!(wire.get("error").is_some(), "{answer}");
    let replay = &answer["replay"];
    assert_eq!(replay["after_token"], token(&answer));
    assert_eq!(replay["stored_records"], 0);
    assert_eq!(replay["chain_first_run_id"], "run-1");
    let replayed = streams
        .read_range(replay_of(&wf, &answer, &[]))
        .await
        .unwrap();
    assert!(replayed.records.is_empty());
}

#[tokio::test]
async fn a_floor_holder_names_the_caller_and_starts_at_a_cursor() {
    let calls = service(memory(), NexusOptions::default());
    let wf = workflow();
    write(&calls, &wf, 1, &["one"]).await;
    for holder in ["reader-run/1", "ns//1", "archive"] {
        let mut input = read_input(&wf, "");
        input["floor_holder"] = json!(holder);
        let refused = read(&calls, input).await.unwrap_err();
        assert_eq!(
            kind_of(&refused),
            ("BAD_REQUEST", false, "ValueError"),
            "{holder}"
        );
    }
    let refused = read(&calls, holding(&wf, "$end")).await.unwrap_err();
    assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
    assert!(
        refused.message.contains("latest_only"),
        "{}",
        refused.message
    );
}

#[tokio::test]
async fn a_floor_holder_is_at_most_512_bytes_and_never_beside_latest_only() {
    let calls = service(memory(), NexusOptions::default());
    let wf = workflow();
    write(&calls, &wf, 1, &["one"]).await;
    let mut input = read_input(&wf, "");
    input["floor_holder"] = json!(format!("ns/run/{}", "h".repeat(505)));
    read(&calls, input.clone()).await.unwrap();
    input["floor_holder"] = json!(format!("ns/run/{}", "h".repeat(506)));
    let refused = read(&calls, input).await.unwrap_err();
    assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
    assert!(refused.message.contains("512"), "{}", refused.message);
    let mut latest = holding(&wf, "");
    latest.as_object_mut().unwrap().remove("after_token");
    latest["latest_only"] = json!(true);
    let refused = read(&calls, latest).await.unwrap_err();
    assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
    assert!(refused.message.contains("floor"), "{}", refused.message);
}

/// A cut answer and the answer after it, with a supersession, each read again through
/// `read_range`, cross the same way the read's did.
async fn a_range_replays_as_it_crossed(store: Arc<dyn StreamStore>) {
    let calls = service(store, NexusOptions::default());
    let wf = workflow();
    let big = "x".repeat(300 * 1024);
    write(&calls, &wf, 1, &[&format!("1{big}"), &format!("2{big}")]).await;
    append(&calls, append_input(&wf, "p", 2, 1, &[&format!("3{big}")]))
        .await
        .unwrap();
    let first = read(&calls, holding(&wf, "")).await.unwrap();
    assert_eq!(records(&first).len(), 2, "the budget cut the answer");
    let mut input = holding(&wf, token(&first));
    input["state"] = first["state"].clone();
    let second = read(&calls, input).await.unwrap();
    assert_eq!(values(&second)[0], "SUPERSEDED p 1->2");
    for (answer, state) in [(&first, Value::Null), (&second, first["state"].clone())] {
        let replayed = read_range(&calls, replay_input(stream(&wf, TOPIC), answer, state))
            .await
            .unwrap();
        assert_eq!(replayed["records"], answer["records"]);
        assert_eq!(replayed["next_token"], answer["next_token"]);
        assert_eq!(replayed["done"], false);
        assert!(replayed.get("state").is_none() && replayed.get("replay").is_none());
    }
}

#[tokio::test]
async fn a_range_replays_as_it_crossed_on_memory() {
    a_range_replays_as_it_crossed(memory()).await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn a_range_replays_as_it_crossed_on_redis() {
    let Some(store) = on_redis().await else {
        return;
    };
    a_range_replays_as_it_crossed(store).await;
}

/// A held read of two records on `wf`, through `calls`.
async fn held_read(calls: &NexusCalls, wf: &str) -> Value {
    write(calls, wf, 1, &["one", "two"]).await;
    read(calls, holding(wf, "")).await.unwrap()
}

#[tokio::test]
async fn a_range_with_other_records_is_a_mismatch() {
    let calls = service(memory(), NexusOptions::default());
    let wf = workflow();
    let answer = held_read(&calls, &wf).await;
    let mut other_digest = replay_input(stream(&wf, TOPIC), &answer, Value::Null);
    other_digest["records_digest"] = json!(base64_of(&[0; 32]));
    let mut other_count = replay_input(stream(&wf, TOPIC), &answer, Value::Null);
    other_count["stored_records"] = json!(1);
    for input in [other_digest, other_count] {
        let refused = read_range(&calls, input).await.unwrap_err();
        assert_eq!(
            kind_of(&refused),
            ("BAD_REQUEST", false, "StreamReplayMismatchError")
        );
    }
}

#[tokio::test]
async fn a_range_the_store_dropped_is_lost_and_a_replay_holds_it_again() {
    let memory = Arc::new(MemoryStore::new());
    let (streams, calls) = layered(memory.clone());
    let wf = workflow();
    let answer = held_read(&calls, &wf).await;
    let release = |holder: &str| temporalio_streams::proto::ReleaseFloorRequest {
        stream: replay_of(&wf, &answer, &[]).stream,
        chain_first_run_id: "run-1".to_string(),
        floor_holder: holder.to_string(),
    };
    streams.release_floor(release(HOLDER)).await.unwrap();
    // A replay that names a holder holds the range again, as after a reset.
    let mut renewing = replay_input(stream(&wf, TOPIC), &answer, Value::Null);
    renewing["floor_holder"] = json!("ns/reset-run/1");
    read_range(&calls, renewing).await.unwrap();
    memory.truncate(&chain_of(&wf), TOPIC, 0);
    let input = replay_input(stream(&wf, TOPIC), &answer, Value::Null);
    let replayed = read_range(&calls, input.clone()).await.unwrap();
    assert_eq!(replayed["records"], answer["records"]);
    streams
        .release_floor(release("ns/reset-run/1"))
        .await
        .unwrap();
    memory.truncate(&chain_of(&wf), TOPIC, 0);
    let refused = read_range(&calls, input).await.unwrap_err();
    assert_eq!(
        kind_of(&refused),
        ("BAD_REQUEST", false, "StreamReplayRangeLostError")
    );
}

#[tokio::test]
async fn a_range_names_a_chain_of_the_workflow_and_a_count_a_read_can_answer() {
    let calls = service(memory(), NexusOptions::default());
    let wf = workflow();
    let answer = held_read(&calls, &wf).await;
    let mut other_chain = replay_input(stream(&wf, TOPIC), &answer, Value::Null);
    other_chain["chain_first_run_id"] = json!("run-9");
    let refused = read_range(&calls, other_chain).await.unwrap_err();
    assert_eq!(
        kind_of(&refused),
        ("BAD_REQUEST", false, "StreamCursorError")
    );
    for count in [-1, 1001] {
        let mut input = replay_input(stream(&wf, TOPIC), &answer, Value::Null);
        input["stored_records"] = json!(count);
        let refused = read_range(&calls, input).await.unwrap_err();
        assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
    }
}

/// A reader's first range starts at the stream's start, so it has no cursor to find gone. Once
/// its hold lapsed and retention trimmed the range, later records stand where it began.
#[cfg(feature = "redis")]
#[tokio::test]
async fn a_range_from_a_trimmed_start_is_lost_on_redis() {
    let Ok(url) = std::env::var("STREAMS_REDIS_URL") else {
        eprintln!("set STREAMS_REDIS_URL to run the Redis cases");
        return;
    };
    let mut options = temporalio_streams::RedisStoreOptions::new(url);
    options.key_prefix = workflow();
    options.retention = Duration::from_millis(300);
    options.floor_lease = Some(Duration::from_millis(100));
    let store = Arc::new(
        temporalio_streams::RedisStore::connect(options)
            .await
            .unwrap(),
    );
    let calls = service(store, NexusOptions::default());
    let wf = workflow();
    let answer = held_read(&calls, &wf).await;
    assert_eq!(answer["replay"]["after_token"], "");
    tokio::time::sleep(Duration::from_millis(600)).await;
    write(&calls, &wf, 3, &["three"]).await;
    write(&calls, &wf, 4, &["four"]).await;
    let refused = read_range(
        &calls,
        replay_input(stream(&wf, TOPIC), &answer, Value::Null),
    )
    .await
    .unwrap_err();
    assert_eq!(
        kind_of(&refused),
        ("BAD_REQUEST", false, "StreamReplayRangeLostError"),
        "{}",
        refused.message
    );
}

#[tokio::test]
async fn an_activity_range_replays_and_names_only_the_activity_run() {
    let calls = service(memory(), NexusOptions::default());
    let id = workflow();
    let mut input = append_input(&id, "p", 1, 1, &["one", "two"]);
    input["stream"] = activity_stream(&id, TOPIC);
    append(&calls, input).await.unwrap();
    let answer = read(
        &calls,
        json!({"stream": activity_stream(&id, TOPIC), "floor_holder": HOLDER}),
    )
    .await
    .unwrap();
    assert_eq!(answer["replay"]["chain_first_run_id"], "run-1");
    let replay = replay_input(activity_stream(&id, TOPIC), &answer, Value::Null);
    let replayed = read_range(&calls, replay.clone()).await.unwrap();
    assert_eq!(replayed["records"], answer["records"]);
    let mut other_run = replay;
    other_run["chain_first_run_id"] = json!("run-9");
    let refused = read_range(&calls, other_run).await.unwrap_err();
    assert_eq!(
        kind_of(&refused),
        ("BAD_REQUEST", false, "StreamCursorError")
    );
}

#[tokio::test]
async fn an_activity_streams_codec_work_names_the_activity() {
    let calls = service(memory(), NexusOptions::default());
    let mut input = append_input("unused", "p", 1, 1, &["one"]);
    input["stream"] = activity_stream("tool-1", TOPIC);
    let step = calls.append(call(input)).await;
    let Some(temporalio_streams::proto::nexus_step::Step::Codec(work)) = step.step else {
        panic!("an append asks for codec work first, not {step:?}");
    };
    assert_eq!(
        (
            work.namespace.as_str(),
            work.owner_kind,
            work.workflow_id.as_str(),
            work.activity_id.as_str()
        ),
        (NAMESPACE, StreamOwnerKind::Activity as i32, "", "tool-1")
    );
}

#[tokio::test]
async fn a_workflow_activity_range_replays_and_names_only_its_run() {
    let calls = service(memory(), NexusOptions::default());
    let id = workflow();
    let tool = workflow_activity_stream(&id, "run-3", "1", TOPIC);
    let mut input = append_input(&id, "p", 1, 1, &["one", "two"]);
    input["stream"] = tool.clone();
    append(&calls, input).await.unwrap();
    let answer = read(&calls, json!({"stream": tool, "floor_holder": HOLDER}))
        .await
        .unwrap();
    assert_eq!(answer["replay"]["chain_first_run_id"], "run-3");
    // A later call carries the state, which names the run.
    let next = read(
        &calls,
        json!({"stream": tool, "after_token": answer["next_token"], "state": answer["state"]}),
    )
    .await
    .unwrap();
    assert!(texts(&next).is_empty());
    let replay = replay_input(tool.clone(), &answer, Value::Null);
    let replayed = read_range(&calls, replay.clone()).await.unwrap();
    assert_eq!(replayed["records"], answer["records"]);
    let mut other_run = replay;
    other_run["chain_first_run_id"] = json!("run-9");
    let refused = read_range(&calls, other_run).await.unwrap_err();
    assert_eq!(
        kind_of(&refused),
        ("BAD_REQUEST", false, "StreamCursorError")
    );
}

#[tokio::test]
async fn a_workflow_activity_streams_codec_work_names_the_activity_and_its_workflow() {
    let calls = service(memory(), NexusOptions::default());
    let mut input = append_input("unused", "p", 1, 1, &["one"]);
    input["stream"] = workflow_activity_stream("agent", "run-1", "tool-42", TOPIC);
    let step = calls.append(call(input)).await;
    let Some(temporalio_streams::proto::nexus_step::Step::Codec(work)) = step.step else {
        panic!("an append asks for codec work first, not {step:?}");
    };
    assert_eq!(
        (
            work.owner_kind,
            work.workflow_id.as_str(),
            work.activity_id.as_str()
        ),
        (StreamOwnerKind::WorkflowActivity as i32, "agent", "tool-42")
    );
}
