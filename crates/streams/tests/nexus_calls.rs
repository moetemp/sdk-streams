//! The stream service's Nexus calls as Core serves them: the state a reader carries, the cut, the
//! codec step and what crosses when a record can't.
//!
//! The cases run on memory, and on Redis when `STREAMS_REDIS_URL` is set.

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
use temporalio_streams::{
    MAX_TRACKED_PRODUCERS, StreamStore,
    nexus::{MAX_STATE_BYTES, NexusOptions},
    proto::{
        ChainId, CodecResult, NexusFailure, NexusResumeRequest, StoreAppendRequest, StreamRecord,
        StreamRecordKind, codec_result, nexus_step,
    },
};

static IDS: AtomicUsize = AtomicUsize::new(0);

fn workflow() -> String {
    format!(
        "nexus-{}-{}",
        std::process::id(),
        IDS.fetch_add(1, Ordering::Relaxed)
    )
}

const TOPIC: &str = "out";

/// Appends `values` as attempt `attempt` of `producer`, from `sequence`.
async fn write(
    calls: &temporalio_streams::nexus::NexusCalls,
    workflow_id: &str,
    producer: &str,
    attempt: i64,
    sequence: i64,
    values: &[&str],
) -> String {
    let answer = append(
        calls,
        json!({
            "stream": stream(workflow_id, TOPIC),
            "producer_id": producer,
            "attempt": attempt,
            "sequence": sequence,
            "payloads": values.iter().map(|value| body(value)).collect::<Vec<_>>(),
        }),
    )
    .await
    .unwrap();
    answer["cursor"].as_str().unwrap().to_string()
}

fn read_input(workflow_id: &str, after: &str, state: &Value) -> Value {
    let mut input = json!({"stream": stream(workflow_id, TOPIC), "after_token": after});
    if !state.is_null() {
        input["state"] = state.clone();
    }
    input
}

fn kind_of(failure: &NexusFailure) -> (&str, bool, &str) {
    let class = failure.message.split(':').next().unwrap();
    (
        failure.handler_error_type.as_str(),
        failure.retryable,
        class,
    )
}

/// Stores `bytes` as the next entry of the topic, as a writer outside Core could.
async fn raw_entry(store: &Arc<dyn StreamStore>, workflow_id: &str, bytes: Vec<u8>) {
    store
        .append(StoreAppendRequest {
            chain: Some(ChainId {
                namespace: NAMESPACE.to_string(),
                workflow_id: workflow_id.to_string(),
                first_run_id: "run-1".to_string(),
                ..Default::default()
            }),
            topic: TOPIC.to_string(),
            producer_id: "raw".to_string(),
            attempt: 1,
            sequence: IDS.fetch_add(1, Ordering::Relaxed) as i64 + 1,
            digest: vec![7; 32],
            records: vec![bytes],
        })
        .await
        .unwrap();
}

mod cases {
    use super::*;

    pub(crate) async fn an_append_reads_back_with_its_body_decoded(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let cursor = write(&calls, &wf, "p", 1, 1, &["one", "two"]).await;
        let answer = read(&calls, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        assert_eq!(values(&answer), ["p@1 one", "p@1 two"]);
        assert_eq!(answer["next_token"], cursor);
        assert_eq!(answer["done"], false);
        assert!(answer["state"].is_string());
    }

    pub(crate) async fn rule_101_a_record_that_does_not_parse_crosses_as_an_error_at_its_cursor(
        store: Arc<dyn StreamStore>,
    ) {
        // One record the store's reader can't parse used to fail every read as a retryable
        // error, so a Workflow's read retried the same call until it timed out.
        let calls = service(store.clone(), NexusOptions::default());
        let wf = workflow();
        raw_entry(&store, &wf, b"\xff not a record".to_vec()).await;
        write(&calls, &wf, "p", 1, 1, &["after"]).await;
        let first = read(&calls, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        let records = first["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        let error = records[0]["error"].as_str().unwrap();
        assert!(error.contains("could not be decoded"), "{error}");
        assert_eq!(records[0]["record"], "");
        assert_eq!(first["next_token"], records[0]["token"]);
        // The reader goes on past it.
        let next = read(
            &calls,
            read_input(&wf, first["next_token"].as_str().unwrap(), &first["state"]),
        )
        .await
        .unwrap();
        assert_eq!(values(&next), ["p@1 after"]);
    }

    pub(crate) async fn rule_101_a_read_never_fails_on_a_record_it_cant_parse(
        store: Arc<dyn StreamStore>,
    ) {
        // Unparsable entries first, in the middle and last of a page, so the store's reader
        // meets each one both at the start of a call and partway through one.
        let calls = service(store.clone(), NexusOptions::default());
        let wf = workflow();
        raw_entry(&store, &wf, b"\xff first".to_vec()).await;
        write(&calls, &wf, "p", 1, 1, &["one", "two"]).await;
        raw_entry(&store, &wf, Vec::new()).await;
        write(&calls, &wf, "p", 1, 3, &["three"]).await;
        raw_entry(&store, &wf, b"\xff last".to_vec()).await;
        let (mut after, mut state) = (String::new(), Value::Null);
        let mut seen = Vec::new();
        for _ in 0..10 {
            let answer = match read(&calls, read_input(&wf, &after, &state)).await {
                Ok(answer) => answer,
                Err(failure) => panic!("a read failed: {}", failure.message),
            };
            if answer["records"].as_array().unwrap().is_empty() {
                break;
            }
            seen.extend(values(&answer).into_iter().map(|value| {
                if value.starts_with("ERROR") {
                    "ERROR".to_string()
                } else {
                    value
                }
            }));
            after = answer["next_token"].as_str().unwrap().to_string();
            state = answer["state"].clone();
        }
        assert_eq!(
            seen,
            ["ERROR", "p@1 one", "p@1 two", "ERROR", "p@1 three", "ERROR"]
        );
    }

    pub(crate) async fn a_body_the_codec_refuses_crosses_as_an_error_and_the_rest_reads(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store.clone(), NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "p", 1, 1, &["before"]).await;
        // Written past the codec, so the handler's codec can't decode it.
        let unsealed = temporalio_streams::stored_append_record(
            TOPIC,
            "raw",
            1,
            1,
            &temporalio_streams::proto::AppendRecord {
                kind: StreamRecordKind::Data as i32,
                body: Some(Default::default()),
                content_hash: vec![1; 32],
                ..Default::default()
            },
        )
        .unwrap();
        raw_entry(&store, &wf, unsealed.encode_to_vec()).await;
        write(&calls, &wf, "p", 1, 2, &["after"]).await;
        let answer = read(&calls, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        let seen = values(&answer);
        assert_eq!(seen[0], "p@1 before");
        assert!(
            seen[1].starts_with("ERROR the record's body could not be decoded"),
            "{seen:?}"
        );
        assert_eq!(seen[2], "p@1 after");
    }

    pub(crate) async fn rule_99_a_cut_hands_back_the_state_at_its_cut(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "a", 1, 1, &["a1"]).await;
        write(&calls, &wf, "c", 1, 1, &["c1"]).await;
        write(&calls, &wf, "c", 2, 1, &[&"x".repeat(4000)]).await;
        let first = drive(
            &calls,
            calls
                .read(call_with(
                    read_input(&wf, "", &Value::Null),
                    2000,
                    0,
                    Duration::from_secs(60),
                ))
                .await,
            Codec::Sealing,
        )
        .await
        .unwrap();
        // The supersession belongs to the record the budget left out, so it waits with it.
        assert_eq!(values(&first), ["a@1 a1", "c@1 c1"]);
        assert_eq!(first["done"], false);
        write(&calls, &wf, "a", 2, 1, &["a2"]).await;
        let next = read(
            &calls,
            read_input(&wf, first["next_token"].as_str().unwrap(), &first["state"]),
        )
        .await
        .unwrap();
        let seen = values(&next);
        assert_eq!(seen[0], "SUPERSEDED c 1->2");
        assert!(seen[1].starts_with("c@2 xxx"), "{seen:?}");
        assert_eq!(seen[2..], ["SUPERSEDED a 1->2", "a@2 a2"]);
    }

    pub(crate) async fn rule_99_a_state_carries_across_handler_workers(
        store: Arc<dyn StreamStore>,
    ) {
        // Two Workers serve one endpoint. The second knows nothing of the first's calls, so the
        // state the reader carries is all it has.
        let first_worker = service(store.clone(), NexusOptions::default());
        let second_worker = service(store, NexusOptions::default());
        let wf = workflow();
        write(&first_worker, &wf, "a", 1, 1, &["a1"]).await;
        write(&first_worker, &wf, "b", 1, 1, &["b1"]).await;
        let first = read(&first_worker, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        write(&first_worker, &wf, "a", 2, 1, &["a2"]).await;
        let next = read(
            &second_worker,
            read_input(&wf, first["next_token"].as_str().unwrap(), &first["state"]),
        )
        .await
        .unwrap();
        assert_eq!(values(&next), ["SUPERSEDED a 1->2", "a@2 a2"]);
    }

    pub(crate) async fn rule_99_a_state_naming_another_chain_is_refused(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "p", 1, 1, &["one"]).await;
        // `started` and a first run id the Workflow's chains don't start with.
        let mut forged = vec![0x10, 0x01, 0x1a, 5];
        forged.extend(b"run-9");
        use base64::Engine;
        let forged = Value::String(base64::engine::general_purpose::STANDARD.encode(forged));
        let failure = read(&calls, read_input(&wf, "", &forged))
            .await
            .unwrap_err();
        assert_eq!(
            kind_of(&failure),
            ("BAD_REQUEST", false, "StreamCursorError")
        );
        let garbage = Value::String("//8=".to_string());
        let failure = read(&calls, read_input(&wf, "", &garbage))
            .await
            .unwrap_err();
        assert_eq!(
            kind_of(&failure),
            ("BAD_REQUEST", false, "StreamCursorError")
        );
    }

    pub(crate) async fn a_read_waits_no_longer_than_its_deadline_allows(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let mut input = read_input(&wf, "", &Value::Null);
        input["wait_ms"] = json!(30_000);
        let started = tokio::time::Instant::now();
        let answer = drive(
            &calls,
            calls
                .read(call_with(input, 0, 0, Duration::from_millis(800)))
                .await,
            Codec::Sealing,
        )
        .await
        .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(values(&answer), Vec::<String>::new());
        assert_eq!(answer["next_token"], "");
    }

    pub(crate) async fn a_retry_through_a_fresh_nonce_lands_once(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let input = json!({
            "stream": stream(&wf, TOPIC),
            "producer_id": "p",
            "attempt": 1,
            "sequence": 1,
            "payloads": [body("one")],
        });
        let first = drive(
            &calls,
            calls.append(call(input.clone())).await,
            Codec::Nonce(1),
        )
        .await
        .unwrap();
        let retry = drive(&calls, calls.append(call(input)).await, Codec::Nonce(2))
            .await
            .unwrap();
        assert_eq!(first, retry);
    }

    pub(crate) async fn a_body_the_codec_refuses_fails_its_append_and_writes_nothing(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let failure = append(
            &calls,
            json!({
                "stream": stream(&wf, TOPIC),
                "producer_id": "p",
                "attempt": 1,
                "sequence": 1,
                "payloads": [body("poison")],
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(kind_of(&failure), ("INTERNAL", true, "PayloadCodecError"));
        let answer = read(&calls, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        assert_eq!(values(&answer), Vec::<String>::new());
    }

    pub(crate) async fn a_supersession_crosses_at_the_cursor_of_the_record_before(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "chat", 1, 1, &["a", "b"]).await;
        let second = write(&calls, &wf, "chat", 2, 1, &["a2"]).await;
        let answer = read(&calls, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        assert_eq!(
            values(&answer),
            ["chat@1 a", "chat@1 b", "SUPERSEDED chat 1->2", "chat@2 a2"]
        );
        let records = answer["records"].as_array().unwrap();
        assert_eq!(records[2]["token"], records[1]["token"]);
        assert_eq!(records[3]["token"], second);
    }

    pub(crate) async fn a_resumed_reader_takes_the_supersession_at_its_cursor(
        store: Arc<dyn StreamStore>,
    ) {
        // A run after Continue-as-New reads on from the earlier run's cursor with no state.
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let delivered = write(&calls, &wf, "chat", 1, 1, &["a", "b", "c"]).await;
        write(&calls, &wf, "chat", 2, 1, &["a2", "b2"]).await;
        let answer = read(&calls, read_input(&wf, &delivered, &Value::Null))
            .await
            .unwrap();
        assert_eq!(
            values(&answer),
            ["SUPERSEDED chat 1->2", "chat@2 a2", "chat@2 b2"]
        );
        assert_eq!(answer["records"][0]["token"], delivered);
    }

    pub(crate) async fn a_supersession_seen_across_two_calls_crosses_once(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "chat", 1, 1, &["a", "b"]).await;
        let first = read(&calls, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        write(&calls, &wf, "chat", 2, 1, &["a2"]).await;
        let next = read(
            &calls,
            read_input(&wf, first["next_token"].as_str().unwrap(), &first["state"]),
        )
        .await
        .unwrap();
        let mut seen = values(&first);
        seen.extend(values(&next));
        assert_eq!(
            seen,
            ["chat@1 a", "chat@1 b", "SUPERSEDED chat 1->2", "chat@2 a2"]
        );
    }

    pub(crate) async fn a_stale_record_crosses_marked(store: Arc<dyn StreamStore>) {
        // Attempt 1 timed out but kept running, and wrote once more after attempt 2 started.
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "chat", 1, 1, &["a"]).await;
        write(&calls, &wf, "chat", 2, 1, &["a2"]).await;
        write(&calls, &wf, "chat", 1, 2, &["late"]).await;
        write(&calls, &wf, "chat", 2, 2, &["b2"]).await;
        let answer = read(&calls, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        assert_eq!(
            values(&answer),
            [
                "chat@1 a",
                "SUPERSEDED chat 1->2",
                "chat@2 a2",
                "chat@1 late stale",
                "chat@2 b2"
            ]
        );
    }

    pub(crate) async fn the_state_stays_within_its_budget(store: Arc<dyn StreamStore>) {
        // The state rides in the caller's History on each call, so it is measured here.
        for (producers, id_bytes) in [(1, 24), (10, 24), (1000, 24), (1000, 200)] {
            let calls = service(store.clone(), NexusOptions::default());
            let wf = workflow();
            for index in 0..producers {
                let producer = format!("{index:0width$}", width = id_bytes);
                write(&calls, &wf, &producer, 1, 1, &["v"]).await;
            }
            let mut input = read_input(&wf, "", &Value::Null);
            input["max_records"] = json!(1000);
            let answer = read(&calls, input).await.unwrap();
            use base64::Engine;
            let state = base64::engine::general_purpose::STANDARD
                .decode(answer["state"].as_str().unwrap())
                .unwrap();
            eprintln!(
                "state for {producers} producers with {id_bytes}-byte ids: {} bytes",
                state.len()
            );
            assert!(state.len() <= MAX_STATE_BYTES, "{}", state.len());
            // The newest producer is the last one forgotten.
            let newest = format!("{:0width$}", producers - 1, width = id_bytes);
            assert!(
                state.windows(newest.len()).any(|w| w == newest.as_bytes()),
                "the newest producer was forgotten"
            );
        }
    }

    pub(crate) async fn a_state_above_its_budget_is_refused_before_it_decodes(
        store: Arc<dyn StreamStore>,
    ) {
        // A state crosses as caller input, so a forged or buggy one must not stall Core.
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "p", 1, 1, &["one"]).await;
        let first = read(&calls, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        let after = first["next_token"].as_str().unwrap();
        let real = decode_state(&first["state"]);
        let too_many = forged_state(&real, MAX_TRACKED_PRODUCERS + 1, 4);
        let too_large = forged_state(&real, 600, 120);
        let huge = forged_state(&real, 60_000, 6);
        assert!(huge.as_str().unwrap().len() > 1_300_000);
        for (name, state) in [
            ("too many producers", too_many),
            ("too many bytes", too_large),
            ("1.3 MB", huge),
        ] {
            let started = tokio::time::Instant::now();
            let failure = read(&calls, read_input(&wf, after, &state))
                .await
                .expect_err(name);
            assert_eq!(
                kind_of(&failure),
                ("BAD_REQUEST", false, "StreamCursorError"),
                "{name}"
            );
            assert!(started.elapsed() < Duration::from_secs(2), "{name}");
        }
    }

    pub(crate) async fn a_cut_read_asks_the_codec_only_for_what_fits(store: Arc<dyn StreamStore>) {
        // Each record crosses at about 267 KB, so the default 1 MiB budget fits three.
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let big = "x".repeat(200_000);
        for sequence in 1..=10 {
            write(&calls, &wf, "p", 1, sequence, &[&big]).await;
        }
        let (mut after, mut state) = (String::new(), Value::Null);
        let mut read_count = 0;
        while read_count < 10 {
            let step = calls.read(call(read_input(&wf, &after, &state))).await;
            let asked = match &step.step {
                Some(nexus_step::Step::Codec(work)) => work.payloads.len(),
                _ => panic!("no codec step"),
            };
            let answer = drive(&calls, step, Codec::Sealing).await.unwrap();
            let kept = answer["records"].as_array().unwrap().len();
            assert_eq!(asked, kept, "the codec decoded records the answer left out");
            assert_eq!(kept, 3.min(10 - read_count));
            read_count += kept;
            after = answer["next_token"].as_str().unwrap().to_string();
            state = answer["state"].clone();
        }
    }

    pub(crate) async fn an_error_wire_counts_in_the_answer_budget(store: Arc<dyn StreamStore>) {
        // A codec error with much detail, on every record of a long page, must not grow the
        // answer past the server's blob limit.
        let calls = service(store.clone(), NexusOptions::default());
        let wf = workflow();
        for _ in 0..20 {
            raw_entry(&store, &wf, unsealed("raw", 1).encode_to_vec()).await;
        }
        let detail = "d".repeat(2_000);
        let mut input = read_input(&wf, "", &Value::Null);
        input["max_records"] = json!(1000);
        let step = calls
            .read(call_with(input, 2_000, 0, Duration::from_secs(60)))
            .await;
        let answer = drive_answering(&calls, step, |_| {
            codec_result::Result::Error(detail.clone())
        })
        .await
        .unwrap();
        let records = answer["records"].as_array().unwrap();
        assert!(
            !records.is_empty() && records.len() < 20,
            "{}",
            records.len()
        );
        for wire in records {
            assert!(wire["error"].as_str().unwrap().len() <= 512);
        }
        let size = serde_json::to_vec(&answer).unwrap().len();
        assert!(size <= 2_000, "{size}");
    }

    pub(crate) async fn an_error_wire_can_end_a_cut_answer(store: Arc<dyn StreamStore>) {
        let calls = service(store.clone(), NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "a", 1, 1, &["a1"]).await;
        raw_entry(&store, &wf, unsealed("b", 1).encode_to_vec()).await;
        write(&calls, &wf, "a", 2, 1, &[&"x".repeat(4000)]).await;
        let first = drive(
            &calls,
            calls
                .read(call_with(
                    read_input(&wf, "", &Value::Null),
                    2000,
                    0,
                    Duration::from_secs(60),
                ))
                .await,
            Codec::Sealing,
        )
        .await
        .unwrap();
        let seen = values(&first);
        assert_eq!(seen.len(), 2, "{seen:?}");
        assert_eq!(seen[0], "a@1 a1");
        assert!(seen[1].starts_with("ERROR "), "{seen:?}");
        assert_eq!(first["next_token"], first["records"][1]["token"]);
        assert_eq!(first["done"], false);
        let next = read(
            &calls,
            read_input(&wf, first["next_token"].as_str().unwrap(), &first["state"]),
        )
        .await
        .unwrap();
        let seen = values(&next);
        assert_eq!(seen[0], "SUPERSEDED a 1->2");
        assert!(seen[1].starts_with("a@2 xxx"), "{seen:?}");
    }

    pub(crate) async fn a_stale_record_the_codec_refuses_keeps_its_mark(
        store: Arc<dyn StreamStore>,
    ) {
        // The reader skips a stale record it can't decode rather than failing on it.
        let calls = service(store.clone(), NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "chat", 1, 1, &["a"]).await;
        write(&calls, &wf, "chat", 2, 1, &["a2"]).await;
        raw_entry(&store, &wf, unsealed("chat", 1).encode_to_vec()).await;
        let answer = read(&calls, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        let wire = &answer["records"][3];
        assert!(wire["error"].is_string(), "{wire}");
        assert_eq!(wire["stale"], true, "{wire}");
    }

    pub(crate) async fn a_first_read_that_meets_an_unparsable_record_carries_its_chain(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store.clone(), NexusOptions::default());
        let wf = workflow();
        raw_entry(&store, &wf, b"\xff not a record".to_vec()).await;
        let first = read(&calls, read_input(&wf, "", &Value::Null))
            .await
            .unwrap();
        assert!(first["records"][0]["error"].is_string());
        assert!(first["state"].is_string(), "{first}");
        write(&calls, &wf, "p", 1, 1, &["after"]).await;
        let next = read(
            &calls,
            read_input(&wf, first["next_token"].as_str().unwrap(), &first["state"]),
        )
        .await
        .unwrap();
        assert_eq!(values(&next), ["p@1 after"]);
    }

    pub(crate) async fn a_wrong_codec_result_count_is_not_retried(store: Arc<dyn StreamStore>) {
        // A lang codec loop that drops a result fails the same way each time.
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "p", 1, 1, &["one"]).await;
        let appending = calls
            .append(call(json!({
                "stream": stream(&wf, TOPIC),
                "producer_id": "p",
                "attempt": 1,
                "sequence": 2,
                "payloads": [body("two")],
            })))
            .await;
        let reading = calls.read(call(read_input(&wf, "", &Value::Null))).await;
        for step in [appending, reading] {
            let Some(nexus_step::Step::Codec(work)) = step.step else {
                panic!("no codec step");
            };
            let answer = calls
                .resume(NexusResumeRequest {
                    continuation: work.continuation,
                    results: Vec::new(),
                })
                .await;
            let Some(nexus_step::Step::Failure(failure)) = answer.step else {
                panic!("a short codec answer went on");
            };
            assert_eq!(kind_of(&failure), ("INTERNAL", false, "PayloadCodecError"));
        }
    }

    pub(crate) async fn an_input_mistake_leaves_the_input_value_out(store: Arc<dyn StreamStore>) {
        // The failure message lands in the caller's History without the codec.
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let secret = "card 4111 1111 1111 1111";
        let mut wrong_kind = stream(&wf, TOPIC);
        wrong_kind["kind"] = json!(secret);
        let mut control = stream(&wf, &format!("{secret}\u{7}"));
        control["kind"] = json!("workflow");
        for input in [
            json!({"stream": stream(&wf, TOPIC), "producer_id": "p", "attempt": 1,
                   "sequence": 1, "payloads": [secret]}),
            json!({"stream": stream(&wf, TOPIC), "producer_id": "p", "attempt": 1,
                   "sequence": 1, "payloads": secret}),
            json!({"stream": wrong_kind, "producer_id": "p", "attempt": 1, "sequence": 1,
                   "payloads": [body("one")]}),
            json!({"stream": control, "producer_id": "p", "attempt": 1, "sequence": 1,
                   "payloads": [body("one")]}),
        ] {
            let failure = append(&calls, input).await.unwrap_err();
            assert!(!failure.message.contains("4111"), "{}", failure.message);
            let cause = failure.failure.unwrap().cause.unwrap();
            assert!(!cause.message.contains("4111"), "{}", cause.message);
        }
    }

    pub(crate) async fn a_deadline_past_any_clock_reads_without_one(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, "p", 1, 1, &["one"]).await;
        let mut far = call(read_input(&wf, "", &Value::Null));
        far.deadline = Some(prost_types::Timestamp {
            seconds: i64::MAX,
            nanos: 999_999_999,
        });
        let answer = drive(&calls, calls.read(far).await, Codec::Sealing)
            .await
            .unwrap();
        assert_eq!(values(&answer), ["p@1 one"]);
    }
}

/// A stored record by `producer` whose body the handler's codec can't decode, written past it.
fn unsealed(producer: &str, attempt: i64) -> StreamRecord {
    temporalio_streams::stored_append_record(
        TOPIC,
        producer,
        attempt,
        IDS.fetch_add(1, Ordering::Relaxed) as i64 + 1,
        &temporalio_streams::proto::AppendRecord {
            kind: StreamRecordKind::Data as i32,
            body: Some(Default::default()),
            content_hash: vec![1; 32],
            ..Default::default()
        },
    )
    .unwrap()
}

/// Runs `step` through lang's side with `codec` answering each body.
async fn drive_answering(
    calls: &temporalio_streams::nexus::NexusCalls,
    step: temporalio_streams::proto::NexusStep,
    codec: impl Fn(
        temporalio_common::protos::temporal::api::common::v1::Payload,
    ) -> codec_result::Result,
) -> Result<Value, NexusFailure> {
    let mut step = step;
    loop {
        match step.step.expect("a step") {
            nexus_step::Step::Output(output) => return Ok(serde_json::from_slice(&output).unwrap()),
            nexus_step::Step::Failure(failure) => return Err(failure),
            nexus_step::Step::Codec(work) => {
                let results = work
                    .payloads
                    .into_iter()
                    .map(|payload| CodecResult {
                        result: Some(codec(payload)),
                    })
                    .collect();
                step = calls
                    .resume(NexusResumeRequest {
                        continuation: work.continuation,
                        results,
                    })
                    .await;
            }
        }
    }
}

fn decode_state(state: &Value) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(state.as_str().unwrap())
        .unwrap()
}

/// The fields of a read state that grow with its producers, as its proto declares them.
#[derive(Clone, PartialEq, prost::Message)]
struct Producers {
    #[prost(map = "string, message", tag = "1")]
    producers: std::collections::HashMap<String, Tracked>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Tracked {
    #[prost(int64, tag = "1")]
    attempt: i64,
    #[prost(uint64, tag = "2")]
    seen: u64,
}

/// `real` with `count` more producers of `id_bytes`-byte ids, as a caller could forge it.
fn forged_state(real: &[u8], count: usize, id_bytes: usize) -> Value {
    use base64::Engine;
    let mut producers = Producers::default();
    for index in 0..count {
        let id = format!("{index:0id_bytes$}");
        producers.producers.insert(
            id,
            Tracked {
                attempt: 1,
                seen: index as u64 + 1,
            },
        );
    }
    let mut forged = real.to_vec();
    forged.extend(producers.encode_to_vec());
    Value::String(base64::engine::general_purpose::STANDARD.encode(forged))
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
            an_append_reads_back_with_its_body_decoded,
            rule_101_a_record_that_does_not_parse_crosses_as_an_error_at_its_cursor,
            rule_101_a_read_never_fails_on_a_record_it_cant_parse,
            a_body_the_codec_refuses_crosses_as_an_error_and_the_rest_reads,
            rule_99_a_cut_hands_back_the_state_at_its_cut,
            rule_99_a_state_carries_across_handler_workers,
            rule_99_a_state_naming_another_chain_is_refused,
            a_read_waits_no_longer_than_its_deadline_allows,
            a_retry_through_a_fresh_nonce_lands_once,
            a_body_the_codec_refuses_fails_its_append_and_writes_nothing,
            a_supersession_crosses_at_the_cursor_of_the_record_before,
            a_resumed_reader_takes_the_supersession_at_its_cursor,
            a_supersession_seen_across_two_calls_crosses_once,
            a_stale_record_crosses_marked,
            the_state_stays_within_its_budget,
            a_state_above_its_budget_is_refused_before_it_decodes,
            a_cut_read_asks_the_codec_only_for_what_fits,
            an_error_wire_counts_in_the_answer_budget,
            an_error_wire_can_end_a_cut_answer,
            a_stale_record_the_codec_refuses_keeps_its_mark,
            a_first_read_that_meets_an_unparsable_record_carries_its_chain,
            a_wrong_codec_result_count_is_not_retried,
            an_input_mistake_leaves_the_input_value_out,
            a_deadline_past_any_clock_reads_without_one,
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

#[tokio::test(start_paused = true)]
async fn a_call_that_waits_too_long_for_the_codec_is_dropped() {
    let calls = service(memory(), NexusOptions::default());
    let wf = workflow();
    let input = json!({
        "stream": stream(&wf, TOPIC),
        "producer_id": "p",
        "attempt": 1,
        "sequence": 1,
        "payloads": [body("one")],
    });
    let step = calls.append(call(input)).await;
    let Some(temporalio_streams::proto::nexus_step::Step::Codec(work)) = step.step else {
        panic!("no codec step");
    };
    tokio::time::advance(Duration::from_secs(61)).await;
    let late = calls
        .resume(temporalio_streams::proto::NexusResumeRequest {
            continuation: work.continuation,
            results: Vec::new(),
        })
        .await;
    let Some(temporalio_streams::proto::nexus_step::Step::Failure(failure)) = late.step else {
        panic!("a late resume went on");
    };
    assert_eq!(
        kind_of(&failure),
        ("UNAVAILABLE", true, "StreamStorageError")
    );
    let answer = read(&calls, read_input(&wf, "", &Value::Null))
        .await
        .unwrap();
    assert_eq!(values(&answer), Vec::<String>::new());
}

#[tokio::test]
async fn calls_waiting_for_the_codec_are_bounded() {
    let calls = service(
        memory(),
        NexusOptions {
            max_continuations: 1,
            ..NexusOptions::default()
        },
    );
    let wf = workflow();
    let input = |sequence: i64| {
        call(json!({
            "stream": stream(&wf, TOPIC),
            "producer_id": "p",
            "attempt": 1,
            "sequence": sequence,
            "payloads": [body("one")],
        }))
    };
    let waiting = calls.append(input(1)).await;
    let refused = drive(&calls, calls.append(input(2)).await, Codec::Sealing)
        .await
        .unwrap_err();
    assert_eq!(
        kind_of(&refused),
        ("UNAVAILABLE", true, "StreamStorageError")
    );
    drive(&calls, waiting, Codec::Sealing).await.unwrap();
    drive(&calls, calls.append(input(2)).await, Codec::Sealing)
        .await
        .unwrap();
}

#[tokio::test]
async fn the_store_holds_only_bodies_the_codec_sealed() {
    // Core holds plaintext in memory during a call only. What it stores went through the codec.
    let store = memory();
    let calls = service(store.clone(), NexusOptions::default());
    let wf = workflow();
    write(&calls, &wf, "p", 1, 1, &["secret"]).await;
    let target = temporalio_streams::ReadTarget {
        chain: ChainId {
            namespace: NAMESPACE.to_string(),
            workflow_id: wf.clone(),
            first_run_id: "run-1".to_string(),
            ..Default::default()
        },
        topic: TOPIC.to_string(),
        stream_hash: temporalio_streams::stream_hash(NAMESPACE, "workflow", &wf, TOPIC),
    };
    let page = temporalio_streams::read_page(&*store, &target, &Default::default())
        .await
        .unwrap();
    let Some(temporalio_streams::proto::read_record::Record::Stored(stored)) =
        &page.records[0].record
    else {
        panic!("no record");
    };
    let body: &StreamRecord = stored;
    assert!(body.body.as_ref().unwrap().data.starts_with(SEALED));
}

#[tokio::test(start_paused = true)]
async fn a_call_that_waits_too_long_for_the_codec_is_dropped_on_a_quiet_process() {
    // Plaintext lives in Core during a call only, also when no later call comes in.
    let calls = service(memory(), NexusOptions::default());
    let wf = workflow();
    let step = calls
        .append(call(json!({
            "stream": stream(&wf, TOPIC),
            "producer_id": "p",
            "attempt": 1,
            "sequence": 1,
            "payloads": [body("secret")],
        })))
        .await;
    assert!(matches!(step.step, Some(nexus_step::Step::Codec(_))));
    assert_eq!(calls.waiting_calls(), 1);
    tokio::time::advance(Duration::from_secs(61)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(calls.waiting_calls(), 0);
}

#[tokio::test]
async fn bytes_waiting_for_the_codec_are_bounded() {
    let calls = service(
        memory(),
        NexusOptions {
            max_continuation_bytes: 1_000,
            ..NexusOptions::default()
        },
    );
    let wf = workflow();
    let input = |sequence: i64| {
        call(json!({
            "stream": stream(&wf, TOPIC),
            "producer_id": "p",
            "attempt": 1,
            "sequence": sequence,
            "payloads": [body(&"x".repeat(600))],
        }))
    };
    // One call goes on alone whatever its size, so no call is refused for good.
    let waiting = calls.append(input(1)).await;
    let refused = drive(&calls, calls.append(input(2)).await, Codec::Sealing)
        .await
        .unwrap_err();
    assert_eq!(
        kind_of(&refused),
        ("UNAVAILABLE", true, "StreamStorageError")
    );
    drive(&calls, waiting, Codec::Sealing).await.unwrap();
    drive(&calls, calls.append(input(2)).await, Codec::Sealing)
        .await
        .unwrap();
}
