//! The stream service's `follow` and `follow_range` operations as a caller sees them.

mod nexus_support;

use nexus_support::*;
use prost::Message;
use serde_json::{Value, json};
use std::sync::Arc;
use temporalio_common::protos::temporal::api::{
    enums::v1::StreamOwnerKind as RefKind, stream::v1::StreamReference,
};
use temporalio_streams::{
    StreamStore, Streams, StreamsOptions,
    nexus::NexusOptions,
    proto::{
        AppendRecord, AppendRequest, NamedProducer, NexusResumeRequest, StreamAddress, StreamLink,
        StreamOwnerKind, StreamRecord, StreamRecordKind, append_request::Producer, nexus_step,
        stream_link,
    },
};

const TOPIC: &str = "out";

fn agent() -> String {
    format!(
        "follow-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// Writes link records straight to the store, as the agent's Workflow does in its own stream.
async fn link(store: Arc<dyn StreamStore>, agent: &str, sequence: i64, link: StreamLink) {
    let streams = Streams::new(store, Arc::new(Running), StreamsOptions::default());
    streams
        .append(AppendRequest {
            stream: Some(StreamAddress {
                namespace: NAMESPACE.to_string(),
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: agent.to_string(),
                topic: TOPIC.to_string(),
                ..Default::default()
            }),
            producer: Some(Producer::Named(NamedProducer {
                producer_id: "agent".to_string(),
                attempt: 1,
            })),
            sequence,
            digest: vec![sequence as u8; 32],
            records: vec![AppendRecord {
                kind: StreamRecordKind::Link as i32,
                link: Some(link),
                ..Default::default()
            }],
        })
        .await
        .unwrap();
}

fn open_tool(agent: &str, namespace: &str) -> StreamLink {
    StreamLink {
        event: stream_link::Event::Open as i32,
        stream: Some(StreamReference {
            owner_kind: RefKind::WorkflowActivity as i32,
            workflow_id: agent.to_string(),
            run_id: "run-1".to_string(),
            topic: TOPIC.to_string(),
            activity_id: "tool-42".to_string(),
        }),
        name: "tool-42".to_string(),
        namespace: namespace.to_string(),
        ..Default::default()
    }
}

fn tool(agent: &str) -> Value {
    workflow_activity_stream(agent, "run-1", "tool-42", TOPIC)
}

async fn write(store: Arc<dyn StreamStore>, stream: Value, producer: &str, texts: &[&str]) {
    let calls = service(store, NexusOptions::default());
    let input = json!({
        "stream": stream,
        "producer_id": producer,
        "attempt": 1,
        "sequence": 1,
        "payloads": texts.iter().map(|text| body(text)).collect::<Vec<_>>(),
    });
    append(&calls, input).await.unwrap();
}

/// Each item as text: a record's body, a link's event and name, or a link state's kind.
fn shape(answer: &Value) -> Vec<String> {
    use base64::Engine;
    answer["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            let source = item["source"].as_u64().unwrap();
            if let Some(state) = item.get("link_state") {
                return format!("{source}:{}", state["kind"].as_str().unwrap());
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(item["record"]["record"].as_str().unwrap())
                .unwrap();
            let record = StreamRecord::decode(bytes.as_slice()).unwrap();
            match record.link {
                Some(link) => format!("{source}:link {}", link.name),
                None => {
                    let text: String = serde_json::from_slice(&record.body.unwrap().data).unwrap();
                    format!("{source}:{text}")
                }
            }
        })
        .collect()
}

/// Follows to the first answer, counting the codec steps and the owner kind each one names.
async fn follow_once(
    calls: &temporalio_streams::nexus::NexusCalls,
    input: Value,
) -> (Value, Vec<i32>) {
    let mut step = calls.follow(call(input)).await;
    let mut kinds = vec![];
    loop {
        match step.step.clone().expect("a step") {
            nexus_step::Step::Codec(work) => {
                kinds.push(work.owner_kind);
                let results = drive_codec(work.payloads);
                step = calls
                    .resume(NexusResumeRequest {
                        continuation: work.continuation,
                        results,
                    })
                    .await;
            }
            _ => return (drive(calls, step, Codec::Sealing).await.unwrap(), kinds),
        }
    }
}

/// What the sealing codec answers when it decodes `payloads`.
fn drive_codec(
    payloads: Vec<temporalio_common::protos::temporal::api::common::v1::Payload>,
) -> Vec<temporalio_streams::proto::CodecResult> {
    use temporalio_streams::proto::{CodecResult, codec_result};
    payloads
        .into_iter()
        .map(|payload| {
            let plain = temporalio_common::protos::temporal::api::common::v1::Payload::decode(
                payload.data.strip_prefix(SEALED).unwrap(),
            )
            .unwrap();
            CodecResult {
                result: Some(codec_result::Result::Payload(plain)),
            }
        })
        .collect()
}

#[tokio::test]
async fn a_follow_answers_the_agent_and_its_tool_with_each_owners_codec() {
    let store = memory();
    let id = agent();
    write(store.clone(), stream(&id, TOPIC), "p", &["d1"]).await;
    link(store.clone(), &id, 1, open_tool(&id, "")).await;
    write(store.clone(), tool(&id), "t", &["t1", "t2"]).await;
    let calls = service(store, NexusOptions::default());
    let (answer, kinds) = follow_once(&calls, json!({"stream": stream(&id, TOPIC)})).await;
    assert_eq!(
        shape(&answer),
        ["0:d1", "0:link tool-42", "1:opened", "1:t1", "1:t2"]
    );
    // One codec step per owner, the agent's first.
    assert_eq!(
        kinds,
        [
            StreamOwnerKind::Workflow as i32,
            StreamOwnerKind::WorkflowActivity as i32
        ]
    );
    let sources = answer["sources"].as_array().unwrap();
    assert_eq!(sources.len(), 2);
    assert_eq!(sources[1]["stream"], tool(&id));
    assert_eq!(sources[1]["name"], "tool-42");
    assert_eq!(answer["done"], false);
    assert!(answer.get("replay").is_none());
    // The state resumes every stream.
    let (next, _) = follow_once(
        &calls,
        json!({"stream": stream(&id, TOPIC), "state": answer["state"]}),
    )
    .await;
    assert!(shape(&next).is_empty(), "{:?}", shape(&next));
}

#[tokio::test]
async fn a_positions_follow_replays_from_its_ranges() {
    let store = memory();
    let id = agent();
    write(store.clone(), stream(&id, TOPIC), "p", &["d1"]).await;
    link(store.clone(), &id, 1, open_tool(&id, "")).await;
    write(store.clone(), tool(&id), "t", &["t1"]).await;
    let calls = service(store, NexusOptions::default());
    let (answer, _) = follow_once(
        &calls,
        json!({"stream": stream(&id, TOPIC), "floor_holder": "ns/reader/1"}),
    )
    .await;
    let replay = &answer["replay"];
    assert!(!replay["ranges"].as_array().unwrap().is_empty());
    let replayed = drive(
        &calls,
        calls
            .follow_range(call(json!({
                "stream": stream(&id, TOPIC),
                "ranges": replay["ranges"],
                "follow_digest": replay["follow_digest"],
                "floor_holder": "ns/reader/1",
            })))
            .await,
        Codec::Sealing,
    )
    .await
    .unwrap();
    assert_eq!(shape(&replayed), shape(&answer));
    let mismatch = drive(
        &calls,
        calls
            .follow_range(call(json!({
                "stream": stream(&id, TOPIC),
                "ranges": replay["ranges"],
                "follow_digest": base64_of(&[0; 32]),
            })))
            .await,
        Codec::Sealing,
    )
    .await
    .unwrap_err();
    assert!(
        mismatch.message.starts_with("StreamReplayMismatchError"),
        "{}",
        mismatch.message
    );
}

#[tokio::test]
async fn a_link_to_another_namespace_is_skipped() {
    let store = memory();
    let id = agent();
    link(store.clone(), &id, 1, open_tool(&id, "other")).await;
    let calls = service(store, NexusOptions::default());
    let (answer, _) = follow_once(&calls, json!({"stream": stream(&id, TOPIC)})).await;
    assert_eq!(shape(&answer), ["0:link tool-42", "0:skipped"]);
    assert_eq!(answer["records"][1]["link_state"]["reason"], "endpoint");
}

#[tokio::test]
async fn follow_argument_mistakes_are_bad_requests() {
    let calls = service(memory(), NexusOptions::default());
    let id = agent();
    for input in [
        json!({"stream": stream(&id, TOPIC), "max_depth": 0}),
        json!({"stream": stream(&id, TOPIC), "max_open": 33}),
        json!({"stream": stream(&id, TOPIC), "max_records": 1001}),
        json!({"stream": stream(&id, TOPIC), "floor_holder": "h".repeat(481)}),
        json!({"stream": stream(&id, TOPIC), "wait": 5}),
        json!({"stream": stream(&id, TOPIC), "state": "!!"}),
        json!({"stream": stream(&id, TOPIC), "wake_token": "x"}),
    ] {
        let refused = drive(
            &calls,
            calls.follow(call(input.clone())).await,
            Codec::Sealing,
        )
        .await
        .unwrap_err();
        assert_eq!(refused.handler_error_type, "BAD_REQUEST", "{input}");
    }
    let refused = drive(
        &calls,
        calls
            .follow_range(call(json!({
                "stream": stream(&id, TOPIC),
                "ranges": [base64_of(b"not a range")],
                "follow_digest": "",
            })))
            .await,
        Codec::Sealing,
    )
    .await
    .unwrap_err();
    assert_eq!(refused.handler_error_type, "BAD_REQUEST");
}

fn base64_of(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[tokio::test]
async fn an_idle_root_with_an_open_tool_waits_for_the_tools_record_in_the_call() {
    let store = memory();
    let id = agent();
    link(store.clone(), &id, 1, open_tool(&id, "")).await;
    let calls = service(store.clone(), NexusOptions::default());
    let (first, _) = follow_once(&calls, json!({"stream": stream(&id, TOPIC)})).await;
    assert_eq!(shape(&first), ["0:link tool-42", "1:opened"]);
    // The caller's operation hears only the root, so the call itself waits on the tool.
    let token = temporalio_streams::nexus::OperationToken {
        stream: StreamAddress {
            namespace: NAMESPACE.to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: id.clone(),
            topic: TOPIC.to_string(),
            ..Default::default()
        },
        attach_request_id: "attach-1".to_string(),
        chain_first_run_id: "run-1".to_string(),
    }
    .encode();
    let input = json!({
        "stream": stream(&id, TOPIC),
        "state": first["state"],
        "wake_token": token,
        "wait_ms": 5000,
    });
    let late = async {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        write(store.clone(), tool(&id), "t", &["t1"]).await;
    };
    let started = std::time::Instant::now();
    let followed = async {
        let step = calls
            .follow(call_with(input, 0, 0, std::time::Duration::from_secs(8)))
            .await;
        drive(&calls, step, Codec::Sealing).await.unwrap()
    };
    let (answer, ()) = tokio::join!(followed, late);
    assert_eq!(shape(&answer), ["1:t1"]);
    assert!(started.elapsed() < std::time::Duration::from_secs(3));
}

/// The caller's operation, as the server takes its waits.
#[derive(Default)]
struct Waits(std::sync::Mutex<Vec<i64>>);

#[async_trait::async_trait]
impl temporalio_streams::NotifierClient for Waits {
    async fn notify(
        &self,
        _: temporalio_streams::StreamNotification,
    ) -> Result<(), temporalio_streams::NotifyError> {
        Ok(())
    }

    async fn wait_callback(
        &self,
        _: temporalio_streams::proto::ChainId,
        _: String,
        _: String,
        after_counter: i64,
    ) -> Result<(), temporalio_streams::NotifyError> {
        self.0.lock().unwrap().push(after_counter);
        Ok(())
    }
}

#[tokio::test]
async fn a_linked_follow_that_finds_nothing_still_waits_on_the_root() {
    let store = memory();
    let id = agent();
    link(store.clone(), &id, 1, open_tool(&id, "")).await;
    let waits = Arc::new(Waits::default());
    let calls = service(store.clone(), NexusOptions::default()).with_callers(waits.clone());
    let (first, _) = follow_once(&calls, json!({"stream": stream(&id, TOPIC)})).await;
    let token = temporalio_streams::nexus::OperationToken {
        stream: StreamAddress {
            namespace: NAMESPACE.to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: id.clone(),
            topic: TOPIC.to_string(),
            ..Default::default()
        },
        attach_request_id: "attach-1".to_string(),
        chain_first_run_id: "run-1".to_string(),
    }
    .encode();
    for wait_ms in [0, 100] {
        let input = json!({
            "stream": stream(&id, TOPIC),
            "state": first["state"],
            "wake_token": token,
            "wait_ms": wait_ms,
        });
        let started = std::time::Instant::now();
        let step = calls
            .follow(call_with(input, 0, 0, std::time::Duration::from_secs(8)))
            .await;
        let answer = drive(&calls, step, Codec::Sealing).await.unwrap();
        assert!(answer["records"].as_array().unwrap().is_empty());
        // The call waits no longer than the caller asked, not half its deadline.
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "{wait_ms}"
        );
    }
    assert_eq!(
        waits.0.lock().unwrap().len(),
        2,
        "one root wait per empty answer"
    );
}
