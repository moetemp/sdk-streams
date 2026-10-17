//! Driving the stream service's Nexus calls as a lang handler does, for tests.
//!
//! Each test file that uses it declares `mod nexus_support;`, so not every item is used by every
//! file.

#![allow(dead_code)]

use prost::Message;
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_common::protos::temporal::api::{
    common::v1::Payload, enums::v1::WorkflowExecutionStatus,
};
use temporalio_streams::{
    MemoryStore, OwnerClient, OwnerDescription, OwnerError, StreamStore, Streams, StreamsOptions,
    nexus::{NexusCalls, NexusOptions},
    proto::{
        CodecResult, NexusCall, NexusFailure, NexusLimits, NexusResumeRequest, NexusStep,
        codec_result, nexus_step,
    },
};

pub(crate) const NAMESPACE: &str = "ns";
/// What the fake codec puts before a body it encodes.
pub(crate) const SEALED: &[u8] = b"sealed:";
/// A body whose plaintext holds this fails the fake codec.
pub(crate) const POISON: &[u8] = b"poison";

/// Temporal with every Workflow's first run `run-1`, still running.
pub(crate) struct Running;

#[async_trait::async_trait]
impl OwnerClient for Running {
    async fn describe(
        &self,
        _: &str,
        _: temporalio_streams::OwnerId<'_>,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        Ok(OwnerDescription {
            run_id: if run_id.is_empty() { "run-1" } else { run_id }.to_string(),
            first_run_id: "run-1".to_string(),
            status: temporalio_streams::OwnerStatus::of_workflow(WorkflowExecutionStatus::Running),
        })
    }
}

/// The Nexus calls over `store`, as one handler Worker serves them.
pub(crate) fn service(store: Arc<dyn StreamStore>, options: NexusOptions) -> NexusCalls {
    let streams = Streams::new(store, Arc::new(Running), StreamsOptions::default());
    NexusCalls::new(Arc::new(streams), options)
}

pub(crate) fn memory() -> Arc<dyn StreamStore> {
    Arc::new(MemoryStore::new())
}

/// How the fake codec treats bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Codec {
    /// Seals each body, so the store never holds plaintext, and fails on [POISON].
    Sealing,
    /// Seals each body with a fresh prefix, as a codec with a random nonce does.
    Nonce(u8),
}

impl Codec {
    fn run(self, encode: bool, payload: Payload) -> CodecResult {
        let result = if encode {
            if payload.data.windows(POISON.len()).any(|w| w == POISON) {
                codec_result::Result::Error("the codec refused a poisoned body".to_string())
            } else {
                let mut data = SEALED.to_vec();
                if let Codec::Nonce(nonce) = self {
                    data.push(nonce);
                }
                data.extend(payload.encode_to_vec());
                codec_result::Result::Payload(Payload {
                    metadata: [("encoding".to_string(), b"binary/sealed".to_vec())].into(),
                    data,
                    ..Default::default()
                })
            }
        } else {
            match payload.data.strip_prefix(SEALED) {
                Some(sealed) => {
                    let inner = match self {
                        Codec::Nonce(_) => &sealed[1..],
                        Codec::Sealing => sealed,
                    };
                    match Payload::decode(inner) {
                        Ok(plain) if plain.data.windows(POISON.len()).any(|w| w == POISON) => {
                            codec_result::Result::Error("the codec refused a poisoned body".into())
                        }
                        Ok(plain) => codec_result::Result::Payload(plain),
                        Err(error) => codec_result::Result::Error(error.to_string()),
                    }
                }
                None => codec_result::Result::Error("the body is not sealed".to_string()),
            }
        };
        CodecResult {
            result: Some(result),
        }
    }
}

/// A call with `input` and the default limits, due in a minute.
pub(crate) fn call(input: Value) -> NexusCall {
    call_with(input, 0, 0, Duration::from_secs(60))
}

pub(crate) fn call_with(
    input: Value,
    answer: u64,
    record: u64,
    deadline_in: Duration,
) -> NexusCall {
    let deadline = SystemTime::now() + deadline_in;
    let since = deadline.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    NexusCall {
        input: serde_json::to_vec(&input).unwrap(),
        deadline: Some(prost_types::Timestamp {
            seconds: since.as_secs() as i64,
            nanos: since.subsec_nanos() as i32,
        }),
        limits: Some(NexusLimits {
            max_answer_bytes: answer,
            max_record_bytes: record,
        }),
        namespace: NAMESPACE.to_string(),
    }
}

/// Runs `step` through lang's side: the codec work it asks for, until an output or a failure.
pub(crate) async fn drive(
    calls: &NexusCalls,
    step: NexusStep,
    codec: Codec,
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
                    .map(|payload| codec.run(work.encode, payload))
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

pub(crate) async fn append(calls: &NexusCalls, input: Value) -> Result<Value, NexusFailure> {
    drive(calls, calls.append(call(input)).await, Codec::Sealing).await
}

pub(crate) async fn read(calls: &NexusCalls, input: Value) -> Result<Value, NexusFailure> {
    drive(calls, calls.read(call(input)).await, Codec::Sealing).await
}

pub(crate) async fn read_range(calls: &NexusCalls, input: Value) -> Result<Value, NexusFailure> {
    drive(calls, calls.read_range(call(input)).await, Codec::Sealing).await
}

/// The `read_range` input that replays `answer`, the answer of a read that sent `state`.
pub(crate) fn replay_input(stream: Value, answer: &Value, state: Value) -> Value {
    let replay = &answer["replay"];
    serde_json::json!({
        "stream": stream,
        "chain_first_run_id": replay["chain_first_run_id"],
        "after_token": replay["after_token"],
        "to_token": answer["next_token"],
        "stored_records": replay["stored_records"],
        "state": state,
        "records_digest": replay["records_digest"],
    })
}

/// A stream reference to `topic` of standalone Activity `activity_id`, on its newest run.
pub(crate) fn activity_stream(activity_id: &str, topic: &str) -> Value {
    serde_json::json!({
        "kind": "activity",
        "workflow_id": "",
        "activity_id": activity_id,
        "topic": topic,
    })
}

/// A stream reference to `topic` of Activity `activity_id` that run `run_id` of Workflow
/// `workflow_id` scheduled.
pub(crate) fn workflow_activity_stream(
    workflow_id: &str,
    run_id: &str,
    activity_id: &str,
    topic: &str,
) -> Value {
    serde_json::json!({
        "kind": "workflow_activity",
        "workflow_id": workflow_id,
        "activity_id": activity_id,
        "chain_run_id": run_id,
        "topic": topic,
    })
}

/// A stream reference to `topic` of Workflow `workflow_id`.
pub(crate) fn stream(workflow_id: &str, topic: &str) -> Value {
    serde_json::json!({"kind": "workflow", "workflow_id": workflow_id, "topic": topic})
}

/// A body as the caller's payload converter leaves it, base64 for the transfer form.
pub(crate) fn body(text: &str) -> String {
    use base64::Engine;
    let payload = Payload {
        metadata: [("encoding".to_string(), b"json/plain".to_vec())].into(),
        data: serde_json::to_vec(text).unwrap(),
        ..Default::default()
    };
    base64::engine::general_purpose::STANDARD.encode(payload.encode_to_vec())
}

/// The value of each record of a read answer, or a marker for one that is no plain record.
pub(crate) fn values(answer: &Value) -> Vec<String> {
    use base64::Engine;
    answer["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|wire| {
            if let Some(superseded) = wire.get("superseded") {
                return format!(
                    "SUPERSEDED {} {}->{}",
                    superseded["producer_id"].as_str().unwrap(),
                    superseded["previous_attempt"],
                    superseded["attempt"]
                );
            }
            if let Some(error) = wire.get("error") {
                return format!("ERROR {}", error.as_str().unwrap());
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(wire["record"].as_str().unwrap())
                .unwrap();
            let record = temporalio_streams::proto::StreamRecord::decode(bytes.as_slice()).unwrap();
            let value = match record.body {
                Some(body) => serde_json::from_slice::<String>(&body.data).unwrap(),
                None => "FINISH".to_string(),
            };
            let stale = if wire.get("stale").is_some() {
                " stale"
            } else {
                ""
            };
            format!("{}@{} {value}{stale}", record.producer_id, record.attempt)
        })
        .collect()
}
