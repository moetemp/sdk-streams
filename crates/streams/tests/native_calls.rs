//! The native Store's calls as the server sees them, on a fake server that records each call.

use parking_lot::Mutex;
use prost::Message;
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporalio_client::{
    Connection, ConnectionOptions, Url,
    callback_based::{CallbackBasedGrpcService, GrpcRequest, GrpcSuccessResponse},
    tonic::Status,
};
use temporalio_streams::{
    NativeStreamExtension, StreamExtension,
    ids::client_producer_id,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, Producer, ProducerEpoch, ProducerKind,
        StreamErrorDetails, StreamRef,
    },
};

type Answer = dyn Fn(&GrpcRequest) -> Result<Vec<u8>, Status> + Send + Sync;

/// A native runtime on a fake server that answers each call with `answer` and records it.
async fn runtime(
    answer: impl Fn(&GrpcRequest) -> Result<Vec<u8>, Status> + Send + Sync + 'static,
) -> (Arc<StreamExtension>, Arc<Mutex<Vec<GrpcRequest>>>) {
    let seen: Arc<Mutex<Vec<GrpcRequest>>> = Default::default();
    let recorded = seen.clone();
    let answer: Arc<Answer> = Arc::new(answer);
    let service = CallbackBasedGrpcService {
        callback: Arc::new(move |request| {
            let (recorded, answer) = (recorded.clone(), answer.clone());
            Box::pin(async move {
                if request.rpc == "GetSystemInfo" {
                    return Ok(GrpcSuccessResponse {
                        headers: Default::default(),
                        proto: vec![],
                    });
                }
                let answered = answer(&request);
                recorded.lock().push(request);
                answered.map(|proto| GrpcSuccessResponse {
                    headers: Default::default(),
                    proto,
                })
            })
        }),
    };
    let options = ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap())
        .service_override(service)
        .dns_load_balancing(None)
        .headers(HashMap::new())
        .build();
    let connection = Connection::connect(options).await.unwrap();
    (
        NativeStreamExtension::create("native", connection, None),
        seen,
    )
}

fn client() -> Producer {
    Producer {
        kind: ProducerKind::Client as i32,
        id: client_producer_id("me", "p"),
        epoch: Some(ProducerEpoch {
            attempt: 1,
            ..Default::default()
        }),
        run_id: String::new(),
    }
}

fn stream() -> Option<StreamRef> {
    Some(StreamRef {
        id: "s".to_string(),
        ..Default::default()
    })
}

/// A create, an append and a close each send the Redis Store's 5 s deadline, not the client's
/// 30 s default, so a server that doesn't answer can't hold the call long past a Local
/// Activity's timeout.
#[tokio::test]
async fn every_write_call_sends_the_store_deadline() {
    let (ext, seen) = runtime(|request| {
        Ok(match request.rpc.as_str() {
            "CreateStream" => CreateStreamResponse {
                created: true,
                ..Default::default()
            }
            .encode_to_vec(),
            "AppendRecords" => AppendRecordsResponse::default().encode_to_vec(),
            _ => CloseStreamResponse::default().encode_to_vec(),
        })
    })
    .await;
    let calls: [(&str, Vec<u8>); 3] = [
        (
            "CreateStream",
            CreateStreamRequest {
                namespace: "ns".to_string(),
                stream_id: "s".to_string(),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        (
            "AppendRecords",
            AppendRecordsRequest {
                namespace: "ns".to_string(),
                stream: stream(),
                producer: Some(client()),
                sequence: 1,
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        (
            "CloseStream",
            CloseStreamRequest {
                namespace: "ns".to_string(),
                stream: stream(),
                producer: Some(client()),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
    ];
    for (method, request) in calls {
        ext.call(method, &request).await.unwrap();
    }
    let seen = seen.lock();
    let methods: Vec<&str> = seen.iter().map(|call| call.rpc.as_str()).collect();
    assert_eq!(methods, ["CreateStream", "AppendRecords", "CloseStream"]);
    for call in seen.iter() {
        let timeout = call.headers["grpc-timeout"].to_str().unwrap();
        assert!(
            deadline(timeout) <= Duration::from_secs(5),
            "{} {timeout}",
            call.rpc
        );
    }
}

/// The time of a `grpc-timeout` header, such as `5000000u`.
fn deadline(header: &str) -> Duration {
    let (value, unit) = header.split_at(header.len() - 1);
    let value: u64 = value.parse().unwrap();
    match unit {
        "H" => Duration::from_secs(value * 3600),
        "M" => Duration::from_secs(value * 60),
        "S" => Duration::from_secs(value),
        "m" => Duration::from_millis(value),
        "u" => Duration::from_micros(value),
        "n" => Duration::from_nanos(value),
        other => panic!("unknown unit {other}"),
    }
}

/// A create with another Retention fails with the server's conflict, which names the stored
/// Retention, not the one the create asked for.
#[tokio::test]
async fn a_create_conflict_names_the_stored_retention() {
    let (ext, _) = runtime(|_| {
        Err(Status::already_exists(
            "stream_already_exists: The Stream exists with the Retention 1h0m0s",
        ))
    })
    .await;
    let request = CreateStreamRequest {
        namespace: "ns".to_string(),
        stream_id: "s".to_string(),
        retention: Some(prost_types::Duration {
            seconds: 7200,
            nanos: 0,
        }),
        ..Default::default()
    };
    let error = ext
        .call("CreateStream", &request.encode_to_vec())
        .await
        .unwrap_err();
    let details = StreamErrorDetails::decode(error.details().as_slice()).unwrap();
    assert_eq!(details.reason, "stream_already_exists");
    assert_eq!(error.message, "The Stream exists with the Retention 1h0m0s");
}
