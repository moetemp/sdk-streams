//! Core's Nexus calls against the contract they serve, `nexus/temporal_streams.nexusrpc.yaml`.
//!
//! Every SDK generates its models from that file, so what Core accepts and answers must be what it
//! declares: each input member is accepted, an unknown or missing one is refused, and each output
//! member is declared, with every required one present.

mod nexus_support;

use nexus_support::*;
use serde_json::{Map, Value, json};
use temporalio_streams::nexus::NexusOptions;

fn contract() -> Value {
    serde_yaml::from_str(include_str!("../nexus/temporal_streams.nexusrpc.yaml")).unwrap()
}

fn model<'a>(contract: &'a Value, name: &str) -> &'a Value {
    &contract["$defs"][name]
}

fn properties(model: &Value) -> Vec<String> {
    model["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect()
}

fn required(model: &Value) -> Vec<String> {
    model["required"]
        .as_array()
        .map(|names| {
            names
                .iter()
                .map(|name| name.as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// A value for every member the contract declares on an input, chosen so the call can go on.
fn full_input(contract: &Value, name: &str, workflow_id: &str) -> Value {
    let mut out = Map::new();
    for member in properties(model(contract, name)) {
        let value = match (name, member.as_str()) {
            (_, "stream") => {
                let mut stream = stream(workflow_id, "out");
                stream["chain_run_id"] = Value::Null;
                stream["activity_id"] = Value::Null;
                stream
            }
            ("AppendInput", "producer_id") => json!("p"),
            ("AppendInput", "attempt" | "sequence") => json!(1),
            ("AppendInput", "payloads") => json!([body("one")]),
            ("AppendInput", "finish") => json!(false),
            ("ReadInput", "after_token") => json!(""),
            ("ReadInput", "max_records") => json!(10),
            ("ReadInput", "wait_ms") => json!(0),
            ("ReadInput", "latest_only") => json!(false),
            ("ReadInput", "state") => Value::Null,
            ("ReadInput", "floor_holder") => json!("ns/run-1/reader"),
            ("ReadInput", "wake_token") => Value::Null,
            ("FollowInput", "after_token") => json!(""),
            ("FollowInput", "state" | "wake_token") => Value::Null,
            ("FollowInput", "max_records") => json!(10),
            ("FollowInput", "wait_ms") => json!(0),
            ("FollowInput", "max_depth") => json!(4),
            ("FollowInput", "max_open") => json!(16),
            ("FollowInput", "floor_holder") => json!("ns/run-1/reader"),
            (model, member) => panic!("the contract added {model}.{member}; teach this test"),
        };
        out.insert(member, value);
    }
    Value::Object(out)
}

/// Every member of `value` is declared on `model`, and every required one is there.
fn check_output(contract: &Value, model_name: &str, value: &Value) {
    let model = model(contract, model_name);
    let declared = properties(model);
    let members = value.as_object().unwrap();
    for member in members.keys() {
        assert!(
            declared.contains(member),
            "{model_name} answers undeclared {member}"
        );
    }
    for member in required(model) {
        assert!(
            members.contains_key(&member),
            "{model_name} lacks required {member}"
        );
    }
}

fn is_contract_mistake(failure: &temporalio_streams::proto::NexusFailure) -> bool {
    failure.message.contains("doesn't match the contract")
}

#[test]
fn the_contract_names_the_operations_core_serves() {
    let contract = contract();
    let operations = &contract["services"]["TemporalStreams"]["operations"];
    assert_eq!(operations["append"]["input"]["$ref"], "#/$defs/AppendInput");
    assert_eq!(
        operations["append"]["output"]["$ref"],
        "#/$defs/AppendOutput"
    );
    assert_eq!(operations["read"]["input"]["$ref"], "#/$defs/ReadInput");
    assert_eq!(operations["read"]["output"]["$ref"], "#/$defs/ReadOutput");
    assert_eq!(operations["readRange"]["fqn"], "read_range");
    assert_eq!(
        operations["readRange"]["input"]["$ref"],
        "#/$defs/ReadRangeInput"
    );
    assert_eq!(
        operations["readRange"]["output"]["$ref"],
        "#/$defs/ReadOutput"
    );
    // nexgen takes an operation key only as a lowercase letter and then letters or digits. The
    // wire name comes from `fqn`.
    for key in operations.as_object().unwrap().keys() {
        let mut chars = key.chars();
        assert!(
            chars.next().is_some_and(|c| c.is_ascii_lowercase())
                && key.len() > 1
                && chars.all(|c| c.is_ascii_alphanumeric()),
            "nexgen refuses operation key {key}"
        );
    }
    for input in ["StreamRef", "AppendInput", "ReadInput", "ReadRangeInput"] {
        assert_eq!(
            model(&contract, input)["additionalProperties"],
            false,
            "{input} must refuse unknown members, as Core does"
        );
    }
}

#[test]
fn a_read_carries_its_state_through_the_generated_long_poll() {
    // The generated caller sends each answer's state back on its next call, so the member it
    // names must be the optional base64 `state` Core reads and answers.
    let contract = contract();
    let long_poll =
        &contract["services"]["TemporalStreams"]["operations"]["read"]["x-nexus-long-poll"];
    assert_eq!(long_poll["state-field"], "state");
    for model_name in ["ReadInput", "ReadOutput"] {
        let model = model(&contract, model_name);
        let state = &model["properties"]["state"];
        assert_eq!(state["type"], "string", "{model_name}");
        assert_eq!(state["contentEncoding"], "base64", "{model_name}");
        assert!(
            !required(model).contains(&"state".to_string()),
            "{model_name}"
        );
    }
}

#[test]
fn a_replay_range_names_what_read_range_takes() {
    // A replay passes the answer's range back as it got it, so every member of `ReplayRange`
    // is a required member of `ReadRangeInput` of the same type, and the answer names it.
    let contract = contract();
    assert_eq!(
        model(&contract, "ReadOutput")["properties"]["replay"]["$ref"],
        "#/$defs/ReplayRange"
    );
    let range = model(&contract, "ReplayRange");
    let input = model(&contract, "ReadRangeInput");
    let mut members = required(range);
    members.sort();
    assert_eq!(
        members,
        [
            "after_token",
            "chain_first_run_id",
            "records_digest",
            "stored_records"
        ]
    );
    for member in members {
        assert!(required(input).contains(&member), "{member}");
        assert_eq!(
            range["properties"][&member]["type"], input["properties"][&member]["type"],
            "{member}"
        );
    }
    for member in ["stream", "to_token"] {
        assert!(required(input).contains(&member.to_string()), "{member}");
    }
    for member in ["state", "floor_holder"] {
        assert!(!required(input).contains(&member.to_string()), "{member}");
    }
    assert_eq!(
        input["properties"]["records_digest"]["contentEncoding"],
        "base64"
    );
    let holder = &model(&contract, "ReadInput")["properties"]["floor_holder"];
    assert_eq!(holder["type"], "string");
    assert!(!required(model(&contract, "ReadInput")).contains(&"floor_holder".to_string()));
}

#[test]
fn a_stream_ref_names_an_activity_owner_by_an_optional_member() {
    // Every caller built before the Activity kind still parses a Workflow ref, since the member
    // is optional and `workflow_id` stays required.
    let contract = contract();
    let stream_ref = model(&contract, "StreamRef");
    assert_eq!(
        stream_ref["properties"]["activity_id"]["oneOf"],
        json!([{ "type": "string" }, { "type": "null" }])
    );
    assert_eq!(
        stream_ref["properties"]["activity_id"]["oneOf"],
        stream_ref["properties"]["chain_run_id"]["oneOf"]
    );
    let mut required = required(stream_ref);
    required.sort();
    assert_eq!(required, ["kind", "topic", "workflow_id"]);
}

#[tokio::test]
async fn an_activity_ref_matches_the_contract() {
    let calls = service(memory(), NexusOptions::default());
    let input = json!({
        "stream": {
            "kind": "activity",
            "workflow_id": "",
            "activity_id": "tool-1",
            "chain_run_id": "act-run-1",
            "topic": "out",
        },
    });
    let step = calls.read(call(input)).await;
    if let Err(failure) = drive(&calls, step, Codec::Sealing).await {
        assert!(!is_contract_mistake(&failure), "{}", failure.message);
    }
}

#[tokio::test]
async fn a_workflow_activity_ref_matches_the_contract() {
    let calls = service(memory(), NexusOptions::default());
    let input = json!({
        "stream": {
            "kind": "workflow_activity",
            "workflow_id": "agent",
            "activity_id": "tool-42",
            "chain_run_id": "run-1",
            "topic": "out",
        },
    });
    let step = calls.read(call(input)).await;
    if let Err(failure) = drive(&calls, step, Codec::Sealing).await {
        assert!(!is_contract_mistake(&failure), "{}", failure.message);
    }
    let contract = contract();
    let kind = model(&contract, "StreamRef")["properties"]["kind"]["description"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(kind.contains("\"workflow_activity\""), "{kind}");
}

#[test]
fn the_operation_token_model_is_closed_and_versioned() {
    // The generated token helpers refuse a token model that isn't closed or that lacks a
    // required integer version constant.
    let contract = contract();
    let token = model(&contract, "StreamOperationToken");
    assert_eq!(token["additionalProperties"], false);
    assert_eq!(token["x-nexus-operation-token"]["version-field"], "v");
    assert_eq!(token["properties"]["v"]["type"], "integer");
    assert_eq!(token["properties"]["v"]["const"], 2);
    assert_eq!(token["properties"]["ref"]["$ref"], "#/$defs/StreamRef");
    let mut required = required(token);
    required.sort();
    assert_eq!(required, ["attach", "chain", "ref", "v"]);
}

#[tokio::test]
async fn every_declared_input_member_is_accepted_and_nothing_else() {
    let contract = contract();
    let calls = service(memory(), NexusOptions::default());
    for name in ["AppendInput", "ReadInput", "FollowInput"] {
        let full = full_input(&contract, name, &format!("contract-{name}"));
        let run = |input: Value| {
            let calls = &calls;
            async move {
                let step = match name {
                    "AppendInput" => calls.append(call(input)).await,
                    "ReadInput" => calls.read(call(input)).await,
                    _ => calls.follow(call(input)).await,
                };
                drive(calls, step, Codec::Sealing).await
            }
        };
        run(full.clone()).await.unwrap();
        let mut unknown = full.clone();
        unknown["surprise"] = json!(1);
        let failure = run(unknown).await.unwrap_err();
        assert!(is_contract_mistake(&failure), "{name}: {}", failure.message);
        assert_eq!(failure.handler_error_type, "BAD_REQUEST");
        for member in required(model(&contract, name)) {
            let mut missing = full.clone();
            missing.as_object_mut().unwrap().remove(&member);
            let failure = run(missing).await.unwrap_err();
            assert!(
                is_contract_mistake(&failure),
                "{name}.{member}: {}",
                failure.message
            );
        }
        let mut stream_unknown = full.clone();
        stream_unknown["stream"]["surprise"] = json!(1);
        let failure = run(stream_unknown).await.unwrap_err();
        assert!(
            is_contract_mistake(&failure),
            "{name}.stream: {}",
            failure.message
        );
        for member in required(model(&contract, "StreamRef")) {
            let mut missing = full.clone();
            missing["stream"].as_object_mut().unwrap().remove(&member);
            let failure = run(missing).await.unwrap_err();
            assert!(
                is_contract_mistake(&failure),
                "StreamRef.{member}: {}",
                failure.message
            );
        }
    }
}

#[tokio::test]
async fn every_read_range_member_is_accepted_and_nothing_else() {
    let contract = contract();
    let calls = service(memory(), NexusOptions::default());
    let wf = "contract-read-range";
    append(
        &calls,
        json!({"stream": stream(wf, "out"), "producer_id": "p", "attempt": 1, "sequence": 1,
               "payloads": [body("a")]}),
    )
    .await
    .unwrap();
    let first = read(
        &calls,
        json!({"stream": stream(wf, "out"), "floor_holder": "ns/run-1/reader"}),
    )
    .await
    .unwrap();
    append(
        &calls,
        json!({"stream": stream(wf, "out"), "producer_id": "p", "attempt": 1, "sequence": 2,
               "payloads": [body("b")]}),
    )
    .await
    .unwrap();
    let second = read(
        &calls,
        json!({"stream": stream(wf, "out"), "after_token": first["next_token"],
               "state": first["state"], "floor_holder": "ns/run-1/reader"}),
    )
    .await
    .unwrap();
    let mut full = replay_input(stream(wf, "out"), &second, first["state"].clone());
    full["stream"]["chain_run_id"] = Value::Null;
    full["floor_holder"] = json!("ns/run-1/reader");
    let mut declared = properties(model(&contract, "ReadRangeInput"));
    let mut sent: Vec<String> = full.as_object().unwrap().keys().cloned().collect();
    declared.sort();
    sent.sort();
    assert_eq!(
        sent, declared,
        "the contract changed ReadRangeInput; teach this test"
    );
    let answer = read_range(&calls, full.clone()).await.unwrap();
    check_output(&contract, "ReadOutput", &answer);
    assert_eq!(answer["records"], second["records"]);
    let mut unknown = full.clone();
    unknown["surprise"] = json!(1);
    let failure = read_range(&calls, unknown).await.unwrap_err();
    assert!(is_contract_mistake(&failure), "{}", failure.message);
    assert_eq!(failure.handler_error_type, "BAD_REQUEST");
    for member in required(model(&contract, "ReadRangeInput")) {
        let mut missing = full.clone();
        missing.as_object_mut().unwrap().remove(&member);
        let failure = read_range(&calls, missing).await.unwrap_err();
        assert!(
            is_contract_mistake(&failure),
            "{member}: {}",
            failure.message
        );
    }
}

#[tokio::test]
async fn every_output_member_is_declared() {
    let contract = contract();
    let store = memory();
    let calls = service(store.clone(), NexusOptions::default());
    let wf = "contract-output";
    let landed = append(
        &calls,
        json!({"stream": stream(wf, "out"), "producer_id": "p", "attempt": 1, "sequence": 1,
               "payloads": [body("a")]}),
    )
    .await
    .unwrap();
    check_output(&contract, "AppendOutput", &landed);
    // A supersession, a stale record and a record the codec can't decode, so every optional
    // member of a record crosses once.
    for (attempt, sequence, value) in [(2, 1, "a2"), (1, 2, "late"), (2, 2, "b2")] {
        append(
            &calls,
            json!({"stream": stream(wf, "out"), "producer_id": "p", "attempt": attempt,
                   "sequence": sequence, "payloads": [body(value)]}),
        )
        .await
        .unwrap();
    }
    // Written past the codec, so the handler's codec can't decode it.
    let unsealed = temporalio_streams::stored_append_record(
        "out",
        "raw",
        1,
        1,
        &temporalio_streams::proto::AppendRecord {
            kind: temporalio_streams::proto::StreamRecordKind::Data as i32,
            body: Some(Default::default()),
            content_hash: vec![1; 32],
            ..Default::default()
        },
    )
    .unwrap();
    use prost::Message;
    store
        .append(temporalio_streams::proto::StoreAppendRequest {
            chain: Some(temporalio_streams::proto::ChainId {
                namespace: NAMESPACE.to_string(),
                workflow_id: wf.to_string(),
                first_run_id: "run-1".to_string(),
                ..Default::default()
            }),
            topic: "out".to_string(),
            producer_id: "raw".to_string(),
            attempt: 1,
            sequence: 1,
            digest: vec![1; 32],
            records: vec![unsealed.encode_to_vec()],
        })
        .await
        .unwrap();
    let answer = read(&calls, json!({"stream": stream(wf, "out")}))
        .await
        .unwrap();
    check_output(&contract, "ReadOutput", &answer);
    let records = answer["records"].as_array().unwrap();
    let mut seen = Vec::new();
    for record in records {
        check_output(&contract, "RecordWire", record);
        if let Some(superseded) = record.get("superseded") {
            check_output(&contract, "Supersession", superseded);
        }
        seen.extend(record.as_object().unwrap().keys().cloned());
    }
    for member in ["superseded", "stale", "error"] {
        assert!(
            seen.contains(&member.to_string()),
            "no answer carried {member}"
        );
    }
    let mut holding = json!({"stream": stream(wf, "out")});
    holding["floor_holder"] = json!("ns/run-1/reader");
    let held = read(&calls, holding).await.unwrap();
    check_output(&contract, "ReadOutput", &held);
    check_output(&contract, "ReplayRange", &held["replay"]);
    let latest = read(
        &calls,
        json!({"stream": stream(wf, "out"), "latest_only": true}),
    )
    .await
    .unwrap();
    check_output(&contract, "ReadOutput", &latest);
}

#[tokio::test]
async fn a_follow_answer_is_one_the_contract_declares() {
    let contract = contract();
    let operations = &contract["services"]["TemporalStreams"]["operations"];
    assert_eq!(operations["follow"]["fqn"], "follow");
    assert_eq!(operations["followRange"]["fqn"], "follow_range");
    let calls = service(memory(), NexusOptions::default());
    let input = json!({"stream": stream("contract-follow", "out")});
    append(
        &calls,
        json!({"stream": input["stream"], "producer_id": "p", "attempt": 1, "sequence": 1,
            "payloads": [body("one")]}),
    )
    .await
    .unwrap();
    let mut with_holder = input.clone();
    with_holder["floor_holder"] = json!("ns/run-1/reader");
    let answer = drive(
        &calls,
        calls.follow(call(with_holder)).await,
        Codec::Sealing,
    )
    .await
    .unwrap();
    check_output(&contract, "FollowOutput", &answer);
    check_output(&contract, "FollowReplay", &answer["replay"]);
    for item in answer["records"].as_array().unwrap() {
        check_output(&contract, "FollowedWire", item);
        check_output(&contract, "RecordWire", &item["record"]);
    }
    for source in answer["sources"].as_array().unwrap() {
        check_output(&contract, "FollowSourceWire", source);
    }
}
