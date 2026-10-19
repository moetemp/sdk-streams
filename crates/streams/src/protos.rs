//! The code `build.rs` generates from `protos/`. The module tree follows the proto packages, since
//! the generated code names a type in another package by a path relative to it.

#![allow(missing_docs)]

pub mod coresdk {
    pub mod streams {
        include!(concat!(env!("OUT_DIR"), "/coresdk.streams.rs"));
    }
}

pub mod temporal {
    /// The stream API types that links name, which sdk-rust does not vendor. Its other API types
    /// come from `temporalio-common`.
    pub mod api {
        pub mod stream {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/temporal.api.stream.v1.rs"));
            }
        }
        pub mod enums {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/temporal.api.enums.v1.rs"));
            }
        }
    }
    pub mod sdk {
        pub mod streams {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/temporal.sdk.streams.v1.rs"));
            }
        }
    }
}

#[cfg(test)]
mod stream_record_envelope {
    use super::temporal::sdk::streams::v1::{StreamRecord, StreamRecordKind};
    use prost::Message;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    // Serialized by the Python SDK's generated envelope. Stores keep these bytes, so a renumbered
    // field or kind would make every retained record unreadable.
    const STORED: &str = "0a1b0a160a08656e636f64696e67120a6a736f6e2f706c61696e1201311208\
                          0a016b12031201761a036f757420022a017030023803";

    fn record() -> StreamRecord {
        StreamRecord {
            body: Some(Payload {
                metadata: [("encoding".to_string(), b"json/plain".to_vec())].into(),
                data: b"1".to_vec(),
                ..Default::default()
            }),
            metadata: [(
                "k".to_string(),
                Payload {
                    data: b"v".to_vec(),
                    ..Default::default()
                },
            )]
            .into(),
            topic: "out".to_string(),
            kind: StreamRecordKind::Finish as i32,
            producer_id: "p".to_string(),
            attempt: 2,
            sequence: 3,
            link: None,
            task_started_event_id: 0,
            completed_tasks: vec![],
        }
    }

    fn stored() -> Vec<u8> {
        (0..STORED.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&STORED[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn the_envelope_writes_the_stored_bytes() {
        assert_eq!(record().encode_to_vec(), stored());
    }

    #[test]
    fn the_envelope_reads_the_stored_bytes() {
        assert_eq!(StreamRecord::decode(stored().as_slice()).unwrap(), record());
    }

    #[test]
    fn the_record_kinds_keep_their_values() {
        assert_eq!(StreamRecordKind::Unspecified as i32, 0);
        assert_eq!(StreamRecordKind::Data as i32, 1);
        assert_eq!(StreamRecordKind::Finish as i32, 2);
        assert_eq!(StreamRecordKind::Link as i32, 4);
        assert_eq!(
            StreamRecordKind::try_from(3),
            Err(prost::UnknownEnumValue(3))
        );
    }
}
