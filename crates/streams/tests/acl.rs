//! The Redis users of `docs/redis.md`, with exactly the commands it lists for each role, run
//! every Store call of that role. Standalone Redis only, since a cluster keeps users per node.

mod support;

use prost::Message;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use support::server_url;
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    digest::batch_digest,
    ids::client_producer_id,
    proto::{
        AppendRecord, Producer, ProducerEpoch, ProducerKind, StreamRecord, StreamRecordKind,
        StreamRef,
    },
    redis::{RedisStore, RedisStoreOptions},
    store::{Batch, Owner, Store, Withdrawal},
};

const PREFIX: &str = "streams-acl";

/// The commands `docs/redis.md` lists for a Producer and for a Reader.
const PRODUCER: &str = "+evalsha +script|load +time +hget +hset +hdel +exists +pexpireat +xadd \
                        +xtrim +xrevrange";
const READER: &str = "+evalsha +script|load +time +hmget +xrange +xread +xrevrange";

async fn admin(url: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// A user with `commands` on the keys of `namespace`, and the URL that logs in as it.
async fn user(url: &str, name: &str, namespace: &str, commands: &str) -> String {
    let mut conn = admin(url).await;
    let mut acl = redis::cmd("ACL");
    acl.arg("SETUSER")
        .arg(name)
        .arg("reset")
        .arg("on")
        .arg(">pw")
        .arg(format!("~{PREFIX}:{{{namespace}:*"));
    for command in commands.split_whitespace() {
        acl.arg(command);
    }
    let _: () = acl.query_async(&mut conn).await.unwrap();
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    format!("redis://{name}:pw@{rest}")
}

async fn store(url: String) -> Box<dyn Store> {
    let mut options = RedisStoreOptions::new(url);
    options.key_prefix = PREFIX.to_string();
    Box::new(RedisStore::connect(options).await.unwrap())
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

#[tokio::test]
async fn the_documented_commands_run_every_call_of_each_role() {
    let Some(url) = server_url("STREAMS_REDIS_URL") else {
        return;
    };
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let namespace = format!("ns-{unique}");
    let (producer_name, reader_name) = (format!("prod-{unique}"), format!("read-{unique}"));
    let stream = StreamRef {
        id: "s".to_string(),
        ..Default::default()
    };
    // With no cached script, each user loads the scripts it runs.
    let _: () = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut admin(&url).await)
        .await
        .unwrap();

    let producer = store(user(&url, &producer_name, &namespace, PRODUCER).await).await;
    producer
        .create(
            &namespace,
            &stream,
            None,
            Owner::Client,
            &Withdrawal::default(),
        )
        .await
        .unwrap();
    let records = vec![AppendRecord {
        topic: "t".to_string(),
        body: Some(Payload {
            data: b"x".to_vec(),
            ..Default::default()
        }),
    }];
    let stored = vec![
        StreamRecord {
            kind: StreamRecordKind::Data as i32,
            topic: "t".to_string(),
            producer: Some(client()),
            body: records[0].body.clone(),
            ..Default::default()
        }
        .encode_to_vec(),
    ];
    let batch = Batch {
        producer: &client(),
        sequence: 1,
        records: &stored,
        digest: &batch_digest(&records),
        withdrawal: Withdrawal::default(),
    };
    let appended = producer.append(&namespace, &stream, &batch).await.unwrap();
    let (first, _) = appended.offsets.unwrap();

    let reader = store(user(&url, &reader_name, &namespace, READER).await).await;
    let (state, entries) = reader.read(&namespace, "s", 0, 10).await.unwrap();
    assert_eq!((state.newest, entries.len()), (first, 1));
    assert_eq!(
        reader
            .read_back(&namespace, "s", first, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    assert!(
        !reader
            .wait_after(&namespace, "s", first, deadline)
            .await
            .unwrap()
    );

    producer
        .close(&namespace, &stream, &client(), b"")
        .await
        .unwrap();
    let mut conn = admin(&url).await;
    for name in [producer_name, reader_name] {
        let _: () = redis::cmd("ACL")
            .arg("DELUSER")
            .arg(name)
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}
