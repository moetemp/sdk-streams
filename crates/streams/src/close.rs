//! Closing streams and naming topics on every store.
//!
//! A store with producer state keeps close flags that its append checks. On other stores a close
//! is a `CLOSE` record, and readers drop every record after the first one. Readers never call
//! this. Producers and the owner's Core do.

use crate::{
    StreamResult, StreamStore, close_record,
    proto::{
        ChainId, StoreAppendRequest, StoreAppendResponse, StoreReadRequest, StreamRecord,
        StreamRecordKind,
    },
    topic_record,
};
use prost::Message;
use sha2::{Digest, Sha256};

/// The chain's control log, where producers name their topics on a store that can't list them.
pub const CHAIN_LOG: &str = "_chain";

const CHAIN_LOG_PAGE: u32 = 1_000;

/// Closes one topic of `chain` for every reader, as `producer_id` at `attempt`. With close flags
/// the store refuses later appends. Without them, the `CLOSE` record ends every read there, and
/// a later append lands where no reader delivers it.
pub async fn close_topic(
    store: &dyn StreamStore,
    chain: &ChainId,
    topic: &str,
    producer_id: &str,
    attempt: i64,
) -> StreamResult<()> {
    if store.capabilities().producer_state {
        return store.close_topic(chain, topic).await;
    }
    write_one(
        store,
        chain,
        topic,
        close_record(topic, producer_id, attempt),
    )
    .await
}

/// Closes every topic of `chain`, as [close_topic] closes one. Without close flags, it finds the
/// topics by listing them, or from the `TOPIC` records in [CHAIN_LOG].
pub async fn close_chain(
    store: &dyn StreamStore,
    chain: &ChainId,
    producer_id: &str,
    attempt: i64,
) -> StreamResult<()> {
    if store.capabilities().producer_state {
        return store.close_chain(chain).await;
    }
    for topic in chain_topics(store, chain).await? {
        close_topic(store, chain, &topic, producer_id, attempt).await?;
    }
    Ok(())
}

/// Appends one batch of a producer attempt. On a store that can neither keep close flags nor list
/// topics, the attempt's first batch on a topic names the topic in [CHAIN_LOG] first. A crash
/// between the two leaves a name with no record, and a close of an empty topic does no harm.
pub async fn append_batch(
    store: &dyn StreamStore,
    request: StoreAppendRequest,
) -> StreamResult<StoreAppendResponse> {
    let capabilities = store.capabilities();
    if !capabilities.producer_state && !capabilities.topic_listing && request.sequence == 1 {
        let chain = request.chain.clone().unwrap_or_default();
        let named = topic_record(&request.topic, &request.producer_id, request.attempt);
        write_one(store, &chain, CHAIN_LOG, named).await?;
    }
    store.append(request).await
}

/// The topics of `chain` that took a record.
async fn chain_topics(store: &dyn StreamStore, chain: &ChainId) -> StreamResult<Vec<String>> {
    if store.capabilities().topic_listing {
        return store.list_topics(chain).await;
    }
    let mut topics: Vec<String> = Vec::new();
    let mut after = String::new();
    loop {
        let page = store
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: CHAIN_LOG.to_string(),
                after_position: after.clone(),
                wait: None,
                max_records: CHAIN_LOG_PAGE,
            })
            .await?;
        let Some(last) = page.records.last() else {
            break;
        };
        after = last.position.clone();
        for stored in &page.records {
            let Ok(record) = StreamRecord::decode(&*stored.record) else {
                continue;
            };
            if record.kind == StreamRecordKind::Topic as i32 && !topics.contains(&record.topic) {
                topics.push(record.topic);
            }
        }
    }
    Ok(topics)
}

/// Appends one control record. Its sequence is zero, so no reader takes it for a copy.
async fn write_one(
    store: &dyn StreamStore,
    chain: &ChainId,
    topic: &str,
    record: StreamRecord,
) -> StreamResult<()> {
    let bytes = record.encode_to_vec();
    store
        .append(StoreAppendRequest {
            chain: Some(chain.clone()),
            topic: topic.to_string(),
            producer_id: record.producer_id,
            attempt: record.attempt,
            sequence: 0,
            digest: Sha256::digest(&bytes).to_vec(),
            records: vec![bytes],
        })
        .await
        .map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Capabilities, MemoryStore, StreamStore};

    fn chain() -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: "run-1".to_string(),
        }
    }

    fn batch(topic: &str, sequence: i64) -> StoreAppendRequest {
        StoreAppendRequest {
            chain: Some(chain()),
            topic: topic.to_string(),
            producer_id: "p".to_string(),
            attempt: 1,
            sequence,
            digest: vec![sequence as u8],
            records: vec![
                StreamRecord {
                    topic: topic.to_string(),
                    kind: StreamRecordKind::Data as i32,
                    producer_id: "p".to_string(),
                    attempt: 1,
                    sequence,
                    ..Default::default()
                }
                .encode_to_vec(),
            ],
        }
    }

    /// Each record of a topic as `(kind, producer)`.
    async fn kinds(store: &dyn StreamStore, topic: &str) -> Vec<(i32, String)> {
        let page = store
            .read(StoreReadRequest {
                chain: Some(chain()),
                topic: topic.to_string(),
                max_records: 100,
                ..Default::default()
            })
            .await
            .unwrap();
        page.records
            .iter()
            .map(|stored| {
                let record = StreamRecord::decode(&*stored.record).unwrap();
                (record.kind, record.producer_id)
            })
            .collect()
    }

    const DATA: i32 = StreamRecordKind::Data as i32;
    const CLOSE: i32 = StreamRecordKind::Close as i32;
    const TOPIC: i32 = StreamRecordKind::Topic as i32;

    fn without(mask: impl FnOnce(&mut Capabilities)) -> MemoryStore {
        let mut capabilities = MemoryStore::new().capabilities();
        mask(&mut capabilities);
        MemoryStore::with_capabilities(capabilities)
    }

    #[tokio::test]
    async fn with_close_flags_a_close_sets_them_and_writes_no_record() {
        let store = MemoryStore::new();
        append_batch(&store, batch("a", 1)).await.unwrap();
        close_chain(&store, &chain(), "@run-1", 7).await.unwrap();
        assert_eq!(kinds(&store, "a").await, [(DATA, "p".to_string())]);
        let refused = store.append(batch("a", 2)).await.unwrap_err();
        assert_eq!(refused.kind, crate::proto::StreamFailureKind::Closed);
    }

    #[tokio::test]
    async fn without_close_flags_a_chain_close_writes_a_close_on_each_listed_topic() {
        let store = without(|c| c.producer_state = false);
        append_batch(&store, batch("a", 1)).await.unwrap();
        append_batch(&store, batch("b", 1)).await.unwrap();
        close_chain(&store, &chain(), "@run-1", 7).await.unwrap();
        for topic in ["a", "b"] {
            assert_eq!(
                kinds(&store, topic).await,
                [(DATA, "p".to_string()), (CLOSE, "@run-1".to_string())]
            );
        }
        assert!(kinds(&store, CHAIN_LOG).await.is_empty());
    }

    #[tokio::test]
    async fn without_listing_a_session_names_its_topic_once_and_a_close_finds_it() {
        let store = without(|c| {
            c.producer_state = false;
            c.topic_listing = false;
        });
        append_batch(&store, batch("a", 1)).await.unwrap();
        append_batch(&store, batch("a", 2)).await.unwrap();
        append_batch(&store, batch("b", 1)).await.unwrap();
        assert_eq!(
            kinds(&store, CHAIN_LOG).await,
            [(TOPIC, "p".to_string()), (TOPIC, "p".to_string())]
        );
        close_chain(&store, &chain(), "@run-1", 7).await.unwrap();
        assert_eq!(kinds(&store, "a").await.last().unwrap().0, CLOSE);
        assert_eq!(kinds(&store, "b").await.last().unwrap().0, CLOSE);
        // A close of a topic no record reached does no harm.
        close_topic(&store, &chain(), "quiet", "", 0).await.unwrap();
        assert_eq!(kinds(&store, "quiet").await, [(CLOSE, String::new())]);
    }
}
