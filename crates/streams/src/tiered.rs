//! A hot store with an archive behind it, as one [StreamStore].

use crate::{
    StreamArchive, StreamError, StreamResult, StreamStore,
    archive::{
        ArchivedClose, CLOSE_NAME, Segment, holding_start, owner_prefixes, segment_name,
        segments_start, sort_key, topic_prefix,
    },
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse, StoredClose,
        StreamFailureKind,
    },
};
use std::{sync::Arc, time::Duration};

/// A [StreamStore] that reads from an archive what its hot store no longer holds.
///
/// Writes, closes, holds and the notification log are the hot store's. The archive keeps the
/// hot store's positions, so the cursors are the hot store's too, and a read crosses from the
/// archive to the hot store with no gap. Only the hot store waits. A read that the archive
/// answers returns at once. A read of records that neither tier holds fails as expired, as on the
/// hot store alone.
pub struct TieredStore {
    hot: Arc<dyn StreamStore>,
    archive: Arc<dyn StreamArchive>,
}

impl TieredStore {
    /// `hot` with `archive` behind it. The hot store must keep an archive hold, and something
    /// must run [crate::archive_topic], or the archive stays empty.
    pub fn new(hot: Arc<dyn StreamStore>, archive: Arc<dyn StreamArchive>) -> Self {
        Self { hot, archive }
    }

    /// The archive behind the hot store.
    pub fn archive(&self) -> &Arc<dyn StreamArchive> {
        &self.archive
    }

    /// The hot store.
    pub fn hot(&self) -> &Arc<dyn StreamStore> {
        &self.hot
    }

    /// Whether the archive may hold something of a topic. A hot store that knows the topic and
    /// archived none of it saves the archive calls.
    async fn may_archive(&self, chain: &ChainId, topic: &str) -> StreamResult<bool> {
        Ok(self.hot.archived(chain, topic).await? != Some(String::new()))
    }

    async fn close_of(&self, prefix: &str) -> StreamResult<Option<ArchivedClose>> {
        let name = format!("{prefix}{CLOSE_NAME}");
        self.archive
            .get(&name)
            .await?
            .map(|bytes| ArchivedClose::decode_object(&name, &bytes))
            .transpose()
    }

    async fn segment(&self, name: &str) -> StreamResult<Segment> {
        let bytes = self.archive.get(name).await?.ok_or_else(|| {
            StreamError::storage(format!("archive object {name} went away while read"))
        })?;
        Segment::decode(name, &bytes)
    }

    /// The archive's answer to a read after `after`, or `None` when the archive holds nothing
    /// after it and does not end at it.
    async fn archived_page(
        &self,
        request: &StoreReadRequest,
        chain: &ChainId,
    ) -> StreamResult<Option<StoreReadResponse>> {
        let after = request.after_position.as_str();
        let prefix = topic_prefix(chain, &request.topic);
        let start = if after.is_empty() {
            segments_start(&prefix)
        } else {
            segment_name(&prefix, after)?
        };
        let Some(name) = self.archive.first_after(&prefix, &start).await? else {
            if after.is_empty() {
                return Ok(None);
            }
            // The archive ends at `after` when a segment holds it.
            if self
                .archive
                .first_after(&prefix, &holding_start(&prefix, after)?)
                .await?
                .is_none()
            {
                return Ok(None);
            }
            let close = self.close_of(&prefix).await?;
            if let Some(close) = close.filter(|close| close.last_position == after) {
                return Ok(Some(StoreReadResponse {
                    closed: true,
                    close: close.close,
                    ..Default::default()
                }));
            }
            // The hot store forgot an open topic. Waiting keeps the reader from spinning.
            tokio::time::sleep(wait_of(request)).await;
            return Ok(Some(StoreReadResponse::default()));
        };
        let mut segment = self.segment(&name).await?;
        let after_key = (!after.is_empty()).then(|| sort_key(after)).transpose()?;
        if let Some(after_key) = &after_key
            && !segment.after_position.is_empty()
            && sort_key(&segment.after_position)? > *after_key
        {
            return Err(StreamError::expired(format!(
                "the records after position {after} on topic {:?} are in neither the store nor \
                 its archive",
                request.topic
            )));
        }
        // A page fills from as many segments as it needs and then from the hot store, as a page
        // of the hot store alone would, so a caller that reads an exact range gets it in one page.
        let max = request.max_records.max(1) as usize;
        let mut page = StoreReadResponse::default();
        let mut name = name;
        loop {
            let last = segment.last_position().to_string();
            for record in segment.records {
                if page.records.len() >= max {
                    break;
                }
                if after_key.as_ref().is_none_or(|after_key| {
                    sort_key(&record.position).is_ok_and(|key| key > *after_key)
                }) {
                    page.records.push(record);
                }
            }
            if page.records.len() >= max
                || page
                    .records
                    .last()
                    .is_none_or(|record| record.position != last)
            {
                return Ok(Some(page));
            }
            match self.archive.first_after(&prefix, &name).await? {
                Some(next) => {
                    let next_segment = self.segment(&next).await?;
                    // A segment that doesn't start at this one's end follows lost records, which
                    // the next read reports.
                    if next_segment.after_position != last {
                        return Ok(Some(page));
                    }
                    (name, segment) = (next, next_segment);
                }
                None => {
                    if let Some(close) = self
                        .close_of(&prefix)
                        .await?
                        .filter(|close| close.last_position == last)
                    {
                        page.closed = true;
                        page.close = close.close;
                        return Ok(Some(page));
                    }
                    // The rest is in the hot store. A failure there reaches the next read.
                    let rest = self
                        .hot
                        .read(StoreReadRequest {
                            after_position: last,
                            wait: None,
                            max_records: (max - page.records.len()) as u32,
                            ..request.clone()
                        })
                        .await;
                    if let Ok(rest) = rest {
                        page.records.extend(rest.records);
                        page.closed = rest.closed;
                        page.close = rest.close;
                    }
                    return Ok(Some(page));
                }
            }
        }
    }

    /// The newest position the archive holds of a topic.
    async fn archived_newest(&self, chain: &ChainId, topic: &str) -> StreamResult<String> {
        let prefix = topic_prefix(chain, topic);
        if let Some(close) = self.close_of(&prefix).await? {
            return Ok(close.newest_ever);
        }
        match self.archive.last(&segments_start(&prefix)).await? {
            Some(name) => Ok(self.segment(&name).await?.last_position().to_string()),
            None => Ok(String::new()),
        }
    }
}

fn wait_of(request: &StoreReadRequest) -> Duration {
    request.wait.as_ref().map_or(Duration::ZERO, |wait| {
        Duration::new(
            u64::try_from(wait.seconds).unwrap_or(0),
            u32::try_from(wait.nanos).unwrap_or(0),
        )
    })
}

fn chain_of(chain: Option<&ChainId>) -> StreamResult<&ChainId> {
    chain.ok_or_else(|| StreamError::invalid_argument("the request names no chain"))
}

#[async_trait::async_trait]
impl StreamStore for TieredStore {
    fn name(&self) -> &str {
        self.hot.name()
    }

    fn capabilities(&self) -> crate::Capabilities {
        self.hot.capabilities()
    }

    fn notification_log(&self) -> Option<Arc<dyn crate::NotificationLog>> {
        self.hot.notification_log()
    }

    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        self.hot.append(request).await
    }

    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        let chain = chain_of(request.chain.as_ref())?.clone();
        if request.after_position.is_empty() {
            // The archive holds a topic from its start, so a read from the start begins there.
            if self.may_archive(&chain, &request.topic).await?
                && let Some(page) = self.archived_page(&request, &chain).await?
            {
                return Ok(page);
            }
            return self.hot.read(request).await;
        }
        match self.hot.read(request.clone()).await {
            Err(error)
                if matches!(
                    error.kind,
                    StreamFailureKind::Expired | StreamFailureKind::NotFound
                ) =>
            {
                Ok(self.archived_page(&request, &chain).await?.ok_or(error)?)
            }
            answer => answer,
        }
    }

    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        let chain = chain_of(request.chain.as_ref())?.clone();
        let topic = request.topic.clone();
        let latest = self.hot.latest(request).await?;
        if !latest.position.is_empty() || !self.may_archive(&chain, &topic).await? {
            return Ok(latest);
        }
        let newest = self.newest_ever(&chain, &topic).await?;
        let held = !newest.is_empty()
            && self
                .archive
                .first_after(
                    &topic_prefix(&chain, &topic),
                    &holding_start(&topic_prefix(&chain, &topic), &newest)?,
                )
                .await?
                .is_some();
        Ok(StoreLatestResponse {
            position: if held { newest } else { String::new() },
        })
    }

    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>> {
        if let Some(record) = self.hot.record_at(chain, topic, position).await? {
            return Ok(Some(record));
        }
        let prefix = topic_prefix(chain, topic);
        let Some(name) = self
            .archive
            .first_after(&prefix, &holding_start(&prefix, position)?)
            .await?
        else {
            return Ok(None);
        };
        Ok(self
            .segment(&name)
            .await?
            .records
            .into_iter()
            .find(|record| record.position == position)
            .map(|record| record.record))
    }

    async fn newest_ever(&self, chain: &ChainId, topic: &str) -> StreamResult<String> {
        let newest = self.hot.newest_ever(chain, topic).await?;
        if !newest.is_empty() || !self.may_archive(chain, topic).await? {
            return Ok(newest);
        }
        self.archived_newest(chain, topic).await
    }

    async fn dropped_after(
        &self,
        chain: &ChainId,
        topic: &str,
        mark: &str,
    ) -> StreamResult<Option<String>> {
        let Some(dropped) = self.hot.dropped_after(chain, topic, mark).await? else {
            return Ok(None);
        };
        let archived = self.hot.archived(chain, topic).await?.unwrap_or_default();
        if !archived.is_empty() && sort_key(&archived)? >= sort_key(&dropped)? {
            return Ok(None);
        }
        Ok(Some(dropped))
    }

    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        self.hot.close_chain(chain).await
    }

    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        self.hot.close_topic(chain, topic).await
    }

    async fn close_topic_with(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<StoredClose> {
        self.hot.close_topic_with(chain, topic, close).await
    }

    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()> {
        self.hot
            .retire_producer(chain, topic, producer_id, attempt)
            .await
    }

    async fn list_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        self.hot.list_topics(chain).await
    }

    async fn written_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        self.hot.written_topics(chain).await
    }

    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        let prefixes = owner_prefixes(&request);
        let mut response = self.hot.delete_owner(request).await?;
        for prefix in prefixes {
            response.deleted += self.archive.delete_prefix(&prefix).await?;
        }
        Ok(response)
    }

    async fn hold_floor(
        &self,
        chain: &ChainId,
        topic: &str,
        holder: &str,
        position: &str,
    ) -> StreamResult<()> {
        self.hot.hold_floor(chain, topic, holder, position).await
    }

    async fn release_floor(&self, chain: &ChainId, topic: &str, holder: &str) -> StreamResult<()> {
        self.hot.release_floor(chain, topic, holder).await
    }

    async fn advance_archive(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<()> {
        self.hot.advance_archive(chain, topic, position).await
    }

    async fn archived(&self, chain: &ChainId, topic: &str) -> StreamResult<Option<String>> {
        self.hot.archived(chain, topic).await
    }

    async fn close_stream(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<()> {
        self.hot.close_stream(chain, topic, close).await
    }

    async fn flush_notifications(&self) {
        self.hot.flush_notifications().await
    }
}
