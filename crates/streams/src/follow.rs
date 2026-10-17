//! The I/O of `Follow` and `FollowRange`: reads each source of a follow and hands the pages to
//! the state machine in `links.rs`.

use crate::{
    END, StreamError, StreamResult, Streams,
    handle::address,
    links::{Applied, FollowState, Page, follow_digest},
    proto::{
        FollowRangeRequest, FollowRequest, FollowResponse, FollowSource, FollowedRecord,
        ReadRangeRequest, ReadRequest, ReadResponse, SourceRange, SourceWake, StreamFailure,
        StreamFailureKind, SubscribeRequest, read_record,
    },
    reader::ReadState,
    subscribe::Subscriptions,
};
use prost::Message;
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    pin::Pin,
    task::Poll,
    time::Duration,
};

/// How many records one Follow answers with when the request doesn't say.
const DEFAULT_MAX_RECORDS: u32 = crate::DEFAULT_MAX_RECORDS;

/// How many times one Follow reads every readable source. A round that delivers nothing ends the
/// call early, so the bound only stops a pathological chain of opens and joins.
const MAX_ROUNDS: usize = 64;

/// One Follow or FollowRange call as it goes.
struct Call<'a> {
    streams: &'a Streams,
    state: FollowState,
    items: Vec<FollowedRecord>,
    named: BTreeMap<u32, FollowSource>,
    ranges: Vec<SourceRange>,
    wake_after: HashMap<u32, i64>,
    budget: u32,
    floor_holder: String,
}

impl Call<'_> {
    fn absorb(&mut self, applied: Applied) {
        self.budget = self.budget.saturating_sub(applied.consumed as u32);
        for (number, source) in applied.named {
            self.named.entry(number).or_insert(source);
        }
        self.items.extend(applied.items);
    }

    fn floor_holder(&self, number: u32) -> String {
        if self.floor_holder.is_empty() {
            String::new()
        } else {
            format!("{}/{number}", self.floor_holder)
        }
    }

    /// The read of source `number` that comes next, waiting up to `wait`.
    fn read_request(&self, number: u32, wait: Duration) -> Option<ReadRequest> {
        let (stream, cursor, state, _) = self.state.read_input(number)?;
        Some(ReadRequest {
            stream: Some(stream),
            after: cursor,
            wait: wait.try_into().ok(),
            max_records: self.budget,
            state,
            floor_holder: self.floor_holder(number),
            subscriber: None,
            max_bytes: 0,
        })
    }

    /// Takes one read of source `number`. A page that runs past a gate is read again up to the
    /// gate, so the answer and its range hold exactly what the follow delivered, and the source's
    /// read state stands at its last delivered record.
    async fn take(
        &mut self,
        number: u32,
        request: ReadRequest,
        result: StreamResult<ReadResponse>,
    ) -> StreamResult<()> {
        let mut page = match result {
            Ok(page) => page,
            Err(error) => return self.failed(number, error),
        };
        let stored = stored_count(&page);
        let consumed = self.state.clone().apply(number, page_of(&page)).consumed;
        if consumed < stored {
            let mut again = ReadRequest {
                max_records: consumed as u32,
                wait: None,
                ..request.clone()
            };
            if request.after == END {
                // Another `$end` would find the end again, after records that landed since. Read
                // from the start the first read found, and look up no record there, as it didn't.
                let mut state = match ReadState::decode_from(&request.state) {
                    Ok(state) => state,
                    Err(error) => return self.failed(number, error),
                };
                state.started = true;
                again.after = page.start_cursor.clone();
                again.state = state.encode_to_vec();
            }
            page = match self.streams.read(again).await {
                Ok(page) => page,
                Err(error) => return self.failed(number, error),
            };
        }
        self.ranges.push(SourceRange {
            source: number,
            chain_first_run_id: page.chain_first_run_id.clone(),
            start_cursor: page.start_cursor.clone(),
            cursor: page.cursor.clone(),
            stored_records: stored_count(&page) as u32,
            state_in: request.state.clone(),
            records_digest: page.records_digest.clone(),
            start_from_end: request.after == END,
            done: page.done,
            close_result: page.close_result.clone(),
            close_failure: page.close_failure.clone(),
            error: None,
        });
        self.wake_after.insert(number, page.wake_after);
        let applied = self.state.apply(number, page_of(&page));
        self.absorb(applied);
        Ok(())
    }

    /// A linked stream that can't be read leaves the follow, which goes on. A root that can't be
    /// read, or a store that failed, fails the call, since every source shares the store.
    fn failed(&mut self, number: u32, error: StreamError) -> StreamResult<()> {
        if number == 0 || error.kind == StreamFailureKind::Storage {
            return Err(error);
        }
        let failure = StreamFailure::from(error);
        self.ranges.push(SourceRange {
            source: number,
            error: Some(failure.clone()),
            ..Default::default()
        });
        let applied = self.state.unavailable(number, failure);
        self.absorb(applied);
        Ok(())
    }

    /// Reads every readable source once with no wait. Answers whether anything was delivered.
    async fn round(&mut self) -> StreamResult<bool> {
        let before = self.items.len();
        for number in self.state.readable() {
            if self.budget == 0 {
                break;
            }
            let Some(request) = self.read_request(number, Duration::ZERO) else {
                continue;
            };
            let result = self.streams.read(request.clone()).await;
            self.take(number, request, result).await?;
        }
        Ok(self.items.len() > before)
    }

    /// Waits up to `wait` on every readable source at once, and takes the first answer.
    async fn wait(&mut self, wait: Duration) -> StreamResult<()> {
        let requests: Vec<(u32, ReadRequest)> = self
            .state
            .readable()
            .into_iter()
            .filter_map(|number| Some((number, self.read_request(number, wait)?)))
            .collect();
        if requests.is_empty() {
            return Ok(());
        }
        type Waiting<'b> = Pin<
            Box<dyn Future<Output = (u32, ReadRequest, StreamResult<ReadResponse>)> + Send + 'b>,
        >;
        let streams = self.streams;
        let mut waiting: Vec<Waiting<'_>> = requests
            .into_iter()
            .map(|(number, request)| {
                Box::pin(async move {
                    let result = streams.read(request.clone()).await;
                    (number, request, result)
                }) as Waiting<'_>
            })
            .collect();
        // The other reads drop unanswered. A dropped read leaves its source as it was.
        let (number, request, result) = std::future::poll_fn(|context| {
            for read in waiting.iter_mut() {
                if let Poll::Ready(answer) = read.as_mut().poll(context) {
                    return Poll::Ready(answer);
                }
            }
            Poll::Pending
        })
        .await;
        drop(waiting);
        self.take(number, request, result).await
    }

    fn answer(self, wakes: Vec<SourceWake>) -> FollowResponse {
        let done = self.state.done();
        let (close_result, close_failure) = if done {
            self.state.root_close()
        } else {
            (None, None)
        };
        FollowResponse {
            follow_digest: follow_digest(&self.items),
            records: self.items,
            state: self.state.encode_to_vec(),
            done,
            sources: self.named.into_values().collect(),
            ranges: self.ranges,
            wakes,
            close_result,
            close_failure,
        }
    }
}

/// Reads a stream and the streams it links, in one order (DD-204, DD-205).
pub(crate) async fn follow(
    streams: &Streams,
    subscriptions: &Subscriptions,
    request: FollowRequest,
) -> StreamResult<FollowResponse> {
    let root = address(request.stream.as_ref())?.clone();
    let state = start(
        &root,
        &request.state,
        &request.after,
        request.max_depth,
        request.max_open,
    )?;
    let wait = request
        .wait
        .and_then(|wait| Duration::try_from(wait).ok())
        .unwrap_or_default();
    let mut call = Call {
        streams,
        state,
        items: vec![],
        named: BTreeMap::new(),
        ranges: vec![],
        wake_after: HashMap::new(),
        budget: match request.max_records {
            0 => DEFAULT_MAX_RECORDS,
            max => max,
        },
        floor_holder: request.floor_holder.clone(),
    };
    for _ in 0..MAX_ROUNDS {
        if !call.round().await? || call.budget == 0 || call.state.done() {
            break;
        }
    }
    if call.items.is_empty() && !wait.is_zero() && !call.state.done() {
        call.wait(wait).await?;
    }
    let mut wakes = vec![];
    if call.items.is_empty()
        && !call.state.done()
        && let Some(subscriber) = &request.subscriber
    {
        let mut landed = false;
        // A source this call didn't read empty has no counter to wait after, and a wait after
        // zero wakes at once. It is still waited on when no source was read.
        let readable = call.state.readable();
        let read_empty: Vec<u32> = readable
            .iter()
            .copied()
            .filter(|number| call.wake_after.contains_key(number))
            .collect();
        let waited = if read_empty.is_empty() {
            readable
        } else {
            read_empty
        };
        for number in waited {
            let Some((stream, ..)) = call.state.read_input(number) else {
                continue;
            };
            let subscription_id = format!("{}.{number}", subscriber.subscription_prefix);
            let wake_after = call.wake_after.get(&number).copied().unwrap_or_default();
            let newest = subscriptions
                .wait_idle(
                    streams,
                    SubscribeRequest {
                        stream: Some(stream),
                        workflow_id: subscriber.workflow_id.clone(),
                        run_id: subscriber.run_id.clone(),
                        subscription_id: subscription_id.clone(),
                        generation: subscriber.generation,
                    },
                    wake_after,
                    None,
                )
                .await?;
            // A record from before the attach woke no one, so it is read now.
            landed |= newest.is_some_and(|newest| newest > wake_after);
            wakes.push(SourceWake {
                source: number,
                subscription_id,
                wake_after,
            });
        }
        if landed && call.round().await? {
            wakes.clear();
        }
    }
    Ok(call.answer(wakes))
}

/// Reads again the ranges of an earlier Follow and gives the same answer, or fails as a replay
/// mismatch (RR-2 per source).
pub(crate) async fn follow_range(
    streams: &Streams,
    request: FollowRangeRequest,
) -> StreamResult<FollowResponse> {
    let root = address(request.stream.as_ref())?.clone();
    let state = start(
        &root,
        &request.state_in,
        "",
        request.max_depth,
        request.max_open,
    )?;
    let mismatch = |message: &str| StreamError::new(StreamFailureKind::ReplayMismatch, message);
    let mut call = Call {
        streams,
        state,
        items: vec![],
        named: BTreeMap::new(),
        ranges: request.ranges.clone(),
        wake_after: HashMap::new(),
        budget: u32::MAX,
        floor_holder: request.floor_holder.clone(),
    };
    for range in request.ranges {
        if let Some(error) = range.error {
            if range.source == 0 {
                return Err(mismatch(
                    "a replayed follow names a failed read of its root",
                ));
            }
            let applied = call.state.unavailable(range.source, error);
            call.absorb(applied);
            continue;
        }
        let Some((stream, ..)) = call.state.read_input(range.source) else {
            return Err(mismatch(
                "a replayed follow reads a source that its earlier answers never opened",
            ));
        };
        let page = if range.chain_first_run_id.is_empty() {
            // The owner had not started, so the read found no chain and nothing to read again.
            if range.stored_records > 0 {
                return Err(mismatch("a replayed read with records names no chain"));
            }
            Page {
                cursor: range.cursor.clone(),
                state: range.state_in.clone(),
                ..Default::default()
            }
        } else {
            let read = streams
                .read_range(ReadRangeRequest {
                    stream: Some(stream),
                    chain_first_run_id: range.chain_first_run_id.clone(),
                    after: range.start_cursor.clone(),
                    to: range.cursor.clone(),
                    stored_records: range.stored_records,
                    state: range.state_in.clone(),
                    records_digest: range.records_digest.clone(),
                    floor_holder: call.floor_holder(range.source),
                    start_from_end: range.start_from_end,
                })
                .await?;
            Page {
                records: read.records,
                cursor: range.cursor.clone(),
                state: read.state,
                ..Default::default()
            }
        };
        let applied = call.state.apply(
            range.source,
            Page {
                done: range.done,
                close_result: range.close_result,
                close_failure: range.close_failure,
                ..page
            },
        );
        call.absorb(applied);
    }
    if follow_digest(&call.items) != request.follow_digest {
        return Err(mismatch(
            "the replayed follow gives other records or another order than the follow it repeats",
        ));
    }
    Ok(call.answer(vec![]))
}

/// The follow `state` resumes, or a new one of `root` from `after`.
fn start(
    root: &crate::proto::StreamAddress,
    state: &[u8],
    after: &str,
    max_depth: u32,
    max_open: u32,
) -> StreamResult<FollowState> {
    if state.is_empty() {
        return Ok(FollowState::new(root.clone(), after, max_depth, max_open));
    }
    let state = FollowState::decode_from(state)?;
    if state.root() != root {
        return Err(StreamError::invalid_argument(
            "a follow resumes only on the root stream it started on",
        ));
    }
    Ok(state)
}

fn stored_count(page: &ReadResponse) -> usize {
    page.records
        .iter()
        .filter(|record| matches!(record.record, Some(read_record::Record::Stored(_))))
        .count()
}

fn page_of(page: &ReadResponse) -> Page {
    Page {
        records: page.records.clone(),
        cursor: page.cursor.clone(),
        state: page.state.clone(),
        done: page.done,
        close_result: page.close_result.clone(),
        close_failure: page.close_failure.clone(),
    }
}
