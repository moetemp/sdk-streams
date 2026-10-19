//! The I/O of `Follow`: reads each source of a follow and hands the pages to the state machine in
//! `links.rs`.

use crate::{
    END, StreamError, StreamResult, Streams,
    handle::address,
    links::{Applied, FollowState, Page},
    proto::{
        FollowRequest, FollowResponse, FollowSource, FollowedRecord, ReadRequest, ReadResponse,
        StreamFailure, StreamFailureKind, read_record,
    },
    reader::ReadState,
};
use prost::Message;
use std::{collections::BTreeMap, future::Future, pin::Pin, task::Poll, time::Duration};

/// How many records one Follow answers with when the request doesn't say.
const DEFAULT_MAX_RECORDS: u32 = crate::DEFAULT_MAX_RECORDS;

/// How many times one Follow reads every readable source. A round that delivers nothing ends the
/// call early, so the bound only stops a pathological chain of opens and joins.
const MAX_ROUNDS: usize = 64;

/// One Follow call as it goes.
struct Call<'a> {
    streams: &'a Streams,
    state: FollowState,
    items: Vec<FollowedRecord>,
    named: BTreeMap<u32, FollowSource>,
    budget: u32,
}

impl Call<'_> {
    fn absorb(&mut self, applied: Applied) {
        self.budget = self.budget.saturating_sub(applied.consumed as u32);
        for (number, source) in applied.named {
            self.named.entry(number).or_insert(source);
        }
        self.items.extend(applied.items);
    }

    /// The read of source `number` that comes next, waiting up to `wait`.
    fn read_request(&self, number: u32, wait: Duration) -> Option<ReadRequest> {
        let (stream, cursor, state) = self.state.read_input(number)?;
        Some(ReadRequest {
            stream: Some(stream),
            after: cursor,
            wait: wait.try_into().ok(),
            max_records: self.budget,
            state,
        })
    }

    /// Takes one read of source `number`. A page that runs past a gate is read again up to the
    /// gate, so the answer holds exactly what the follow delivered, and the source's read state
    /// stands at its last delivered record.
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
        let applied = self.state.unavailable(number, StreamFailure::from(error));
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

    fn answer(self) -> FollowResponse {
        FollowResponse {
            done: self.state.done(),
            state: self.state.encode_to_vec(),
            records: self.items,
            sources: self.named.into_values().collect(),
        }
    }
}

/// Reads a stream and the streams it links, in one order.
pub(crate) async fn follow(
    streams: &Streams,
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
        budget: match request.max_records {
            0 => DEFAULT_MAX_RECORDS,
            max => max,
        },
    };
    for _ in 0..MAX_ROUNDS {
        if !call.round().await? || call.budget == 0 || call.state.done() {
            break;
        }
    }
    if call.items.is_empty() && !wait.is_zero() && !call.state.done() {
        call.wait(wait).await?;
    }
    Ok(call.answer())
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
    }
}
