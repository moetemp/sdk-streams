//! Readers that sleep on a stream's notifier.
//!
//! A Workflow's reader attaches to the stream's notifier as an idle callback at its first read
//! that finds nothing. Each read that finds nothing asks the notifier for one wake after the
//! read's counter, which reaches the Workflow on its next Workflow Task. A read that finds records
//! asks for nothing, so a reader that keeps up costs the notifier nothing. The attach names the
//! reader by its run and its subscription id, which replay gives back, so a rerun attaches as the
//! same reader and the server keeps one callback. A client reader has no task to wake, so it waits on
//! the notifier with a long poll.

use crate::{
    NotifierClient, NotifyError, OWNER_RECHECK, StreamError, StreamResult, Streams, StreamsOptions,
    WorkflowAttachment,
    handle::{Remembered, address, chain_of},
    proto::{
        ChainId, ReadRequest, ReadResponse, StreamAddress, StreamFailureKind, SubscribeRequest,
        SubscribeResponse, UnsubscribeRequest, UnsubscribeResponse, WaitForNotificationRequest,
        WaitForNotificationResponse,
    },
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// How long after it sends a poll Core reads the stream once more. The poll starts the stream's
/// notifier, and a record that lands later notifies it.
const REREAD_AFTER: Duration = Duration::from_millis(200);

/// The longest subscription id the server takes.
const MAX_SUBSCRIPTION_ID_BYTES: usize = 256;

/// The subscriptions this process attached, so a reader's later reads send no attach. A reader
/// that ends or a run that closes never detaches, so the oldest are forgotten. A reader this
/// process forgot attaches again, and the server dedupes that attach.
pub(crate) struct Subscriptions {
    client: Option<Arc<dyn NotifierClient>>,
    attached: Remembered<SubscriptionKey>,
    /// The server keeps every reader standing, as one from before idle callbacks does, so this
    /// process attaches readers that way and sends no waits.
    standing: AtomicBool,
}

impl Default for Subscriptions {
    fn default() -> Self {
        Self::new(None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SubscriptionKey {
    namespace: String,
    owner_kind: i32,
    owner_workflow_id: String,
    owner_activity_id: String,
    owner_run_id: String,
    topic: String,
    request_id: String,
}

impl SubscriptionKey {
    fn new(stream: &StreamAddress, request_id: &str) -> Self {
        Self {
            namespace: stream.namespace.clone(),
            owner_kind: stream.owner_kind,
            owner_workflow_id: stream.workflow_id.clone(),
            owner_activity_id: stream.activity_id.clone(),
            owner_run_id: stream.run_id.clone(),
            topic: stream.topic.clone(),
            request_id: request_id.to_string(),
        }
    }
}

/// The request id of a reader's attach. The server dedupes attaches by it, so it depends only on
/// what replay gives back. A later generation is a new attach, after the notifier dropped the
/// reader and the server would answer the old id as already done.
fn request_id(run_id: &str, subscription_id: &str, generation: u32) -> String {
    match generation {
        0 => format!("jetty/{run_id}/{subscription_id}"),
        generation => format!("jetty/{run_id}/{subscription_id}/{generation}"),
    }
}

/// Where a reader is attached, and whether this call sent the attach.
struct Attach {
    chain: ChainId,
    request_id: String,
    key: SubscriptionKey,
    attached: bool,
}

fn check_subscription(run_id: &str, subscription_id: &str) -> StreamResult<()> {
    if run_id.is_empty() || subscription_id.is_empty() {
        return Err(StreamError::invalid_argument(
            "a subscription needs the reader's run id and its subscription id",
        ));
    }
    if subscription_id.len() > MAX_SUBSCRIPTION_ID_BYTES {
        return Err(StreamError::invalid_argument(format!(
            "a subscription id is at most {MAX_SUBSCRIPTION_ID_BYTES} bytes, not {}",
            subscription_id.len()
        )));
    }
    Ok(())
}

fn subscribe_error(error: NotifyError) -> StreamError {
    match error {
        NotifyError::Refused(message) => StreamError::refused(message),
        NotifyError::Unsupported(message) => StreamError::unsupported(message),
        NotifyError::Failed(message) => StreamError::storage(message),
    }
}

impl Subscriptions {
    pub(crate) fn new(client: Option<Arc<dyn NotifierClient>>) -> Self {
        Self {
            client,
            attached: Remembered::new(StreamsOptions::default().remembered),
            standing: AtomicBool::new(false),
        }
    }

    pub(crate) fn client(&self) -> StreamResult<&Arc<dyn NotifierClient>> {
        self.client.as_ref().ok_or_else(|| {
            StreamError::unsupported("a Workflow reader needs a client connected to the server")
        })
    }

    /// Attaches the reader idle once per process. An idle reader hears nothing until a wait.
    pub(crate) async fn subscribe(
        &self,
        streams: &Streams,
        request: SubscribeRequest,
    ) -> StreamResult<SubscribeResponse> {
        let stream = address(request.stream.as_ref())?.clone();
        let attach = self.attach(streams, request, None).await?;
        Ok(SubscribeResponse {
            newest_counter: streams.newest(&stream, &attach.chain).await?.0,
            chain_first_run_id: attach.chain.first_run_id,
            attached: attach.attached,
        })
    }

    /// Attaches the reader idle when this process has not, and asks the notifier for one wake
    /// once its counter is above `after_counter`. Answers the newest counter after a fresh
    /// attach, since a record that landed before the notifier existed woke no one, and `None`
    /// after a wait on a reader attached before. `chain_first_run_id` names the chain a read
    /// answered from, which the wait must be on. Without it, the wait is on the chain the stream
    /// names now.
    pub(crate) async fn wait_idle(
        &self,
        streams: &Streams,
        request: SubscribeRequest,
        after_counter: i64,
        chain_first_run_id: Option<&str>,
    ) -> StreamResult<Option<i64>> {
        let stream = address(request.stream.as_ref())?.clone();
        let client = self.client()?;
        let mut attach = self
            .attach(streams, request.clone(), chain_first_run_id)
            .await?;
        match self
            .send_wait(client, &stream, &attach, after_counter)
            .await
        {
            Err(NotifyError::Refused(error)) if !attach.attached => {
                // The notifier lost the callback this process remembers, so it attaches again.
                tracing::debug!(%error, "The notifier refused a reader's wait, so it attaches again");
                self.attached.forget(&attach.key);
                attach = self.attach(streams, request, chain_first_run_id).await?;
                self.send_wait(client, &stream, &attach, after_counter)
                    .await
                    .map_err(subscribe_error)?;
            }
            other => other.map_err(subscribe_error)?,
        }
        if !attach.attached {
            return Ok(None);
        }
        // Read after the wait landed, so a record after this one passes the wait.
        Ok(Some(streams.newest(&stream, &attach.chain).await?.0))
    }

    /// Sends the reader's wait, unless the server keeps readers standing.
    async fn send_wait(
        &self,
        client: &Arc<dyn NotifierClient>,
        stream: &StreamAddress,
        attach: &Attach,
        after_counter: i64,
    ) -> Result<(), NotifyError> {
        if self.standing.load(Ordering::Relaxed) {
            return Ok(());
        }
        match client
            .wait_callback(
                attach.chain.clone(),
                stream.topic.clone(),
                attach.request_id.clone(),
                after_counter,
            )
            .await
        {
            Err(NotifyError::Unsupported(error)) => {
                // The attach landed, so the server has a notifier but no waits. It took the
                // reader as a standing one, which every notification wakes.
                self.keep_standing(&error);
                Ok(())
            }
            other => other,
        }
    }

    fn keep_standing(&self, error: &str) {
        if !self.standing.swap(true, Ordering::Relaxed) {
            tracing::debug!(
                error,
                "The server keeps stream readers standing, so this process sends no waits"
            );
        }
    }

    /// Reads, and when the answer has no records and is not done, waits as `wait_idle` does for
    /// the read's `subscriber`. Reads once more when a record landed before a fresh attach.
    pub(crate) async fn read(
        &self,
        streams: &Streams,
        mut request: ReadRequest,
    ) -> StreamResult<ReadResponse> {
        let Some(subscriber) = request.subscriber.take() else {
            return streams.read(request).await;
        };
        check_subscription(&subscriber.run_id, &subscriber.subscription_id)?;
        let answer = streams.read(request.clone()).await?;
        // A read inside the owner's start grace has no chain yet, so there is no notifier to wait
        // on. Lang reads again on its timer.
        if !answer.records.is_empty() || answer.done || answer.chain_first_run_id.is_empty() {
            return Ok(answer);
        }
        let waited = self
            .wait_idle(
                streams,
                SubscribeRequest {
                    stream: request.stream.clone(),
                    workflow_id: subscriber.workflow_id,
                    run_id: subscriber.run_id,
                    subscription_id: subscriber.subscription_id,
                    generation: subscriber.generation,
                },
                answer.wake_after,
                Some(&answer.chain_first_run_id),
            )
            .await;
        let newest = match waited {
            // The read's chain ended and a new one took the stream's owner id, so the ended
            // chain's notifier is gone. The read is over, as for any ended owner.
            Err(error)
                if error.kind == StreamFailureKind::Refused
                    && streams
                        .chain_now(address(request.stream.as_ref())?)
                        .await?
                        .first_run_id
                        != answer.chain_first_run_id =>
            {
                return Ok(ReadResponse {
                    done: true,
                    ..answer
                });
            }
            waited => waited?,
        };
        if newest.is_none_or(|newest| newest <= answer.wake_after) {
            return Ok(answer);
        }
        streams
            .read(ReadRequest {
                after: answer.cursor,
                wait: None,
                state: answer.state,
                ..request
            })
            .await
    }

    /// Attaches the reader idle once per process, on chain `chain_first_run_id` when given.
    async fn attach(
        &self,
        streams: &Streams,
        request: SubscribeRequest,
        chain_first_run_id: Option<&str>,
    ) -> StreamResult<Attach> {
        let stream = address(request.stream.as_ref())?;
        if request.workflow_id.is_empty() {
            return Err(StreamError::invalid_argument(
                "a subscription needs the reader's Workflow id",
            ));
        }
        check_subscription(&request.run_id, &request.subscription_id)?;
        let request_id = request_id(
            &request.run_id,
            &request.subscription_id,
            request.generation,
        );
        let key = SubscriptionKey::new(stream, &request_id);
        if let Some(attached) = self.attached.get(&key)
            && chain_first_run_id.is_none_or(|first| first == attached.first_run_id)
        {
            return Ok(Attach {
                chain: ChainId {
                    first_run_id: attached.first_run_id,
                    ..streams.chain(stream).await?
                },
                request_id,
                key,
                attached: false,
            });
        }
        let client = self.client()?;
        // A read's own chain, so the wait lands where the read's counters come from. Otherwise
        // fresh for the latest run, since the Workflow id may have moved on to a new chain since
        // a read warmed the cache, and an attach to the ended chain would never wake the reader.
        let chain = match chain_first_run_id {
            Some(first) => chain_of(stream, first),
            None => streams.chain_now(stream).await?,
        };
        let attachment = WorkflowAttachment {
            chain: chain.clone(),
            topic: stream.topic.clone(),
            request_id: request_id.clone(),
            workflow_id: request.workflow_id,
            run_id: request.run_id,
            subscription_id: request.subscription_id,
            idle: !self.standing.load(Ordering::Relaxed),
        };
        match client.attach_workflow(attachment.clone()).await {
            Err(NotifyError::Refused(error)) if attachment.idle => {
                // A server that refuses idle Workflow targets keeps readers standing. A refusal
                // for another cause fails the standing attach too, and the process keeps waits.
                client
                    .attach_workflow(WorkflowAttachment {
                        idle: false,
                        ..attachment
                    })
                    .await
                    .map_err(|_| subscribe_error(NotifyError::Refused(error.clone())))?;
                self.keep_standing(&error);
            }
            other => other.map_err(subscribe_error)?,
        }
        self.attached
            .put(key.clone(), &chain.first_run_id, OWNER_RECHECK);
        Ok(Attach {
            chain,
            request_id,
            key,
            attached: true,
        })
    }

    /// Detaches the reader from the chain it attached to, or from the chain the stream names now
    /// when this process never attached it.
    pub(crate) async fn unsubscribe(
        &self,
        streams: &Streams,
        request: UnsubscribeRequest,
    ) -> StreamResult<UnsubscribeResponse> {
        let stream = address(request.stream.as_ref())?;
        check_subscription(&request.run_id, &request.subscription_id)?;
        let request_id = request_id(
            &request.run_id,
            &request.subscription_id,
            request.generation,
        );
        let client = self.client()?;
        let key = SubscriptionKey::new(stream, &request_id);
        let chain = match self.attached.get(&key) {
            Some(attached) => chain_of(stream, &attached.first_run_id),
            None => streams.chain_now(stream).await?,
        };
        client
            .detach(chain, stream.topic.clone(), request_id)
            .await
            .map_err(subscribe_error)?;
        self.attached.forget(&key);
        Ok(UnsubscribeResponse {})
    }

    /// Waits on the notifier of the chain the stream names. A wait that reached its limit answers
    /// `timed_out`, and the reader's next read checks the store itself, which covers a
    /// notification that a producer never sent.
    pub(crate) async fn wait(
        &self,
        streams: &Streams,
        request: WaitForNotificationRequest,
    ) -> StreamResult<WaitForNotificationResponse> {
        let stream = address(request.stream.as_ref())?;
        let client = self.client()?;
        let chain = streams.chain(stream).await?;
        let poll = client.poll(chain.clone(), stream.topic.clone(), request.after_counter);
        tokio::pin!(poll);
        // The poll starts the notifier, so a record from before it woke no one. One more read
        // once the poll is out finds it.
        let reread = async {
            tokio::time::sleep(REREAD_AFTER).await;
            streams.newest(stream, &chain).await
        };
        let polled = tokio::select! {
            polled = &mut poll => polled,
            newest = reread => match newest {
                Ok((counter, position)) if counter > request.after_counter => {
                    return Ok(WaitForNotificationResponse {
                        counter,
                        position,
                        ..Default::default()
                    });
                }
                _ => poll.await,
            },
        }
        .map_err(subscribe_error)?;
        let Some(polled) = polled else {
            return Ok(WaitForNotificationResponse {
                timed_out: true,
                ..Default::default()
            });
        };
        Ok(WaitForNotificationResponse {
            timed_out: !polled.closed && polled.counter <= request.after_counter,
            counter: polled.counter,
            position: polled.position,
            closed: polled.closed,
            close_result: polled.close_result,
            close_failure: polled.close_failure,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryStore, NotifierPoll, OwnerClient, OwnerDescription, OwnerError, OwnerId, OwnerStatus,
        StreamNotification, StreamService, StreamsOptions,
        proto::{ChainId, StreamFailureKind, StreamOwnerKind},
    };
    use prost::Message;
    use std::sync::Mutex;

    /// The owner's chain is `first_run_id`, which a test moves to a new chain.
    struct Owner(Mutex<String>);

    #[async_trait::async_trait]
    impl OwnerClient for Owner {
        async fn describe(
            &self,
            _: &str,
            _: OwnerId<'_>,
            _: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            let first_run_id = self.0.lock().unwrap().clone();
            Ok(OwnerDescription {
                run_id: first_run_id.clone(),
                first_run_id,
                status: OwnerStatus::Running,
            })
        }
    }

    /// Records attaches and detaches, and answers each with `answer`.
    #[derive(Default)]
    struct Server {
        attached: Mutex<Vec<WorkflowAttachment>>,
        detached: Mutex<Vec<(ChainId, String, String)>>,
        answer: Mutex<Option<NotifyError>>,
        polled: Mutex<Vec<(ChainId, String, i64)>>,
        poll_answer: Mutex<Option<Result<Option<NotifierPoll>, NotifyError>>>,
        /// How long the poll takes before it answers.
        poll_delay: Mutex<Duration>,
        waits: Mutex<Vec<(ChainId, String, String, i64)>>,
        /// The answers of the next waits, in order. A wait past them lands.
        wait_answers: Mutex<Vec<NotifyError>>,
        /// Refuses idle attaches, as a server that keeps readers standing.
        refuse_idle: Mutex<bool>,
        /// Appends a record to this store as the attach lands, as a producer racing the attach.
        append_on_attach: Mutex<Option<Arc<MemoryStore>>>,
        /// The same as the wait lands.
        append_on_wait: Mutex<Option<Arc<MemoryStore>>>,
    }

    #[async_trait::async_trait]
    impl NotifierClient for Server {
        async fn notify(&self, _: StreamNotification) -> Result<(), NotifyError> {
            Ok(())
        }

        async fn attach_workflow(&self, attachment: WorkflowAttachment) -> Result<(), NotifyError> {
            if let Some(error) = self.answer.lock().unwrap().clone() {
                return Err(error);
            }
            if attachment.idle && *self.refuse_idle.lock().unwrap() {
                return Err(NotifyError::Refused(
                    "idle is only allowed with a Nexus callback".to_string(),
                ));
            }
            let store = self.append_on_attach.lock().unwrap().take();
            if let Some(store) = store {
                append_to(&store, &attachment.chain.first_run_id, 1).await;
            }
            self.attached.lock().unwrap().push(attachment);
            Ok(())
        }

        async fn wait_callback(
            &self,
            chain: ChainId,
            topic: String,
            request_id: String,
            after_counter: i64,
        ) -> Result<(), NotifyError> {
            {
                let mut answers = self.wait_answers.lock().unwrap();
                if !answers.is_empty() {
                    return Err(answers.remove(0));
                }
            }
            let store = self.append_on_wait.lock().unwrap().take();
            if let Some(store) = store {
                append_to(&store, &chain.first_run_id, 1).await;
            }
            self.waits
                .lock()
                .unwrap()
                .push((chain, topic, request_id, after_counter));
            Ok(())
        }

        async fn detach(
            &self,
            chain: ChainId,
            topic: String,
            request_id: String,
        ) -> Result<(), NotifyError> {
            self.detached
                .lock()
                .unwrap()
                .push((chain, topic, request_id));
            Ok(())
        }

        async fn poll(
            &self,
            chain: ChainId,
            topic: String,
            after_counter: i64,
        ) -> Result<Option<NotifierPoll>, NotifyError> {
            self.polled
                .lock()
                .unwrap()
                .push((chain, topic, after_counter));
            let delay = *self.poll_delay.lock().unwrap();
            tokio::time::sleep(delay).await;
            self.poll_answer
                .lock()
                .unwrap()
                .clone()
                .expect("the test sets the poll's answer")
        }
    }

    struct Setup {
        service: StreamService,
        store: Arc<MemoryStore>,
        owner: Arc<Owner>,
        server: Arc<Server>,
    }

    fn setup() -> Setup {
        let store = Arc::new(MemoryStore::new());
        let owner = Arc::new(Owner(Mutex::new("chain-1".to_string())));
        let server = Arc::new(Server::default());
        let service = StreamService::new(store.clone(), owner.clone(), StreamsOptions::default())
            .with_subscriptions(server.clone());
        Setup {
            service,
            store,
            owner,
            server,
        }
    }

    fn stream() -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "owner".to_string(),
            run_id: String::new(),
            topic: "tokens".to_string(),
            ..Default::default()
        }
    }

    fn subscribe_request(subscription_id: &str) -> SubscribeRequest {
        SubscribeRequest {
            stream: Some(stream()),
            workflow_id: "reader".to_string(),
            run_id: "reader-run".to_string(),
            subscription_id: subscription_id.to_string(),
            generation: 0,
        }
    }

    async fn subscribe(
        service: &StreamService,
        request: SubscribeRequest,
    ) -> Result<SubscribeResponse, crate::proto::StreamFailure> {
        service
            .call("Subscribe", &request.encode_to_vec())
            .await
            .map(|answer| SubscribeResponse::decode(answer.as_slice()).unwrap())
    }

    fn chain(first_run_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: "owner".to_string(),
            first_run_id: first_run_id.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_full_process_forgets_its_subscriptions_and_attaches_again() {
        let setup = setup();
        let subscriptions = Subscriptions {
            client: Some(setup.server.clone()),
            attached: Remembered::new(2),
            standing: AtomicBool::new(false),
        };
        let streams = Streams::new(
            setup.store.clone(),
            setup.owner.clone(),
            StreamsOptions::default(),
        );
        let subscribe = |id: &str| subscriptions.subscribe(&streams, subscribe_request(id));
        for id in ["1", "2", "3"] {
            assert!(subscribe(id).await.unwrap().attached);
        }
        assert!(!subscribe("3").await.unwrap().attached);
        assert!(
            subscribe("1").await.unwrap().attached,
            "the oldest was forgotten"
        );
        assert_eq!(setup.server.attached.lock().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn a_subscription_attaches_the_workflow_target_once_per_process() {
        let setup = setup();
        let first = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(first.attached);
        assert_eq!(first.chain_first_run_id, "chain-1");
        let again = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(!again.attached, "a later read sends no attach");
        assert_eq!(again.chain_first_run_id, "chain-1");
        let attached = setup.server.attached.lock().unwrap().clone();
        assert_eq!(
            attached,
            [WorkflowAttachment {
                chain: chain("chain-1"),
                topic: "tokens".to_string(),
                request_id: "jetty/reader-run/1".to_string(),
                workflow_id: "reader".to_string(),
                run_id: "reader-run".to_string(),
                subscription_id: "1".to_string(),
                idle: true,
            }]
        );
    }

    #[tokio::test]
    async fn a_new_generation_attaches_again_after_the_notifier_dropped_the_reader() {
        let setup = setup();
        subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        let again = subscribe(
            &setup.service,
            SubscribeRequest {
                generation: 1,
                ..subscribe_request("1")
            },
        )
        .await
        .unwrap();
        assert!(again.attached, "a resubscribe sends a new attach");
        let attached = setup.server.attached.lock().unwrap().clone();
        let ids: Vec<&str> = attached.iter().map(|a| a.request_id.as_str()).collect();
        assert_eq!(ids, ["jetty/reader-run/1", "jetty/reader-run/1/1"]);
        assert!(
            attached.iter().all(|a| a.subscription_id == "1"),
            "the wakes still name the same reader"
        );
    }

    #[tokio::test]
    async fn readers_of_one_run_attach_apart() {
        let setup = setup();
        subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        subscribe(&setup.service, subscribe_request("2"))
            .await
            .unwrap();
        let ids: Vec<String> = setup
            .server
            .attached
            .lock()
            .unwrap()
            .iter()
            .map(|a| a.request_id.clone())
            .collect();
        assert_eq!(ids, ["jetty/reader-run/1", "jetty/reader-run/2"]);
    }

    #[tokio::test]
    async fn a_subscription_attaches_to_the_chain_that_holds_the_workflow_id_now() {
        let setup = setup();
        // A read warms the owner cache with the first chain.
        setup
            .service
            .call(
                "Latest",
                &crate::proto::LatestRequest {
                    stream: Some(stream()),
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        *setup.owner.0.lock().unwrap() = "chain-2".to_string();
        let answer = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert_eq!(answer.chain_first_run_id, "chain-2");
    }

    #[tokio::test]
    async fn a_refused_attach_fails_the_subscription_and_is_not_remembered() {
        let setup = setup();
        *setup.server.answer.lock().unwrap() = Some(NotifyError::Refused(
            "the execution has 32 stream wake sources".to_string(),
        ));
        let error = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Refused as i32);
        *setup.server.answer.lock().unwrap() = None;
        let answer = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(answer.attached, "the next read tries the attach again");
    }

    #[tokio::test]
    async fn an_attach_that_may_land_later_fails_as_storage() {
        let setup = setup();
        *setup.server.answer.lock().unwrap() = Some(NotifyError::Failed("down".to_string()));
        let error = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage as i32);
    }

    #[tokio::test]
    async fn a_subscription_needs_its_reader() {
        let setup = setup();
        for request in [
            SubscribeRequest {
                workflow_id: String::new(),
                ..subscribe_request("1")
            },
            SubscribeRequest {
                run_id: String::new(),
                ..subscribe_request("1")
            },
            subscribe_request(""),
            subscribe_request(&"x".repeat(MAX_SUBSCRIPTION_ID_BYTES + 1)),
            SubscribeRequest {
                stream: None,
                ..subscribe_request("1")
            },
        ] {
            let error = subscribe(&setup.service, request).await.unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument as i32);
        }
        assert!(setup.server.attached.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_subscription_without_a_client_is_unsupported() {
        let service = StreamService::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Owner(Mutex::new("chain-1".to_string()))),
            StreamsOptions::default(),
        );
        let error = subscribe(&service, subscribe_request("1"))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported as i32);
    }

    #[tokio::test]
    async fn an_unsubscribe_detaches_from_the_chain_it_attached_to_and_forgets_it() {
        let setup = setup();
        subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        *setup.owner.0.lock().unwrap() = "chain-2".to_string();
        let unsubscribe = UnsubscribeRequest {
            stream: Some(stream()),
            run_id: "reader-run".to_string(),
            subscription_id: "1".to_string(),
            ..Default::default()
        };
        setup
            .service
            .call("Unsubscribe", &unsubscribe.encode_to_vec())
            .await
            .unwrap();
        assert_eq!(
            setup.server.detached.lock().unwrap().clone(),
            [(
                chain("chain-1"),
                "tokens".to_string(),
                "jetty/reader-run/1".to_string()
            )]
        );
        let answer = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(answer.attached, "a new subscription attaches again");
        assert_eq!(answer.chain_first_run_id, "chain-2");
    }

    #[tokio::test]
    async fn a_workflow_reader_subscribes_to_a_stream_that_an_activity_owns() {
        let setup = setup();
        let activity = StreamAddress {
            owner_kind: StreamOwnerKind::Activity as i32,
            workflow_id: String::new(),
            activity_id: "owner".to_string(),
            ..stream()
        };
        let activity_chain = ChainId {
            namespace: "ns".to_string(),
            first_run_id: "chain-1".to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: "owner".to_string(),
            ..Default::default()
        };
        let request = SubscribeRequest {
            stream: Some(activity.clone()),
            ..subscribe_request("1")
        };
        let answer = subscribe(&setup.service, request).await.unwrap();
        assert!(answer.attached);
        assert_eq!(answer.chain_first_run_id, "chain-1");
        // A Workflow with the same id owns another stream, so its reader attaches apart.
        assert!(
            subscribe(&setup.service, subscribe_request("1"))
                .await
                .unwrap()
                .attached
        );
        let attached = setup.server.attached.lock().unwrap().clone();
        assert_eq!(attached[0].chain, activity_chain);
        assert_eq!(attached[1].chain, chain("chain-1"));
        let unsubscribe = UnsubscribeRequest {
            stream: Some(activity),
            run_id: "reader-run".to_string(),
            subscription_id: "1".to_string(),
            ..Default::default()
        };
        setup
            .service
            .call("Unsubscribe", &unsubscribe.encode_to_vec())
            .await
            .unwrap();
        assert_eq!(setup.server.detached.lock().unwrap()[0].0, activity_chain);
    }

    #[tokio::test]
    async fn a_workflow_reader_subscribes_to_a_stream_that_a_workflow_activity_owns() {
        let setup = setup();
        let tool = |run_id: &str| StreamAddress {
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            run_id: run_id.to_string(),
            activity_id: "1".to_string(),
            ..stream()
        };
        let tool_chain = |run_id: &str| ChainId {
            namespace: "ns".to_string(),
            workflow_id: "owner".to_string(),
            first_run_id: run_id.to_string(),
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            activity_id: "1".to_string(),
        };
        // Activity `1` of two runs, and the Workflow's own stream, are three streams.
        for (address, id) in [(tool("run-a"), "1"), (tool("run-b"), "2")] {
            let request = SubscribeRequest {
                stream: Some(address),
                ..subscribe_request(id)
            };
            let answer = subscribe(&setup.service, request).await.unwrap();
            assert!(answer.attached);
        }
        assert!(
            subscribe(&setup.service, subscribe_request("3"))
                .await
                .unwrap()
                .attached
        );
        let attached = setup.server.attached.lock().unwrap().clone();
        assert_eq!(attached[0].chain, tool_chain("run-a"));
        assert_eq!(attached[1].chain, tool_chain("run-b"));
        assert_eq!(attached[2].chain, chain("chain-1"));
        let unsubscribe = UnsubscribeRequest {
            stream: Some(tool("run-b")),
            run_id: "reader-run".to_string(),
            subscription_id: "2".to_string(),
            ..Default::default()
        };
        setup
            .service
            .call("Unsubscribe", &unsubscribe.encode_to_vec())
            .await
            .unwrap();
        assert_eq!(
            setup.server.detached.lock().unwrap()[0].0,
            tool_chain("run-b")
        );
    }

    async fn follow(service: &StreamService, request: crate::proto::FollowRequest) -> FollowAnswer {
        FollowAnswer::decode(
            service
                .call("Follow", &request.encode_to_vec())
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap()
    }

    type FollowAnswer = crate::proto::FollowResponse;

    fn append_one(value: &str) -> crate::proto::AppendRequest {
        crate::proto::AppendRequest {
            stream: Some(stream()),
            producer: Some(crate::proto::append_request::Producer::Named(
                crate::proto::NamedProducer {
                    producer_id: "p".to_string(),
                    attempt: 1,
                },
            )),
            sequence: 1,
            digest: vec![1; 32],
            records: vec![crate::proto::AppendRecord {
                kind: crate::proto::StreamRecordKind::Data as i32,
                body: Some(Default::default()),
                content_hash: vec![value.as_bytes()[0]; 32],
                link: None,
            }],
        }
    }

    #[tokio::test]
    async fn a_workflow_follower_attaches_each_idle_source_and_answers_its_wake() {
        let setup = setup();
        let request = crate::proto::FollowRequest {
            stream: Some(stream()),
            subscriber: Some(crate::proto::FollowSubscriber {
                workflow_id: "reader".to_string(),
                run_id: "reader-run".to_string(),
                subscription_prefix: "7".to_string(),
                generation: 0,
            }),
            ..Default::default()
        };
        let idle = follow(&setup.service, request.clone()).await;
        assert!(idle.records.is_empty());
        assert_eq!(idle.wakes.len(), 1);
        assert_eq!(idle.wakes[0].source, 0);
        assert_eq!(idle.wakes[0].subscription_id, "7.0");
        let attached = setup.server.attached.lock().unwrap().clone();
        assert_eq!(attached.len(), 1);
        assert_eq!(attached[0].subscription_id, "7.0");
        assert_eq!(
            setup.server.waits.lock().unwrap().clone(),
            [(
                chain("chain-1"),
                "tokens".to_string(),
                "jetty/reader-run/7.0".to_string(),
                0
            )],
            "the idle source asks for one wake after its counter"
        );
        // A record that lands later wakes the run, and the next step reads it.
        setup
            .service
            .call("Append", &append_one("a").encode_to_vec())
            .await
            .unwrap();
        let next = follow(
            &setup.service,
            crate::proto::FollowRequest {
                state: idle.state,
                ..request
            },
        )
        .await;
        assert_eq!(next.records.len(), 1);
        assert!(next.wakes.is_empty());
        assert_eq!(
            setup.server.waits.lock().unwrap().len(),
            1,
            "a step with records asks for no wake"
        );
    }

    fn reader() -> crate::proto::ReadSubscriber {
        crate::proto::ReadSubscriber {
            workflow_id: "reader".to_string(),
            run_id: "reader-run".to_string(),
            subscription_id: "1".to_string(),
            generation: 0,
        }
    }

    fn idle_read(after: &str, state: Vec<u8>) -> ReadRequest {
        ReadRequest {
            stream: Some(stream()),
            after: after.to_string(),
            state,
            subscriber: Some(reader()),
            ..Default::default()
        }
    }

    async fn read(service: &StreamService, request: ReadRequest) -> ReadResponse {
        ReadResponse::decode(
            service
                .call("Read", &request.encode_to_vec())
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn an_idle_read_attaches_once_and_asks_for_one_wake_after_each_empty_answer() {
        let setup = setup();
        let empty = read(&setup.service, idle_read("", vec![])).await;
        assert!(empty.records.is_empty() && !empty.done);
        assert_eq!(empty.wake_after, 0);
        append_records(&setup, 2).await;
        let busy = read(&setup.service, idle_read(&empty.cursor, empty.state)).await;
        assert_eq!(busy.records.len(), 2);
        let idle = read(&setup.service, idle_read(&busy.cursor, busy.state)).await;
        assert!(idle.records.is_empty());
        assert_eq!(idle.wake_after, 2);
        let attached = setup.server.attached.lock().unwrap().clone();
        assert_eq!(attached.len(), 1, "one attach per process");
        assert_eq!(attached[0].request_id, "jetty/reader-run/1");
        let request_id = "jetty/reader-run/1".to_string();
        assert_eq!(
            setup.server.waits.lock().unwrap().clone(),
            [
                (
                    chain("chain-1"),
                    "tokens".to_string(),
                    request_id.clone(),
                    0
                ),
                (chain("chain-1"), "tokens".to_string(), request_id, 2),
            ],
            "one wait per idle answer, none for the answer with records"
        );
    }

    /// Temporal, before the owner started.
    struct NotStarted;

    #[async_trait::async_trait]
    impl OwnerClient for NotStarted {
        async fn describe(
            &self,
            _: &str,
            _: OwnerId<'_>,
            _: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            Err(OwnerError::NotFound("workflow not found".to_string()))
        }
    }

    #[tokio::test]
    async fn an_idle_read_before_the_owner_starts_answers_empty_and_attaches_nothing() {
        let server = Arc::new(Server::default());
        let service = StreamService::new(
            Arc::new(MemoryStore::new()),
            Arc::new(NotStarted),
            StreamsOptions::default(),
        )
        .with_subscriptions(server.clone());
        let mut request = idle_read("", vec![]);
        request.wait = Some(std::time::Duration::from_millis(50).try_into().unwrap());
        let answer = read(&service, request).await;
        assert!(answer.records.is_empty() && !answer.done);
        assert!(server.attached.lock().unwrap().is_empty());
        assert!(server.waits.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_idle_read_of_an_ended_chain_waits_on_that_chain() {
        let setup = setup();
        append_records(&setup, 2).await;
        let busy = read(&setup.service, idle_read("", vec![])).await;
        assert_eq!(busy.chain_first_run_id, "chain-1");
        // The owner's id moves to a new chain before the reader goes idle.
        *setup.owner.0.lock().unwrap() = "chain-2".to_string();
        let idle = read(&setup.service, idle_read(&busy.cursor, busy.state)).await;
        let attached = setup.server.attached.lock().unwrap().clone();
        let waits = setup.server.waits.lock().unwrap().clone();
        assert!(!attached.is_empty() || idle.done);
        for attachment in attached {
            assert_eq!(attachment.chain.first_run_id, idle.chain_first_run_id);
        }
        for (chain, ..) in waits {
            assert_eq!(chain.first_run_id, idle.chain_first_run_id);
        }
    }

    #[tokio::test]
    async fn a_read_without_a_subscriber_or_with_records_asks_for_nothing() {
        let setup = setup();
        let plain = read(
            &setup.service,
            ReadRequest {
                subscriber: None,
                ..idle_read("", vec![])
            },
        )
        .await;
        assert!(plain.records.is_empty());
        append_records(&setup, 1).await;
        read(&setup.service, idle_read("", vec![])).await;
        assert!(setup.server.attached.lock().unwrap().is_empty());
        assert!(setup.server.waits.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_record_that_lands_before_the_first_attach_is_read_at_once() {
        let setup = setup();
        *setup.server.append_on_attach.lock().unwrap() = Some(setup.store.clone());
        let answer = read(&setup.service, idle_read("", vec![])).await;
        assert_eq!(
            answer.records.len(),
            1,
            "the notifier did not exist, so the record woke no one"
        );
        assert_eq!(setup.server.waits.lock().unwrap()[0].3, 0);
    }

    #[tokio::test]
    async fn a_later_idle_read_leaves_a_racing_record_to_the_wait() {
        let setup = setup();
        let first = read(&setup.service, idle_read("", vec![])).await;
        *setup.server.append_on_wait.lock().unwrap() = Some(setup.store.clone());
        // The notifier exists since the attach, so it heard the record, and the wait after
        // counter 0 wakes the Workflow at once.
        let again = read(&setup.service, idle_read(&first.cursor, first.state)).await;
        assert!(again.records.is_empty());
        assert_eq!(setup.server.waits.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_failed_wait_fails_the_read_and_a_retry_waits_again() {
        let setup = setup();
        setup
            .server
            .wait_answers
            .lock()
            .unwrap()
            .push(NotifyError::Failed("down".to_string()));
        let error = setup
            .service
            .call("Read", &idle_read("", vec![]).encode_to_vec())
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage as i32);
        read(&setup.service, idle_read("", vec![])).await;
        assert_eq!(setup.server.attached.lock().unwrap().len(), 1);
        assert_eq!(setup.server.waits.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_lost_wait_attaches_the_reader_again_and_waits_once_more() {
        let setup = setup();
        let first = read(&setup.service, idle_read("", vec![])).await;
        setup
            .server
            .wait_answers
            .lock()
            .unwrap()
            .push(NotifyError::Refused("no such callback".to_string()));
        read(&setup.service, idle_read(&first.cursor, first.state)).await;
        assert_eq!(setup.server.attached.lock().unwrap().len(), 2);
        assert_eq!(
            setup.server.waits.lock().unwrap().len(),
            2,
            "the refused wait is not counted, the one after the attach is"
        );
    }

    #[tokio::test]
    async fn a_server_without_waits_keeps_the_reader_standing_and_gets_no_more_waits() {
        let setup = setup();
        setup
            .server
            .wait_answers
            .lock()
            .unwrap()
            .push(NotifyError::Unsupported("unimplemented".to_string()));
        let first = read(&setup.service, idle_read("", vec![])).await;
        assert!(first.records.is_empty(), "the read still answers");
        read(&setup.service, idle_read(&first.cursor, first.state)).await;
        let fresh = ReadRequest {
            subscriber: Some(crate::proto::ReadSubscriber {
                subscription_id: "2".to_string(),
                ..reader()
            }),
            ..idle_read("", vec![])
        };
        read(&setup.service, fresh).await;
        assert!(setup.server.waits.lock().unwrap().is_empty());
        let attached = setup.server.attached.lock().unwrap().clone();
        assert_eq!(
            attached.iter().map(|a| a.idle).collect::<Vec<_>>(),
            [true, false],
            "later attaches are standing"
        );
    }

    #[tokio::test]
    async fn a_server_that_refuses_idle_readers_gets_a_standing_attach_and_no_waits() {
        let setup = setup();
        *setup.server.refuse_idle.lock().unwrap() = true;
        let first = read(&setup.service, idle_read("", vec![])).await;
        read(&setup.service, idle_read(&first.cursor, first.state)).await;
        let attached = setup.server.attached.lock().unwrap().clone();
        assert_eq!(attached.len(), 1);
        assert!(!attached[0].idle);
        assert!(setup.server.waits.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_reader_refused_for_another_cause_fails_and_the_process_keeps_waits() {
        let setup = setup();
        *setup.server.answer.lock().unwrap() = Some(NotifyError::Refused(
            "the execution has 32 stream wake sources".to_string(),
        ));
        let error = setup
            .service
            .call("Read", &idle_read("", vec![]).encode_to_vec())
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Refused as i32);
        assert!(
            error.message.contains("32 stream wake sources"),
            "{error:?}"
        );
        *setup.server.answer.lock().unwrap() = None;
        read(&setup.service, idle_read("", vec![])).await;
        assert!(setup.server.attached.lock().unwrap()[0].idle);
        assert_eq!(setup.server.waits.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn an_idle_read_names_its_reader() {
        let setup = setup();
        for subscriber in [
            crate::proto::ReadSubscriber {
                workflow_id: String::new(),
                ..reader()
            },
            crate::proto::ReadSubscriber {
                run_id: String::new(),
                ..reader()
            },
            crate::proto::ReadSubscriber {
                subscription_id: String::new(),
                ..reader()
            },
        ] {
            let request = ReadRequest {
                subscriber: Some(subscriber),
                ..idle_read("", vec![])
            };
            let error = setup
                .service
                .call("Read", &request.encode_to_vec())
                .await
                .unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument as i32);
        }
        assert!(setup.server.attached.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_idle_read_without_a_client_is_unsupported() {
        let service = StreamService::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Owner(Mutex::new("chain-1".to_string()))),
            StreamsOptions::default(),
        );
        let error = service
            .call("Read", &idle_read("", vec![]).encode_to_vec())
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported as i32);
    }

    #[tokio::test]
    async fn a_new_generation_attaches_again_and_waits_on_the_new_attach() {
        let setup = setup();
        read(&setup.service, idle_read("", vec![])).await;
        let request = ReadRequest {
            subscriber: Some(crate::proto::ReadSubscriber {
                generation: 1,
                ..reader()
            }),
            ..idle_read("", vec![])
        };
        read(&setup.service, request).await;
        let waits: Vec<String> = setup
            .server
            .waits
            .lock()
            .unwrap()
            .iter()
            .map(|wait| wait.2.clone())
            .collect();
        assert_eq!(waits, ["jetty/reader-run/1", "jetty/reader-run/1/1"]);
    }

    #[tokio::test]
    async fn a_client_follower_waits_for_the_first_record_of_any_source() {
        let setup = setup();
        let append = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            setup
                .service
                .call("Append", &append_one("a").encode_to_vec())
                .await
                .unwrap();
        };
        let request = crate::proto::FollowRequest {
            stream: Some(stream()),
            wait: Duration::from_secs(10).try_into().ok(),
            ..Default::default()
        };
        let started = std::time::Instant::now();
        let (answer, ()) = tokio::join!(follow(&setup.service, request), append);
        assert_eq!(answer.records.len(), 1);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    async fn wait(
        setup: &Setup,
        after_counter: i64,
        answer: Result<Option<NotifierPoll>, NotifyError>,
    ) -> Result<WaitForNotificationResponse, crate::proto::StreamFailure> {
        *setup.server.poll_answer.lock().unwrap() = Some(answer);
        setup
            .service
            .call(
                "WaitForNotification",
                &WaitForNotificationRequest {
                    stream: Some(stream()),
                    after_counter,
                }
                .encode_to_vec(),
            )
            .await
            .map(|answer| WaitForNotificationResponse::decode(answer.as_slice()).unwrap())
    }

    /// Appends `count` records to the chain the owner names now.
    async fn append_records(setup: &Setup, count: i64) {
        let first_run_id = setup.owner.0.lock().unwrap().clone();
        append_to(&setup.store, &first_run_id, count).await;
    }

    /// Appends `count` records to the stream on the chain that `first_run_id` starts.
    async fn append_to(store: &MemoryStore, first_run_id: &str, count: i64) {
        use crate::StreamStore;
        let first_run_id = first_run_id.to_string();
        store
            .append(crate::proto::StoreAppendRequest {
                chain: Some(ChainId {
                    namespace: "ns".to_string(),
                    workflow_id: "owner".to_string(),
                    first_run_id,
                    ..Default::default()
                }),
                topic: "tokens".to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence: 1,
                digest: vec![1],
                records: (0..count)
                    .map(|index| {
                        crate::proto::StreamRecord {
                            topic: "tokens".to_string(),
                            kind: crate::proto::StreamRecordKind::Data as i32,
                            producer_id: "p".to_string(),
                            attempt: 1,
                            sequence: index + 1,
                            ..Default::default()
                        }
                        .encode_to_vec()
                    })
                    .collect(),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_subscription_names_the_newest_counter_it_found_after_the_attach() {
        let setup = setup();
        let empty = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert_eq!(empty.newest_counter, 0, "an empty stream");
        append_records(&setup, 2).await;
        // A record from before the attach woke no one, so the reader reads once more.
        let again = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(!again.attached);
        assert_eq!(again.newest_counter, 2);
        let fresh = subscribe(&setup.service, subscribe_request("2"))
            .await
            .unwrap();
        assert!(fresh.attached);
        assert_eq!(fresh.newest_counter, 2);
    }

    #[tokio::test]
    async fn a_wait_answers_at_once_for_a_record_from_before_the_poll() {
        let setup = setup();
        append_records(&setup, 1).await;
        *setup.server.poll_delay.lock().unwrap() = Duration::from_secs(30);
        let started = tokio::time::Instant::now();
        let answer = wait(&setup, 0, Ok(None)).await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(!answer.timed_out && !answer.closed);
        assert_eq!(answer.counter, 1);
        assert!(answer.position.starts_with("memory:"), "{answer:?}");
    }

    #[tokio::test]
    async fn a_wait_with_nothing_newer_waits_for_the_poll() {
        let setup = setup();
        append_records(&setup, 1).await;
        *setup.server.poll_delay.lock().unwrap() = Duration::from_millis(400);
        let answer = wait(
            &setup,
            1,
            Ok(Some(NotifierPoll {
                counter: 2,
                position: "cursor-2".to_string(),
                ..Default::default()
            })),
        )
        .await
        .unwrap();
        assert_eq!((answer.counter, answer.position.as_str()), (2, "cursor-2"));
    }

    #[tokio::test]
    async fn a_wait_asks_the_notifier_of_the_streams_chain_for_a_counter_above_the_read() {
        let setup = setup();
        let answer = wait(
            &setup,
            41,
            Ok(Some(NotifierPoll {
                counter: 42,
                position: "cursor-42".to_string(),
                ..Default::default()
            })),
        )
        .await
        .unwrap();
        assert_eq!(
            setup.server.polled.lock().unwrap().clone(),
            [(chain("chain-1"), "tokens".to_string(), 41)]
        );
        assert!(!answer.timed_out && !answer.closed);
        assert_eq!(
            (answer.counter, answer.position.as_str()),
            (42, "cursor-42")
        );
    }

    #[tokio::test]
    async fn a_wait_with_no_new_counter_times_out_for_a_re_read() {
        let setup = setup();
        let unchanged = wait(
            &setup,
            41,
            Ok(Some(NotifierPoll {
                counter: 41,
                ..Default::default()
            })),
        )
        .await
        .unwrap();
        assert!(unchanged.timed_out);
        let deadline = wait(&setup, 41, Ok(None)).await.unwrap();
        assert!(deadline.timed_out && !deadline.closed);
    }

    #[tokio::test]
    async fn a_wait_on_a_closed_stream_carries_the_outcome() {
        let setup = setup();
        let failure = temporalio_common::protos::temporal::api::failure::v1::Failure {
            message: "the agent gave up".to_string(),
            ..Default::default()
        };
        let answer = wait(
            &setup,
            41,
            Ok(Some(NotifierPoll {
                counter: 41,
                closed: true,
                close_failure: Some(failure.clone()),
                ..Default::default()
            })),
        )
        .await
        .unwrap();
        assert!(answer.closed && !answer.timed_out);
        assert_eq!(answer.close_failure, Some(failure));
    }

    #[tokio::test]
    async fn a_wait_on_a_server_without_the_poll_is_unsupported() {
        let setup = setup();
        let error = wait(
            &setup,
            0,
            Err(NotifyError::Unsupported("unimplemented".to_string())),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported as i32);
    }
}
