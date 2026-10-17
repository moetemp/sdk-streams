//! Connections: one multiplexed connection for every short call, and dedicated ones for blocking
//! reads.
//!
//! redis-rs sends every command of a multiplexed connection down one socket in order, so a
//! blocking `XREAD` there would stall every append behind it. A blocking read takes a connection
//! of its own from a bounded pool per node instead. A read that is cancelled drops its connection,
//! since the server may still answer the blocked command on it.

use redis::{
    AsyncConnectionConfig, Client, Cmd, ConnectionAddr, ConnectionInfo, IntoConnectionInfo,
    Pipeline, RedisError, RedisFuture, RedisResult, Value,
    aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
    cluster::ClusterClient,
    cluster_async::ClusterConnection,
    cluster_routing::{MultipleNodeRoutingInfo, RoutingInfo, SingleNodeRoutingInfo},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// The connection every short call shares.
#[derive(Clone)]
pub(crate) enum Shared {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

impl ConnectionLike for Shared {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            Shared::Single(conn) => conn.req_packed_command(cmd),
            Shared::Cluster(conn) => conn.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            Shared::Single(conn) => conn.req_packed_commands(pipeline, offset, count),
            Shared::Cluster(conn) => conn.req_packed_commands(pipeline, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Shared::Single(conn) => conn.get_db(),
            Shared::Cluster(conn) => conn.get_db(),
        }
    }
}

impl Shared {
    pub(crate) async fn connect(
        urls: &[String],
        cluster: bool,
        response_timeout: Duration,
    ) -> RedisResult<Self> {
        if cluster {
            let client = ClusterClient::builder(urls.iter().map(String::as_str))
                .response_timeout(response_timeout)
                .build()?;
            return Ok(Shared::Cluster(client.get_async_connection().await?));
        }
        let client = Client::open(urls[0].as_str())?;
        let config = ConnectionManagerConfig::new().set_response_timeout(Some(response_timeout));
        Ok(Shared::Single(
            client.get_connection_manager_with_config(config).await?,
        ))
    }

    /// The primaries a scan must visit: the server itself, or each primary of a cluster.
    pub(crate) async fn primaries(&self) -> RedisResult<Vec<Option<Node>>> {
        if let Shared::Single(_) = self {
            return Ok(vec![None]);
        }
        Ok(self
            .on_each_primary(redis::cmd("PING"))
            .await?
            .into_iter()
            .map(|(node, _)| {
                node.rsplit_once(':')
                    .and_then(|(host, port)| Some((host.to_string(), port.parse().ok()?)))
            })
            .collect())
    }

    /// One `SCAN` step on one primary, answering the next cursor and the keys found.
    pub(crate) async fn scan(
        &self,
        node: Option<&Node>,
        cursor: &str,
        pattern: &str,
    ) -> RedisResult<(String, Vec<String>)> {
        let mut scan = redis::cmd("SCAN");
        scan.arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(1000);
        match (self.clone(), node) {
            (Shared::Cluster(mut conn), Some((host, port))) => {
                let routing = RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                    host: host.clone(),
                    port: *port,
                });
                redis::from_redis_value(conn.route_command(scan, routing).await?)
                    .map_err(RedisError::from)
            }
            (mut shared, _) => scan.query_async(&mut shared).await,
        }
    }

    /// Runs `pipeline`, which holds `count` commands that all answer, on `node`, or on the server
    /// when the store talks to one.
    pub(crate) async fn query_pipeline_on<T: redis::FromRedisValue>(
        &self,
        node: Option<&Node>,
        pipeline: Pipeline,
        count: usize,
    ) -> RedisResult<T> {
        match (self.clone(), node) {
            (Shared::Cluster(mut conn), Some((host, port))) => {
                let routing = SingleNodeRoutingInfo::ByAddress {
                    host: host.clone(),
                    port: *port,
                };
                let replies = conn.route_pipeline(pipeline, 0, count, routing).await?;
                redis::from_redis_value(Value::Array(replies)).map_err(RedisError::from)
            }
            (mut shared, _) => pipeline.query_async(&mut shared).await,
        }
    }

    /// Runs `cmd` on the server, or on every primary of a cluster, and answers each reply with
    /// the node that sent it. Each primary holds its own share of the keys and its own settings.
    pub(crate) async fn on_each_primary(&self, cmd: Cmd) -> RedisResult<Vec<(String, Value)>> {
        match self.clone() {
            Shared::Single(mut conn) => {
                Ok(vec![(String::new(), cmd.query_async(&mut conn).await?)])
            }
            Shared::Cluster(mut conn) => {
                let routing = RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllMasters, None));
                match conn.route_command(cmd, routing).await? {
                    Value::Map(replies) => Ok(replies
                        .into_iter()
                        .map(|(node, reply)| {
                            let node = match node {
                                Value::BulkString(bytes) => {
                                    String::from_utf8_lossy(&bytes).into_owned()
                                }
                                Value::SimpleString(text) => text,
                                other => format!("{other:?}"),
                            };
                            (node, reply)
                        })
                        .collect()),
                    other => Ok(vec![(String::new(), other)]),
                }
            }
        }
    }
}

/// The node a key lives on, for a store that talks to one.
pub(crate) type Node = (String, u16);

struct NodePool {
    info: ConnectionInfo,
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<MultiplexedConnection>>,
}

/// Dedicated connections for blocking reads, a bounded number per node.
pub(crate) struct BlockingPool {
    seed: ConnectionInfo,
    per_node: usize,
    cluster: bool,
    /// How long past its own deadline a blocked read waits for the server's answer. A server
    /// that died without a reset never answers, and the read must still end.
    response_timeout: Duration,
    nodes: Mutex<HashMap<Node, Arc<NodePool>>>,
    /// The node that serves each slot, learned from redirects. A cluster moves slots rarely, and
    /// a stale entry costs one redirect.
    slots: Mutex<HashMap<u16, Node>>,
}

/// A dedicated connection, returned to its pool only when the call on it finished.
struct Lease {
    pool: Arc<NodePool>,
    conn: Option<MultiplexedConnection>,
    /// Taken idle from the pool, so the server may have dropped it since its last use.
    reused: bool,
    _permit: OwnedSemaphorePermit,
}

impl Lease {
    fn release(mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.idle.lock().unwrap().push(conn);
        }
    }
}

impl BlockingPool {
    pub(crate) fn new(
        seed: &str,
        per_node: usize,
        cluster: bool,
        response_timeout: Duration,
    ) -> RedisResult<Self> {
        Ok(Self {
            seed: seed.into_connection_info()?,
            per_node: per_node.max(1),
            cluster,
            response_timeout,
            nodes: Mutex::default(),
            slots: Mutex::default(),
        })
    }

    /// Runs `cmd` on a dedicated connection to the node that holds `key`.
    ///
    /// Returns `None` when no connection came free before `deadline`, which the caller treats as
    /// a wait in which nothing arrived.
    pub(crate) async fn query(
        &self,
        key: &str,
        cmd: &Cmd,
        deadline: Instant,
    ) -> RedisResult<Option<Value>> {
        let slot = self.cluster.then(|| slot(key));
        let cached = slot.and_then(|slot| self.slots.lock().unwrap().get(&slot).cloned());
        let mut from_cache = cached.is_some();
        let mut node = cached.unwrap_or_else(|| address(&self.seed));
        // One redirect is expected when the slot was unknown or moved. A second means the
        // cluster is resharding, and the caller's next read tries again.
        let mut redirected = false;
        let mut fresh = false;
        loop {
            let mut lease = match self.lease(&node, deadline, fresh).await {
                Ok(Some(lease)) => lease,
                Ok(None) => return Ok(None),
                Err(error) if from_cache && unreachable(&error) => {
                    node = self.forget(slot, &node);
                    from_cache = false;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let conn = lease.conn.as_mut().expect("a lease holds its connection");
            let answer = tokio::time::timeout_at(
                deadline + self.response_timeout,
                cmd.query_async::<Value>(conn),
            )
            .await;
            // Dropping the lease drops the connection, which may still get the late answer.
            let Ok(result) = answer else {
                return Err(RedisError::from((
                    redis::ErrorKind::Io,
                    "Redis did not answer a blocked read by its deadline",
                )));
            };
            match result {
                Ok(value) => {
                    lease.release();
                    return Ok(Some(value));
                }
                // A restart drops every idle connection, so one fresh try tells a dead connection
                // from a dead server.
                Err(error)
                    if lease.reused
                        && !fresh
                        && (error.is_io_error() || error.is_connection_dropped()) =>
                {
                    fresh = true;
                }
                Err(error) if from_cache && unreachable(&error) => {
                    node = self.forget(slot, &node);
                    from_cache = false;
                }
                Err(error) => match (slot, redirect(&error)) {
                    (Some(slot), Some(target)) if !redirected => {
                        lease.release();
                        self.slots.lock().unwrap().insert(slot, target.clone());
                        node = target;
                        redirected = true;
                    }
                    (Some(_), Some(_)) => {
                        return Err(RedisError::from((
                            redis::ErrorKind::Io,
                            "the cluster moved the stream's slot twice during one read",
                        )));
                    }
                    _ => return Err(error),
                },
            }
        }
    }

    /// Drops a slot's cached node and answers the seed. A failover can replace a node's
    /// address for good, and only a redirect would ever update the entry otherwise.
    fn forget(&self, slot: Option<u16>, node: &Node) -> Node {
        if let Some(slot) = slot {
            let mut slots = self.slots.lock().unwrap();
            if slots.get(&slot) == Some(node) {
                slots.remove(&slot);
            }
        }
        address(&self.seed)
    }

    async fn lease(
        &self,
        node: &Node,
        deadline: Instant,
        fresh: bool,
    ) -> RedisResult<Option<Lease>> {
        let pool = self.node(node);
        let Ok(Ok(permit)) =
            tokio::time::timeout_at(deadline, pool.permits.clone().acquire_owned()).await
        else {
            return Ok(None);
        };
        let idle = if fresh {
            None
        } else {
            pool.idle.lock().unwrap().pop()
        };
        let reused = idle.is_some();
        let conn = match idle {
            Some(conn) => conn,
            None => {
                // The read bounds the wait itself, so the connection sets no response timeout.
                let config = AsyncConnectionConfig::new().set_response_timeout(None);
                Client::open(pool.info.clone())?
                    .get_multiplexed_async_connection_with_config(&config)
                    .await?
            }
        };
        Ok(Some(Lease {
            pool,
            conn: Some(conn),
            reused,
            _permit: permit,
        }))
    }

    fn node(&self, node: &Node) -> Arc<NodePool> {
        self.nodes
            .lock()
            .unwrap()
            .entry(node.clone())
            .or_insert_with(|| {
                Arc::new(NodePool {
                    info: at(&self.seed, node),
                    permits: Arc::new(Semaphore::new(self.per_node)),
                    idle: Mutex::default(),
                })
            })
            .clone()
    }

    /// How many dedicated connections are open or in use, for tests.
    #[cfg(test)]
    pub(crate) fn in_use(&self) -> usize {
        self.nodes
            .lock()
            .unwrap()
            .values()
            .map(|pool| self.per_node - pool.permits.available_permits())
            .sum()
    }
}

fn address(info: &ConnectionInfo) -> Node {
    match info.addr() {
        ConnectionAddr::Tcp(host, port) | ConnectionAddr::TcpTls { host, port, .. } => {
            (host.clone(), *port)
        }
        ConnectionAddr::Unix(path) => (path.display().to_string(), 0),
        _ => (String::new(), 0),
    }
}

/// The seed's settings, credentials and TLS included, at another node.
fn at(seed: &ConnectionInfo, (host, port): &Node) -> ConnectionInfo {
    let addr = match seed.addr() {
        ConnectionAddr::TcpTls {
            insecure,
            tls_params,
            ..
        } => ConnectionAddr::TcpTls {
            host: host.clone(),
            port: *port,
            insecure: *insecure,
            tls_params: tls_params.clone(),
        },
        ConnectionAddr::Tcp(..) => ConnectionAddr::Tcp(host.clone(), *port),
        other => other.clone(),
    };
    seed.clone().set_addr(addr)
}

fn unreachable(error: &RedisError) -> bool {
    error.is_io_error() || error.is_connection_refusal() || error.is_connection_dropped()
}

fn redirect(error: &RedisError) -> Option<Node> {
    if error.kind() != redis::ErrorKind::Server(redis::ServerErrorKind::Moved) {
        return None;
    }
    error
        .redirect_node()
        .and_then(|(address, _)| address.rsplit_once(':'))
        .and_then(|(host, port)| Some((host.to_string(), port.parse().ok()?)))
}

/// The cluster slot of a key: CRC16 of its hash tag, or of the whole key without one.
pub(crate) fn slot(key: &str) -> u16 {
    let bytes = key.as_bytes();
    let tagged = bytes.iter().position(|&b| b == b'{').and_then(|open| {
        let close = bytes[open + 1..].iter().position(|&b| b == b'}')?;
        (close > 0).then(|| &bytes[open + 1..open + 1 + close])
    });
    crc16::State::<crc16::XMODEM>::calculate(tagged.unwrap_or(bytes)) % 16384
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_is_taken_over_the_hash_tag() {
        // Values from `CLUSTER KEYSLOT`.
        assert_eq!(slot("foo"), 12182);
        assert_eq!(slot("{user1000}.following"), slot("{user1000}.followers"));
        assert_eq!(slot("{user1000}.following"), slot("user1000"));
        // An empty tag hashes the whole key.
        assert_ne!(slot("foo{}{bar}"), slot("bar"));
        assert_eq!(slot("p:{ns:wf:run}:t:a"), slot("p:{ns:wf:run}:chain"));
    }

    #[test]
    fn rule_8_4_only_a_moved_slot_is_remembered() {
        let reply = |text: &str| {
            redis::parse_redis_value(format!("-{text}\r\n").as_bytes())
                .unwrap()
                .extract_error()
                .unwrap_err()
        };
        assert_eq!(
            redirect(&reply("MOVED 3999 127.0.0.1:7102")),
            Some(("127.0.0.1".to_string(), 7102))
        );
        // An `ASK` target holds the slot only for one command sent after `ASKING`.
        assert_eq!(redirect(&reply("ASK 3999 127.0.0.1:7102")), None);
    }

    #[tokio::test]
    async fn a_cached_slot_on_a_dead_node_goes_back_to_the_seed() {
        let Ok(url) = std::env::var("STREAMS_REDIS_URL") else {
            eprintln!("set STREAMS_REDIS_URL to run this test");
            return;
        };
        // A standalone server answers every slot, so the seed serves the read once the dead
        // node is dropped.
        let pool = BlockingPool::new(&url, 1, true, Duration::from_secs(1)).unwrap();
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = dead.local_addr().unwrap().port();
        drop(dead);
        let key = "x-core:{dead-node}:log";
        pool.slots
            .lock()
            .unwrap()
            .insert(slot(key), ("127.0.0.1".to_string(), port));
        let deadline = Instant::now() + Duration::from_secs(2);
        let answer = pool.query(key, &redis::cmd("PING"), deadline).await;
        assert_eq!(
            answer.unwrap(),
            Some(Value::SimpleString("PONG".to_string()))
        );
        assert!(pool.slots.lock().unwrap().is_empty());
    }

    #[test]
    fn another_node_keeps_the_seed_credentials() {
        let seed = "redis://user:secret@127.0.0.1:7101/0"
            .into_connection_info()
            .unwrap();
        let other = at(&seed, &("10.0.0.2".to_string(), 7102));
        assert_eq!(address(&other), ("10.0.0.2".to_string(), 7102));
        let settings = |info: &ConnectionInfo| {
            let redis = info.redis_settings();
            (
                redis.username().map(str::to_string),
                redis.password().map(str::to_string),
                redis.db(),
            )
        };
        assert_eq!(settings(&other), settings(&seed));
        assert_eq!(settings(&other).1.as_deref(), Some("secret"));
    }
}
