//! Connection lifecycle: WebSocket transport, Noise handshake, read loop,
//! keepalive, and auto-reconnect.
//!
//! The connection layer is split into three parts:
//! - `Connection` - Factory that creates a runner and handle pair.
//! - `ConnectionRunner` - Owns the read loop, keepalive, and reconnect logic.
//! - `ConnectionHandle` - Handle for sending nodes and IQs.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_broadcast::broadcast;
use async_channel::unbounded;
use async_lock::Mutex;
use futures_channel::oneshot;
use futures_timer::Delay;
use futures_util::future::{Either, select};
use wacore::{
    iq::spec::IqSpec,
    net::{Transport, TransportEvent},
    request::{InfoQuery, InfoQueryType},
    store::traits::Backend,
};
use wacore_binary::{
    Node,
    node::{Attrs, NodeValue},
};

use crate::{ConnectionError, NoiseSocket, Result, frame};

/// Configuration for the WebSocket connection layer.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// WebSocket endpoint URL.
    pub websocket_url: String,
    /// Whether to automatically reconnect on disconnect.
    pub auto_reconnect: bool,
    /// Interval between keepalive pings.
    pub keepalive_interval: Duration,
    /// Maximum number of reconnect attempts (0 = unlimited).
    pub max_reconnect_attempts: u32,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            websocket_url: "wss://web.whatsapp.com/ws/chat".into(),
            auto_reconnect: true,
            keepalive_interval: Duration::from_secs(20),
            max_reconnect_attempts: 10,
        }
    }
}

/// Raw event from the connection layer, before high-level processing.
#[derive(Clone, Debug)]
pub enum RawEvent {
    /// A decoded protocol node received from the server.
    Node(Node),
    /// The WebSocket connection was established (or re-established).
    Connected,
    /// The WebSocket connection was lost.
    Disconnected,
    /// A connection-level error occurred.
    Error(String),
}

#[allow(dead_code)]
pub(crate) enum ConnectionCommand {
    SendNode(Node, oneshot::Sender<Result<()>>),
    Disconnect,
}

/// Factory for creating a [`ConnectionRunner`] and [`ConnectionHandle`] pair.
pub struct Connection {
    #[allow(dead_code)]
    backend: Arc<dyn Backend>,
    #[allow(dead_code)]
    config: ConnectionConfig,
}

/// Owns the connection read loop, keepalive, and auto-reconnect logic.
///
/// Created by [`Connection::new`]. Call [`run`](Self::run) to start the
/// connection lifecycle. The runner consumes itself on `run`.
#[allow(dead_code)]
pub struct ConnectionRunner {
    cmd_rx: async_channel::Receiver<ConnectionCommand>,
    event_tx: async_broadcast::Sender<RawEvent>,
    /// Keeps the broadcast channel alive so events aren't lost when no
    /// external receivers exist yet. Never read from.
    #[allow(dead_code)]
    event_rx: async_broadcast::Receiver<RawEvent>,
    backend: Arc<dyn Backend>,
    config: ConnectionConfig,
    transport: Arc<Mutex<Option<Arc<dyn Transport>>>>,
    noise_socket: Arc<Mutex<Option<Arc<NoiseSocket>>>>,
    transport_events: Arc<Mutex<Option<async_channel::Receiver<TransportEvent>>>>,
    is_connected: Arc<AtomicBool>,
    iq_waiters: Arc<Mutex<HashMap<String, oneshot::Sender<Result<Node>>>>>,
    #[allow(dead_code)]
    iq_id_counter: Arc<AtomicU64>,
    /// Set to true when the server sends a `<failure>` or `<stream:error>` node,
    /// indicating the user logged out from their phone. Prevents reconnection loops.
    logged_out: Arc<AtomicBool>,
}

/// Handle for sending nodes and IQs through the connection.
#[derive(Clone)]
pub struct ConnectionHandle {
    cmd_tx: async_channel::Sender<ConnectionCommand>,
    is_connected: Arc<AtomicBool>,
    iq_waiters: Arc<Mutex<HashMap<String, oneshot::Sender<Result<Node>>>>>,
    iq_id_counter: Arc<AtomicU64>,
}

impl Connection {
    /// Create a new connection, returning the runner, event sender, and handle.
    ///
    /// The runner should be spawned (e.g. via `tokio::spawn`) to drive the
    /// connection lifecycle. The event sender can be used to create new
    /// receivers via [`async_broadcast::Sender::new_receiver`]. The handle is
    /// used to send nodes and IQs.
    #[allow(clippy::new_ret_no_self)]
    #[tracing::instrument(skip(backend))]
    pub fn new(
        backend: Arc<dyn Backend>,
        config: ConnectionConfig,
    ) -> (
        ConnectionRunner,
        async_broadcast::Sender<RawEvent>,
        ConnectionHandle,
    ) {
        let (cmd_tx, cmd_rx) = unbounded();
        let (event_tx, event_rx) = broadcast(256);
        let is_connected = Arc::new(AtomicBool::new(false));
        let transport = Arc::new(Mutex::new(None));
        let noise_socket = Arc::new(Mutex::new(None));
        let transport_events = Arc::new(Mutex::new(None));
        let iq_waiters = Arc::new(Mutex::new(HashMap::new()));
        let iq_id_counter = Arc::new(AtomicU64::new(0));
        let logged_out = Arc::new(AtomicBool::new(false));

        let runner = ConnectionRunner {
            cmd_rx,
            event_tx: event_tx.clone(),
            event_rx,
            backend: Arc::clone(&backend),
            config: config.clone(),
            transport: Arc::clone(&transport),
            noise_socket: Arc::clone(&noise_socket),
            transport_events: Arc::clone(&transport_events),
            is_connected: Arc::clone(&is_connected),
            iq_waiters: Arc::clone(&iq_waiters),
            iq_id_counter: Arc::clone(&iq_id_counter),
            logged_out: Arc::clone(&logged_out),
        };

        let handle = ConnectionHandle {
            cmd_tx,
            is_connected: Arc::clone(&is_connected),
            iq_waiters,
            iq_id_counter,
        };

        (runner, event_tx, handle)
    }
}

impl ConnectionRunner {
    /// Run the connection lifecycle: connect, read loop, keepalive, reconnect.
    ///
    /// Consumes `self`. Returns `Ok(())` on clean disconnect, or `Err` if
    /// the connection failed and auto-reconnect is disabled.
    #[tracing::instrument(skip(self))]
    pub async fn run(self) -> Result<()> {
        let fields = frame::RunnerFields {
            cmd_rx: self.cmd_rx,
            event_tx: self.event_tx,
            backend: self.backend,
            config: self.config,
            transport: self.transport,
            noise_socket: self.noise_socket,
            transport_events: self.transport_events,
            is_connected: self.is_connected,
            iq_waiters: self.iq_waiters,
            logged_out: self.logged_out,
        };

        let mut reconnect_attempt = 0u64;

        loop {
            if fields.logged_out.load(Ordering::SeqCst) {
                tracing::info!("logged out, stopping connection runner");
                return Ok(());
            }

            match frame::connect_once(&fields).await {
                Ok(()) => {
                    reconnect_attempt = 0;
                    tracing::info!("connected successfully");
                }
                Err(e) => {
                    tracing::warn!("Connect failed: {e:#}");
                    if !fields.config.auto_reconnect || fields.logged_out.load(Ordering::SeqCst) {
                        let _ = fields.event_tx.try_broadcast(RawEvent::Error(format!(
                            "connect failed and auto_reconnect is disabled: {e}"
                        )));
                        return Err(e);
                    }

                    let delay = frame::fibonacci_backoff(reconnect_attempt);
                    reconnect_attempt = reconnect_attempt.saturating_add(1);

                    tracing::info!("reconnecting in {delay:?} (attempt {reconnect_attempt})");
                    Delay::new(delay).await;
                    continue;
                }
            }

            let loop_result = frame::read_loop(&fields).await;

            fields.is_connected.store(false, Ordering::SeqCst);
            frame::cleanup_connection(&fields).await;

            match loop_result {
                Ok(()) => {
                    let _ = fields.event_tx.try_broadcast(RawEvent::Disconnected);
                    return Ok(());
                }
                Err(e) => {
                    let _ = fields.event_tx.try_broadcast(RawEvent::Disconnected);
                    if !fields.config.auto_reconnect || fields.logged_out.load(Ordering::SeqCst) {
                        let _ = fields
                            .event_tx
                            .try_broadcast(RawEvent::Error(format!("disconnected: {e}")));
                        return Err(e);
                    }

                    let delay = frame::fibonacci_backoff(reconnect_attempt);
                    reconnect_attempt = reconnect_attempt.saturating_add(1);

                    tracing::info!("reconnecting in {delay:?} (attempt {reconnect_attempt})");
                    Delay::new(delay).await;
                }
            }
        }
    }
}

impl ConnectionHandle {
    /// Send a protocol node to the server through the encrypted connection.
    ///
    /// Routes through the command channel so the read loop processes incoming
    /// transport events (e.g. pair-device SET IQ) before sending outgoing IQs.
    pub async fn send_node(&self, node: Node) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.cmd_tx
            .send(ConnectionCommand::SendNode(node, response_tx))
            .await
            .map_err(|_| ConnectionError::NotConnected)?;

        timeout(Duration::from_secs(30), response_rx)
            .await
            .map_err(|_| ConnectionError::Protocol("send_node timed out".into()))?
            .map_err(|_| ConnectionError::NotConnected)?
    }

    /// Send an IQ (Info Query) and await the parsed response.
    ///
    /// The `IqSpec` trait handles building the request node and parsing
    /// the response. Times out after the spec's configured timeout (default 30s).
    pub async fn send_iq<S: IqSpec>(&self, iq: S) -> Result<S::Response> {
        let info_query = iq.build_iq();
        let iq_id = info_query.id.clone().unwrap_or_else(|| self.next_iq_id());
        let iq_node = build_iq_node(&info_query, &iq_id);
        tracing::trace!(
            iq_id = %iq_id,
            tag = %iq_node.tag,
            attrs = ?iq_node.attrs.iter().collect::<Vec<_>>(),
            child_tags = ?iq_node.children().map(|c| c.iter().map(|n| n.tag.as_ref()).collect::<Vec<_>>()),
            "sending IQ node"
        );

        let (response_tx, response_rx) = oneshot::channel();

        {
            let mut waiters = self.iq_waiters.lock().await;
            waiters.insert(iq_id.clone(), response_tx);
        }

        self.send_node(iq_node).await?;

        let timeout_dur = info_query.timeout.unwrap_or(Duration::from_secs(30));
        let response_node = timeout(timeout_dur, response_rx)
            .await
            .map_err(|_| ConnectionError::Protocol("IQ timed out".into()))?
            .map_err(|_| ConnectionError::NotConnected)??;

        let node_ref = response_node.as_node_ref();

        if let Some(type_attr) = node_ref.get_attr("type")
            && type_attr.as_str() == "error"
        {
            let error_text = node_ref
                .get_optional_child_by_tag(&["error"])
                .and_then(|e| e.get_attr("text"))
                .map_or_else(
                    || {
                        let code = node_ref
                            .get_optional_child_by_tag(&["error"])
                            .and_then(|e| e.get_attr("code"))
                            .map_or_else(
                                || "unknown error".to_string(),
                                |c| c.as_str().to_string(),
                            );
                        format!("error code: {code}")
                    },
                    |t| t.as_str().to_string(),
                );
            tracing::warn!(id = %iq_id, error = %error_text, "IQ error response");

            return Err(ConnectionError::Protocol(format!("IQ error: {error_text}")));
        }

        iq.parse_response(&node_ref)
            .map_err(|e| ConnectionError::Protocol(format!("IQ parse error: {e}")))
    }

    /// Whether the connection is currently established.
    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    /// Send a disconnect command to the connection runner.
    pub async fn disconnect(&self) -> Result<()> {
        self.cmd_tx
            .send(ConnectionCommand::Disconnect)
            .await
            .map_err(|_| ConnectionError::Protocol("connection runner dropped".into()))?;

        Ok(())
    }

    fn next_iq_id(&self) -> String {
        let id = self.iq_id_counter.fetch_add(1, Ordering::Relaxed);
        format!("waepic_{id}")
    }
}

/// Build an `<iq>` Node from an `InfoQuery` and an ID string.
fn build_iq_node(info_query: &InfoQuery<'_>, id: &str) -> Node {
    let type_str = match info_query.query_type {
        InfoQueryType::Get => "get",
        InfoQueryType::Set => "set",
    };

    let mut attrs = Attrs::with_capacity(4);
    attrs.push("id", NodeValue::String(id.into()));
    attrs.push("xmlns", NodeValue::String(info_query.namespace.into()));
    attrs.push("type", NodeValue::String(type_str.into()));
    attrs.push("to", NodeValue::Jid(info_query.to.clone()));

    Node::new("iq", attrs, info_query.content.clone())
}

/// Timeout helper using `futures_timer::Delay`.
pub(crate) async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output> {
    let timer = Delay::new(duration);
    let future = Box::pin(future);
    futures_util::pin_mut!(timer);

    match select(future, timer).await {
        Either::Left((result, _)) => Ok(result),
        Either::Right(_) => Err(ConnectionError::Socket("operation timed out".into())),
    }
}
