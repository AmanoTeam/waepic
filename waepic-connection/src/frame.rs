//! Frame processing: decryption, node parsing, IQ routing, and keepalive.
//!
//! These functions are called from [`ConnectionRunner::run`] to drive the
//! connection lifecycle. They are split out to keep the runner focused on
//! the top-level reconnect loop.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_broadcast::Sender as BroadcastSender;
use async_channel::Receiver;
use async_lock::Mutex;
use bytes::{Bytes, BytesMut};
use futures_channel::oneshot;
use futures_timer::Delay;
use futures_util::future::{Either, select};
use buffa::message::Message as _;
use wacore::{
    framing::FrameDecoder,
    handshake::{XxHandshakeState, build_handshake_header},
    net::{Transport, TransportEvent, TransportFactory},
    store::traits::Backend,
};
use wacore_binary::{
    Node, OwnedNodeRef, SERVER_JID,
    consts::WA_CONN_HEADER,
    node::{Attrs, NodeValue},
    util::unpack_bytes,
};

use crate::{
    ConnectionConfig, ConnectionError, NoiseSocket, RawEvent, Result,
    connection::{ConnectionCommand, timeout},
    transport::WebSocketTransportFactory,
};

const TRANSPORT_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const NOISE_HANDSHAKE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(20);
const KEEPALIVE_DEADLINE: Duration = Duration::from_secs(10);
const BACKOFF_BASE_SECS: u64 = 2;
const BACKOFF_MAX_SECS: u64 = 60;

/// Map of pending IQ request IDs to their response senders.
type IqWaiters = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Node>>>>>;

/// Shared state passed to frame processing functions.
pub(crate) struct RunnerFields {
    pub cmd_rx: Receiver<ConnectionCommand>,
    pub event_tx: BroadcastSender<RawEvent>,
    pub backend: Arc<dyn Backend>,
    pub config: ConnectionConfig,
    pub transport: Arc<Mutex<Option<Arc<dyn Transport>>>>,
    pub noise_socket: Arc<Mutex<Option<Arc<NoiseSocket>>>>,
    pub transport_events: Arc<Mutex<Option<Receiver<TransportEvent>>>>,
    pub is_connected: Arc<AtomicBool>,
    pub iq_waiters: IqWaiters,
    pub logged_out: Arc<AtomicBool>,
}

/// Establish a WebSocket connection and complete the Noise XX handshake.
///
/// On success the transport, NoiseSocket, and transport event receiver are
/// stored in the shared mutexes, `is_connected` is set to `true`, and a
/// [`RawEvent::Connected`] is broadcast.
#[tracing::instrument(skip(fields))]
pub(crate) async fn connect_once(fields: &RunnerFields) -> Result<()> {
    let device = fields
        .backend
        .load()
        .await
        .map_err(|e| ConnectionError::Protocol(format!("failed to load device: {e}")))?
        .ok_or_else(|| ConnectionError::Protocol("no device stored".into()))?;

    let factory = WebSocketTransportFactory::new(&fields.config.websocket_url);
    let (transport, mut transport_events) =
        timeout(TRANSPORT_CONNECT_TIMEOUT, factory.create_transport())
            .await
            .map_err(|_| ConnectionError::Socket("transport connect timed out".into()))?
            .map_err(|e| ConnectionError::Socket(format!("transport connect failed: {e}")))?;

    match transport_events.recv().await {
        Ok(TransportEvent::Connected) => {
            tracing::debug!("transport connected");
        }
        Ok(TransportEvent::Disconnected(_)) => {
            return Err(ConnectionError::Socket(
                "transport disconnected during connect".into(),
            ));
        }
        Ok(_) => {}
        Err(_) => {
            return Err(ConnectionError::Socket(
                "transport event channel closed during connect".into(),
            ));
        }
    }

    let noise_socket = perform_xx_handshake(&device, &transport, &mut transport_events).await?;

    {
        let mut t = fields.transport.lock().await;
        *t = Some(transport);
    }
    {
        let mut ns = fields.noise_socket.lock().await;
        *ns = Some(Arc::new(noise_socket));
    }
    {
        let mut te = fields.transport_events.lock().await;
        *te = Some(transport_events);
    }
    fields.is_connected.store(true, Ordering::SeqCst);

    if let Err(e) = fields.event_tx.try_broadcast(RawEvent::Connected) {
        tracing::warn!("failed to broadcast Connected event: {e}");
    }

    Ok(())
}

/// Perform the Noise XX handshake with the WhatsApp server.
#[tracing::instrument(skip(device, transport, transport_events))]
async fn perform_xx_handshake(
    device: &wacore::store::Device,
    transport: &Arc<dyn Transport>,
    transport_events: &mut Receiver<TransportEvent>,
) -> Result<NoiseSocket> {
    let client_payload = device.get_client_payload().encode_to_vec();
    let mut handshake_state =
        XxHandshakeState::new(device.noise_key.clone(), client_payload, &WA_CONN_HEADER)
            .map_err(|e| ConnectionError::Protocol(format!("handshake init failed: {e}")))?;

    let mut frame_decoder = FrameDecoder::new();

    let client_hello = handshake_state
        .build_client_hello()
        .map_err(|e| ConnectionError::Protocol(format!("build client hello failed: {e}")))?;

    let (header, _used_edge_routing) = build_handshake_header(device.edge_routing_info.as_deref());
    let framed = wacore::framing::encode_frame(&client_hello, Some(&header))
        .map_err(|e| ConnectionError::Protocol(format!("frame encode failed: {e}")))?;
    transport
        .send(Bytes::from(framed))
        .await
        .map_err(|e| ConnectionError::Socket(format!("send client hello failed: {e}")))?;

    tracing::debug!("sent ClientHello, waiting for ServerHello");

    let resp_frame = recv_handshake_frame(
        transport_events,
        &mut frame_decoder,
        NOISE_HANDSHAKE_RESPONSE_TIMEOUT,
    )
    .await?;
    tracing::debug!("received ServerHello");

    let client_finish = handshake_state
        .read_server_hello_and_build_client_finish(&resp_frame)
        .map_err(|e| ConnectionError::Protocol(format!("handshake server hello failed: {e}")))?;

    let framed = wacore::framing::encode_frame(&client_finish, None)
        .map_err(|e| ConnectionError::Protocol(format!("frame encode failed: {e}")))?;
    transport
        .send(Bytes::from(framed))
        .await
        .map_err(|e| ConnectionError::Socket(format!("send client finish failed: {e}")))?;

    tracing::debug!("sent ClientFinish, deriving cipher keys");

    let outcome = handshake_state
        .finish()
        .map_err(|e| ConnectionError::Protocol(format!("handshake finish failed: {e}")))?;

    tracing::info!("noise XX handshake complete");

    Ok(NoiseSocket::new(outcome.write_cipher, outcome.read_cipher))
}

/// Main read loop: three-way select over transport events, commands, and keepalive.
///
/// Returns `Ok(())` on clean disconnect (command or channel close), or `Err`
/// on transport error.
#[tracing::instrument(skip(fields))]
pub(crate) async fn read_loop(fields: &RunnerFields) -> Result<()> {
    let transport = {
        let t = fields.transport.lock().await;
        t.clone()
            .ok_or_else(|| ConnectionError::Protocol("transport not set in read loop".into()))?
    };
    let noise_socket = {
        let ns = fields.noise_socket.lock().await;
        ns.clone()
            .ok_or_else(|| ConnectionError::Protocol("noise_socket not set in read loop".into()))?
    };
    let transport_events = {
        let mut te = fields.transport_events.lock().await;
        te.take().ok_or_else(|| {
            ConnectionError::Protocol("transport_events not set in read loop".into())
        })?
    };

    let mut frame_decoder = FrameDecoder::new();
    let keepalive_interval = fields.config.keepalive_interval;

    loop {
        let event_fut = Box::pin(transport_events.recv());
        let cmd_fut = Box::pin(fields.cmd_rx.recv());
        let keepalive_fut = Box::pin(Delay::new(keepalive_interval));

        match select(select(event_fut, cmd_fut), keepalive_fut).await {
            Either::Left((Either::Left((event, _)), _)) => match event {
                Ok(TransportEvent::DataReceived(data)) => {
                    frame_decoder.feed(&data);
                    while let Some(frame) = frame_decoder.decode_frame() {
                        match process_incoming_frame(
                            &transport,
                            &noise_socket,
                            frame,
                            &fields.event_tx,
                            &fields.iq_waiters,
                            &fields.logged_out,
                        )
                        .await
                        {
                            Ok(()) => {}
                            Err(e) => {
                                tracing::warn!("Error processing frame: {e}");
                                let _ = fields
                                    .event_tx
                                    .try_broadcast(RawEvent::Error(e.to_string()));
                            }
                        }
                    }
                }
                Ok(TransportEvent::Connected) => {
                    tracing::debug!("transport re-connected event (unexpected in read loop)");
                }
                Ok(TransportEvent::Disconnected(_)) => {
                    tracing::info!("transport disconnected");
                    return Err(ConnectionError::Socket("disconnected".into()));
                }
                Err(_) => {
                    tracing::warn!("transport event channel closed");
                    return Err(ConnectionError::Socket(
                        "transport event channel closed".into(),
                    ));
                }
            },
            Either::Left((Either::Right((cmd, _)), _)) => match cmd {
                Ok(ConnectionCommand::SendNode(node, response_tx)) => {
                    let result = handle_send_node(&transport, &noise_socket, node).await;
                    let _ = response_tx.send(result);
                }
                Ok(ConnectionCommand::Disconnect) => {
                    tracing::info!("disconnect command received");
                    return Ok(());
                }
                Err(_) => {
                    tracing::debug!("command channel closed");
                    return Ok(());
                }
            },
            Either::Right((_, _)) => {
                if let Err(e) = send_keepalive(&transport, &noise_socket).await {
                    tracing::warn!("keepalive failed: {e}");
                    let _ = fields
                        .event_tx
                        .try_broadcast(RawEvent::Error(format!("keepalive failed: {e}")));
                }
            }
        }
    }
}

/// Decrypt, unpack, and route a single incoming frame.
#[tracing::instrument(skip(transport, noise_socket, event_tx, iq_waiters, logged_out))]
pub(crate) async fn process_incoming_frame(
    transport: &Arc<dyn Transport>,
    noise_socket: &NoiseSocket,
    mut frame: BytesMut,
    event_tx: &BroadcastSender<RawEvent>,
    iq_waiters: &IqWaiters,
    logged_out: &Arc<AtomicBool>,
) -> Result<()> {
    noise_socket.decrypt_frame(&mut frame)?;

    let unpacked = unpack_bytes(frame)
        .map_err(|e| ConnectionError::Protocol(format!("frame unpack failed: {e}")))?;

    let owned = OwnedNodeRef::new(unpacked)
        .map_err(|e| ConnectionError::Protocol(format!("node parse failed: {e}")))?;
    let node = owned.to_owned_node();

    let child_tags: Vec<&str> = match node.children() {
        Some(children) => children.iter().map(|c| c.tag.as_ref()).collect(),
        None => vec![],
    };
    tracing::debug!(
        tag = %node.tag,
        type_attr = ?node.attrs.get("type").map(|v| v.as_str().to_string()),
        from_attr = ?node.attrs.get("from").map(|v| v.as_str().to_string()),
        id_attr = ?node.attrs.get("id").map(|v| v.as_str().to_string()),
        xmlns_attr = ?node.attrs.get("xmlns").map(|v| v.as_str().to_string()),
        child_count = node.children().map(|c| c.len()).unwrap_or(0),
        child_tags = ?child_tags,
        "parsed incoming node"
    );

    if node.tag == "iq"
        && let Some(iq_type) = node.attrs.get("type")
        && iq_type.as_str() == "set"
    {
        let has_success = node
            .children()
            .is_some_and(|children| children.iter().any(|c| c.tag == "success"));

        if has_success {
            if let Some(children) = node.children()
                && let Some(success_child) = children.iter().find(|c| c.tag == "success")
            {
                let lid = success_child
                    .attrs
                    .get("lid")
                    .map(|v| v.as_str().to_string())
                    .unwrap_or_default();
                let ts = success_child
                    .attrs
                    .get("ts")
                    .map(|v| v.as_str().to_string())
                    .unwrap_or_default();
                tracing::info!(lid = %lid, ts = %ts, "received server success IQ");
            }

            if let Err(e) = event_tx.try_broadcast(RawEvent::Connected) {
                tracing::warn!("failed to broadcast Connected event: {e}");
            }
        }

        let has_pair_success = node
            .children()
            .is_some_and(|children| children.iter().any(|c| c.tag == "pair-success"));

        if !has_pair_success
            && let Some(server_id) = node.attrs.get("id")
        {
            let server_id_str = server_id.as_str().to_string();
            let mut attrs = Attrs::with_capacity(3);
            attrs.push("type", NodeValue::String("result".into()));
            attrs.push("id", NodeValue::String(server_id_str.clone().into()));
            attrs.push("to", NodeValue::String(SERVER_JID.into()));
            let result_node = Node::new("iq", attrs, None);

            if let Err(e) = handle_send_node(transport, noise_socket, result_node).await {
                tracing::warn!("failed to send IQ acknowledgment for id={server_id_str}: {e}");
            }
        }

        if node.tag == "failure" {
            tracing::warn!("received <failure> node - marking as logged out");
            logged_out.store(true, Ordering::SeqCst);
        }
        if let Err(e) = event_tx.broadcast(RawEvent::Node(node)).await {
            tracing::warn!("failed to broadcast node event: {e}");
        }

        return Ok(());
    }

    if node.tag == "iq"
        && let Some(id) = node.attrs.get("id")
    {
        let id_str = id.as_str().to_string();
        let mut waiters = iq_waiters.lock().await;
        if let Some(tx) = waiters.remove(&id_str) {
            let _ = tx.send(Ok(node.clone()));
        }
    }

    if node.tag == "failure" {
        tracing::warn!("received <failure> node - marking as logged out");
        logged_out.store(true, Ordering::SeqCst);
    }
    if let Err(e) = event_tx.broadcast(RawEvent::Node(node)).await {
        tracing::warn!("failed to broadcast node event: {e}");
    }

    Ok(())
}

/// Marshal a node, encrypt it, and send it through the transport.
#[tracing::instrument(skip(transport, noise_socket))]
pub(crate) async fn handle_send_node(
    transport: &Arc<dyn Transport>,
    noise_socket: &NoiseSocket,
    node: Node,
) -> Result<()> {
    let payload = wacore_binary::marshal(&node)
        .map_err(|e| ConnectionError::Protocol(format!("marshal failed: {e}")))?;

    noise_socket
        .encrypt_and_send(transport, Bytes::from(payload))
        .await
}

/// Send a keepalive ping to the server.
#[tracing::instrument(skip(transport, noise_socket))]
async fn send_keepalive(transport: &Arc<dyn Transport>, noise_socket: &NoiseSocket) -> Result<()> {
    let mut attrs = Attrs::with_capacity(3);
    attrs.push("to", NodeValue::String("s.whatsapp.net".into()));
    attrs.push("xmlns", NodeValue::String("w:p".into()));
    attrs.push("type", NodeValue::String("get".into()));

    let ping = Node::new("iq", attrs, None);

    timeout(
        KEEPALIVE_DEADLINE,
        handle_send_node(transport, noise_socket, ping),
    )
    .await
    .map_err(|_| ConnectionError::Socket("keepalive timed out".into()))?
}

/// Clean up connection state after disconnect.
///
/// Drains pending IQ waiters with [`ConnectionError::NotConnected`],
/// disconnects the transport, and clears the noise socket and event receiver.
#[tracing::instrument(skip(fields))]
pub(crate) async fn cleanup_connection(fields: &RunnerFields) {
    let mut waiters = fields.iq_waiters.lock().await;
    for (_, tx) in waiters.drain() {
        let _ = tx.send(Err(ConnectionError::NotConnected));
    }

    let mut t = fields.transport.lock().await;
    if let Some(transport) = t.take() {
        transport.disconnect().await;
    }

    let mut ns = fields.noise_socket.lock().await;
    *ns = None;

    let mut te = fields.transport_events.lock().await;
    *te = None;
}

/// Fibonacci backoff with small jitter for reconnect delays.
///
/// Starts at 2 seconds, caps at 60 seconds.
pub(crate) fn fibonacci_backoff(attempt: u64) -> Duration {
    let secs = match attempt {
        0 => BACKOFF_BASE_SECS,
        1 => BACKOFF_BASE_SECS,
        _ => {
            let mut a = BACKOFF_BASE_SECS;
            let mut b = BACKOFF_BASE_SECS;
            for _ in 2..=attempt {
                let next = a.saturating_add(b);
                a = b;
                b = next;
                if b >= BACKOFF_MAX_SECS {
                    return Duration::from_secs(BACKOFF_MAX_SECS);
                }
            }
            b
        }
    };

    let jitter = (secs as f64 * 0.1) as i64;
    let offset = if jitter > 0 {
        ((attempt.wrapping_mul(17) % (jitter as u64 * 2 + 1)) as i64) - jitter
    } else {
        0
    };
    let adjusted = (secs as i64 + offset).max(1) as u64;
    Duration::from_secs(adjusted)
}

/// Receive the first complete frame from the transport during the Noise handshake.
async fn recv_handshake_frame(
    transport_events: &mut Receiver<TransportEvent>,
    frame_decoder: &mut FrameDecoder,
    timeout_dur: Duration,
) -> Result<BytesMut> {
    loop {
        match timeout(timeout_dur, transport_events.recv()).await {
            Ok(Ok(TransportEvent::DataReceived(data))) => {
                frame_decoder.feed(&data);
                if let Some(frame) = frame_decoder.decode_frame() {
                    return Ok(frame);
                }
                continue;
            }
            Ok(Ok(TransportEvent::Connected)) => continue,
            Ok(Ok(TransportEvent::Disconnected(_))) => {
                return Err(ConnectionError::Socket(
                    "disconnected during handshake".into(),
                ));
            }
            Ok(Err(_)) => {
                return Err(ConnectionError::Socket(
                    "transport event channel closed during handshake".into(),
                ));
            }
            Err(_) => {
                return Err(ConnectionError::Socket(
                    "handshake response timed out".into(),
                ));
            }
        }
    }
}
