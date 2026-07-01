//! WebSocket transport via [`async-tungstenite`] with pluggable TLS.
//!
//! Provides a runtime-agnostic WebSocket transport implementing
//! [`wacore::net::TransportFactory`] and [`wacore::net::Transport`].
//!
//! # Feature flags
//!
//! - `smol` (default): Enables [`async_tungstenite::smol`] for the non-tokio
//!   connection path.
//! - `tokio`: Uses [`async_tungstenite::tokio::connect_async`] for TCP+TLS+WS
//!   in one call and [`tokio::task::spawn`] for the read pump.
//! - `rustls` (default): TLS via rustls with webpki-roots.
//! - `native-tls`: TLS via the platform's native TLS implementation.

use std::sync::Arc;

use anyhow::anyhow;
use async_channel::{Receiver, Sender};
use async_lock::Mutex;
use async_trait::async_trait;
use async_tungstenite::{WebSocketReceiver, WebSocketSender, WebSocketStream, tungstenite};
use bytes::Bytes;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::StreamExt;
use wacore::net::{Transport, TransportEvent, TransportFactory};

const EVENT_CHANNEL_CAPACITY: usize = 64;

/// A WebSocket transport wrapping a split sender half.
///
/// The generic parameter `S` is the underlying stream type, which varies
/// between the tokio and non-tokio connection paths.  The concrete type is
/// erased when the struct is returned as `Arc<dyn Transport>`.
struct WsTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    sink: Arc<Mutex<Option<WebSocketSender<S>>>>,
    shutdown_tx: Sender<()>,
}

impl<S> WsTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn new(sink: WebSocketSender<S>, shutdown_tx: Sender<()>) -> Self {
        Self {
            sink: Arc::new(Mutex::new(Some(sink))),
            shutdown_tx,
        }
    }
}

#[async_trait]
impl<S> Transport for WsTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async fn send(&self, data: Bytes) -> Result<(), anyhow::Error> {
        let mut guard = self.sink.lock().await;
        let sink = guard.as_mut().ok_or_else(|| anyhow!("socket is closed"))?;

        tracing::debug!("--> sending {} bytes", data.len());
        sink.send(tungstenite::Message::Binary(data))
            .await
            .map_err(|e| anyhow!("WebSocket send error: {e}"))?;

        Ok(())
    }

    async fn disconnect(&self) {
        let _ = self.shutdown_tx.try_send(());
        if let Some(mut sink) = self.sink.lock().await.take() {
            let _ = sink
                .send(tungstenite::Message::Close(Some(
                    tungstenite::protocol::CloseFrame {
                        code: tungstenite::protocol::frame::coding::CloseCode::Normal,
                        reason: "".into(),
                    },
                )))
                .await;
        }
    }
}

/// Reads from the WebSocket receiver and forwards binary messages as [`TransportEvent::DataReceived`].
/// Signals [`TransportEvent::Disconnected`] when the stream ends, a close frame is
/// received, or an error occurs.
async fn read_pump<S>(
    mut stream: WebSocketReceiver<S>,
    tx: Sender<TransportEvent>,
    shutdown_rx: Receiver<()>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    use futures_util::future::{Either, select};

    loop {
        let shutdown_fut = Box::pin(shutdown_rx.recv());
        let next_fut = Box::pin(stream.next());

        match select(shutdown_fut, next_fut).await {
            Either::Left(_) => break,
            Either::Right((next, _)) => match next {
                Some(Ok(msg)) if msg.is_binary() => {
                    let payload = msg.into_data();
                    tracing::debug!("<-- received WebSocket data: {} bytes", payload.len());

                    let inner_shutdown = Box::pin(shutdown_rx.recv());
                    let send_fut = Box::pin(tx.send(TransportEvent::DataReceived(payload)));
                    match select(inner_shutdown, send_fut).await {
                        Either::Left(_) => break,
                        Either::Right((r, _)) => {
                            if r.is_err() {
                                tracing::warn!("event receiver dropped");
                                break;
                            }
                        }
                    }
                }
                Some(Ok(msg)) if msg.is_close() => {
                    tracing::debug!("received close frame");
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    tracing::warn!("WebSocket read error: {e}");
                    break;
                }
                None => {
                    tracing::debug!("WebSocket stream ended");
                    break;
                }
            },
        }
    }

    let _ = tx.send(TransportEvent::Disconnected).await;
}

/// Wrap a WebSocket stream into a [`Transport`] and event receiver pair.
///
/// Spawns a background read pump that forwards binary messages as
/// [`TransportEvent::DataReceived`] and signals disconnection on close.
fn from_websocket<S>(ws: WebSocketStream<S>) -> (Arc<dyn Transport>, Receiver<TransportEvent>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (sink, stream) = ws.split();
    let (event_tx, event_rx) = async_channel::bounded(EVENT_CHANNEL_CAPACITY);
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

    let transport = Arc::new(WsTransport::new(sink, shutdown_tx));
    let _ = event_tx.try_send(TransportEvent::Connected);

    #[cfg(feature = "tokio")]
    tokio::task::spawn(read_pump(stream, event_tx, shutdown_rx));

    #[cfg(not(feature = "tokio"))]
    async_global_executor::spawn(read_pump(stream, event_tx, shutdown_rx)).detach();

    (transport, event_rx)
}

/// WebSocket transport factory that dials a WhatsApp WebSocket endpoint.
///
/// TLS is handled automatically using the default platform configuration
/// (webpki-roots for `rustls` feature, native certs for `native-tls` feature).
///
/// # Example
///
/// ```no_run
/// use waepic_connection::WebSocketTransportFactory;
/// use wacore::net::TransportFactory;
///
/// # async fn example() -> Result<(), anyhow::Error> {
/// let factory = WebSocketTransportFactory::new("wss://web.whatsapp.com/ws/chat");
/// let (transport, events) = factory.create_transport().await?;
/// # Ok(())
/// # }
/// ```
pub struct WebSocketTransportFactory {
    url: String,
}

impl WebSocketTransportFactory {
    /// Create a new factory for the given WebSocket URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

#[async_trait]
impl TransportFactory for WebSocketTransportFactory {
    async fn create_transport(
        &self,
    ) -> Result<(Arc<dyn Transport>, Receiver<TransportEvent>), anyhow::Error> {
        use async_tungstenite::tungstenite::client::IntoClientRequest;

        #[cfg(feature = "tokio")]
        {
            let request = self
                .url
                .as_str()
                .into_client_request()
                .map_err(|e| anyhow!("invalid WebSocket URL: {e}"))?;
            tracing::debug!("dialing {}", self.url);

            let (ws, _response) = async_tungstenite::tokio::connect_async(request)
                .await
                .map_err(|e| anyhow!("WebSocket connect failed: {e}"))?;
            tracing::debug!("WebSocket upgrade complete");

            Ok(from_websocket(ws))
        }

        #[cfg(not(feature = "tokio"))]
        {
            use std::net::ToSocketAddrs;

            use async_net::TcpStream;

            let request = self
                .url
                .as_str()
                .into_client_request()
                .map_err(|e| anyhow!("invalid WebSocket URL: {e}"))?;

            let host = request
                .uri()
                .host()
                .ok_or_else(|| anyhow!("no host in URL: {}", self.url))?
                .trim_start_matches('[')
                .trim_end_matches(']');
            let port = request.uri().port_u16().unwrap_or(443);
            tracing::debug!("dialing {} ({}:{})", self.url, host, port);

            let addr = (host, port)
                .to_socket_addrs()
                .map_err(|e| anyhow!("DNS resolution failed for {host}: {e}"))?
                .next()
                .ok_or_else(|| anyhow!("DNS resolution returned no addresses for {host}"))?;

            let tcp = TcpStream::connect(addr)
                .await
                .map_err(|e| anyhow!("TCP connect to {host}:{port} failed: {e}"))?;
            tracing::debug!("TCP connected to {host}:{port}");

            let (ws, _response) =
                async_tungstenite::smol::client_async_tls_with_connector_and_config(
                    request, tcp, None, None,
                )
                .await
                .map_err(|e| anyhow!("WebSocket connect failed: {e}"))?;
            tracing::debug!("WebSocket upgrade complete");

            Ok(from_websocket(ws))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[compio::test]
    #[ignore = "requires network access to web.whatsapp.com"]
    async fn transport_connects_to_whatsapp() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let factory = WebSocketTransportFactory::new(wacore::net::WHATSAPP_WEB_WS_URL);
        let result = factory.create_transport().await;
        assert!(
            result.is_ok(),
            "transport connect failed: {:?}",
            result.err()
        );
    }
}
