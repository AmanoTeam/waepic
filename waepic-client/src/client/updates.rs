//! Update stream: converts raw connection events into high-level `Update` items.
//!
//! The `UpdateStream` is the primary way applications receive events from
//! the WhatsApp connection - messages, connection state changes, receipts,
//! and presence updates all flow through this stream.

use std::{future::Future, sync::atomic::Ordering};

use wacore::iq::usync::DeviceListSpec;
use wacore_binary::builder::NodeBuilder;
use waepic_connection::RawEvent;

use crate::{
    Client, Result,
    client::{
        handlers::{
            handle_chatstate, handle_failure, handle_history_sync, handle_ib, handle_notification,
            handle_pair_code, handle_presence, handle_receipt, handle_success,
            process_incoming_node,
        },
        pair::{send_active_iq, upload_prekeys_if_needed},
    },
    error::ClientError,
    update::Update,
};

/// A stream of high-level [`Update`] events emitted by the client.
///
/// Created by [`Client::stream_updates`]. Call [`UpdateStream::next`] in a
/// loop to receive connection-state changes, incoming messages, and other
/// events.
///
/// # Example
///
/// ```no_run
/// # use waepic_client::Client;
///
/// # async fn example(client: &Client) {
/// let (mut updates, update_task) = client.stream_updates().expect("client must be connected");
/// // Spawn the update task on your runtime
/// while let Some(update) = updates.next().await {
///     println!("Received update: {update:?}");
/// }
/// # }
/// ```
pub struct UpdateStream {
    rx: async_channel::Receiver<Update>,
}

impl UpdateStream {
    /// Wait for and return the next update.
    ///
    /// Returns `None` when the underlying connection is closed and no more
    /// updates will arrive.
    pub async fn next(&mut self) -> Option<Update> {
        self.rx.recv().await.ok()
    }
}

impl Client {
    /// Create an [`UpdateStream`] that converts raw connection events into
    /// high-level [`Update`] items.
    ///
    /// Returns the stream and a future that must be spawned on your runtime
    /// to drive the update processing loop.
    pub fn stream_updates(&self) -> Result<(UpdateStream, impl Future<Output = ()> + use<>)> {
        let raw_rx = self
            .inner
            .raw_tx
            .as_ref()
            .ok_or(ClientError::NotConnected)?
            .new_receiver();

        let (tx, rx) = async_channel::unbounded();
        let client = self.clone();

        let future = run_update_stream(client, raw_rx, tx);
        Ok((UpdateStream { rx }, future))
    }
}

/// Background task that drives the update stream.
///
/// Reads [`RawEvent`]s from the connection layer, converts them to
/// [`Update`]s, and forwards them to the application via the channel.
/// Periodically flushes the Signal protocol cache to the backend
/// (every 100 events).
async fn run_update_stream(
    client: Client,
    mut raw_rx: async_broadcast::Receiver<RawEvent>,
    tx: async_channel::Sender<Update>,
) {
    let mut event_count = 0u64;

    loop {
        let event = match raw_rx.recv().await {
            Ok(event) => event,
            Err(async_broadcast::RecvError::Closed) => break,
            Err(async_broadcast::RecvError::Overflowed(n)) => {
                tracing::warn!("update stream receiver overflowed by {n} events, continuing");
                continue;
            }
        };

        match event {
            RawEvent::Node(node) => {
                // Dispatch by node tag to the appropriate handler
                let tag: &str = &node.tag;

                // History sync arrives as notification type="history_sync_notification"
                // or as an <ib> node with a <history_sync> child.
                let is_history_sync = (tag == "notification"
                    && node
                        .attrs
                        .get("type")
                        .is_some_and(|v| v.as_str().as_ref() == "history_sync_notification"))
                    || (tag == "ib" && node.get_optional_child("history_sync").is_some());

                if is_history_sync {
                    let updates = handle_history_sync(&node, &client);
                    for update in updates {
                        let _ = tx.send(update).await;
                    }
                    continue;
                }

                // Message nodes can produce multiple updates (e.g., history
                // sync notifications embedded in encrypted messages), so
                // handle them separately from the single-update dispatch.
                if tag == "message" {
                    match process_incoming_node(&node, &client).await {
                        Ok(updates) => {
                            for update in updates {
                                if matches!(update, Update::ConnectFailure(_))
                                    && client.inner.post_pair_reconnect.load(Ordering::Acquire)
                                {
                                    tracing::debug!("ignoring ConnectFailure during post-pair reconnect");
                                    continue;
                                }
                                let _ = tx.send(update).await;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("error processing incoming node: {e}");
                        }
                    }
                    continue;
                }

                let result = match tag {
                    "receipt" => Ok(handle_receipt(&node, &client)),
                    "presence" => Ok(handle_presence(&node, &client)),
                    "chatstate" => Ok(handle_chatstate(&node, &client)),
                    "notification" => handle_notification(&node, &client).await,
                    "success" => Ok(Some(handle_success(&node))),
                    "failure" | "stream:error" => Ok(Some(handle_failure(&node))),
                    "iq" => Ok(handle_pair_code(&node)),
                    "ib" => handle_ib(&node, &client).await,
                    _ => Ok(None), // Unknown node tags are silently ignored
                };

                match result {
                    Ok(Some(update)) => {
                        // Suppress ConnectFailure during post-pair reconnect
                        // window. The server sends error 515 to force a
                        // reconnect after pairing - this is expected and
                        // should not be surfaced to the app.
                        if matches!(update, Update::ConnectFailure(_))
                            && client.inner.post_pair_reconnect.load(Ordering::Acquire)
                        {
                            tracing::debug!("ignoring ConnectFailure during post-pair reconnect");
                            continue;
                        }

                        // When the server sends a bare <failure> (no error
                        // code), it means the companion device was removed
                        // server-side. Clear the stored device credentials
                        // so the next connection starts fresh instead of
                        // reconnecting with stale credentials.
                        if matches!(update, Update::LoggedOut) {
                            if let Err(e) = client.inner.session.clear_device().await {
                                tracing::warn!("failed to auto-clear device on logout: {e}");
                            }
                        }

                        let _ = tx.send(update).await;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!("error processing incoming node: {e}");
                    }
                }
            }
            RawEvent::Connected => {
                if client.inner.post_pair_reconnect.load(Ordering::Acquire) {
                    tracing::debug!("ignoring Connected during post-pair reconnect window");

                    // Run post-connect init silently (prekeys, device sync,
                    // presence) without emitting Connected to the app.
                    run_post_connect_init(&client).await;

                    // Reset the flag after the first successful post-pair
                    // reconnect so subsequent real disconnects are not
                    // suppressed.
                    client
                        .inner
                        .post_pair_reconnect
                        .store(false, Ordering::Release);
                    continue;
                }

                let _ = tx.send(Update::Connected).await;

                // After reconnecting with stored credentials (post-pairing),
                // upload prekeys so the phone can encrypt messages to us.
                // The update stream runs concurrently with the read loop, so
                // IQ responses will arrive during read_loop.

                // Reload device from backend to get fresh state.
                // After QR pairing, handle_pair_success saves the updated
                // device (with pn set) to the backend, but the in-memory
                // RwLock is NOT updated. The server then sends stream:error
                // to force a reconnect, and after reconnect the Connected
                // handler fires. We must reload from backend to see the
                // fresh device state with pn set.
                run_post_connect_init(&client).await;
            }
            RawEvent::Disconnected => {
                if client.inner.post_pair_reconnect.load(Ordering::Acquire) {
                    tracing::debug!("ignoring Disconnected during post-pair reconnect window");
                    continue;
                }

                let _ = tx.send(Update::Disconnected).await;
            }
            RawEvent::Error(msg) => {
                tracing::warn!("connection error event: {msg}");
            }
        }

        event_count += 1;

        // Flush cache every 100 events to persist Signal state
        if event_count.is_multiple_of(100) {
            let backend = client.inner.session.clone();
            if let Err(e) = client.inner.signal_cache.flush(&*backend).await {
                tracing::warn!("failed to flush signal cache: {e}");
            }
        }
    }
}

/// Reload device state and run post-connect init (prekeys, active iq,
/// device sync, presence). Used both during normal Connected and during
/// the post-pair reconnect window.
async fn run_post_connect_init(client: &Client) {
    let backend = client.inner.session.clone();
    match backend.load().await {
        Ok(Some(fresh_device)) => {
            // Update the in-memory RwLock with the fresh device
            *client.inner.device.write().await = fresh_device.clone();
            if fresh_device.pn.is_some() {
                // Upload prekeys if the server count is low
                if let Err(e) = upload_prekeys_if_needed(
                    &client.inner.handle,
                    &client.inner.session,
                    &fresh_device,
                )
                .await
                {
                    tracing::warn!("failed to upload prekeys after reconnect: {e}");
                }

                // Send active IQ to exit passive mode (backup for post_connect_init)
                if let Err(e) = send_active_iq(&client.inner.handle).await {
                    tracing::warn!("failed to send active iq after reconnect: {e}");
                }

                // Sync our own device list so the server knows
                // about this companion device for DM fan-out.
                if let Some(pn) = &fresh_device.pn {
                    let device_spec = DeviceListSpec::new(vec![pn.clone()], "device_list_sync");
                    if let Err(e) = client.inner.handle.send_iq(device_spec).await {
                        tracing::warn!("failed to sync device list: {e}");
                    }
                }

                // Send presence available if push_name is set.
                // Skip if empty (matches WA Web guard).
                if !fresh_device.push_name.is_empty() {
                    let presence_node = NodeBuilder::new("presence")
                        .attrs([
                            ("type", "available"),
                            ("name", fresh_device.push_name.as_str()),
                        ])
                        .build();
                    if let Err(e) = client.inner.handle.send_node(presence_node).await {
                        tracing::warn!("failed to send presence available: {e}");
                    }
                }
            }
        }
        Ok(None) => {
            tracing::debug!("no device stored in backend, skipping post-connect init");
        }
        Err(e) => {
            tracing::warn!("failed to load device from backend: {e}");
        }
    }
}
