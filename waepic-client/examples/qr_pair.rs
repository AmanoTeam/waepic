//! QR Pairing Example
//!
//! Demonstrates the waepic-client API by:
//! 1. Creating a session and configuration
//! 2. Connecting to WhatsApp (returns a runner to spawn)
//! 3. Checking authorization status
//! 4. If not paired, requesting QR pairing and displaying the QR code
//! 5. Streaming updates after pairing
//! 6. Handling Ctrl+C for graceful shutdown
//!
//! # Usage
//!
//! ```bash
//! cargo run --example qr_pair -p waepic-client
//! ```

use std::sync::Arc;

use futures_util::future::{join, pending};
use tracing::Level;
use wacore::store::traits::DeviceStore;
use waepic_client::{Client, ClientConfiguration, PairEvent, Update};
use waepic_session::MemorySession;

#[compio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    println!("=== waepic-client QR Pairing Example ===\n");

    // Step 1: Create a session and configuration
    // MemorySession wraps InMemoryBackend and adds chat caching.
    // Session extends Backend, so one object serves both roles.
    println!("[1/6] Creating session and configuration...");

    let session = Arc::new(MemorySession::new());

    // Initialize the device in the backend (generates noise keys, identity keys, etc.)
    session.create().await.expect("failed to create device");

    let config = ClientConfiguration::default();

    println!("       Session: MemorySession");
    println!("       Config:  {:?}", config.device.version);

    // Step 2: Connect to WhatsApp
    // Client::connect returns (Client, Receiver<RawEvent>, ConnectionRunner).
    // The caller must spawn the runner on their runtime.
    println!("\n[2/6] Connecting to WhatsApp...");
    let (client, raw_rx, runner) = Client::connect(session, config);
    compio::runtime::spawn(runner.run()).detach();
    println!("       Client created, connection runner spawned in background.");

    // Load device state from the session backend
    client
        .load_or_create_device()
        .await
        .expect("failed to load device");

    // Step 3: Start update stream and wait for connection
    // stream_updates() returns (UpdateStream, Future). The future must be
    // spawned to drive the update processing.
    println!("\n[3/6] Starting update stream, waiting for connection...");
    let (mut updates, update_task) = client.stream_updates(raw_rx);
    compio::runtime::spawn(update_task).detach();

    // Wait for the Connected event before proceeding (may take a few retries)
    let mut connected = false;
    let mut attempts = 0;
    while let Some(update) = updates.next().await {
        match update {
            Update::Connected => {
                println!("       connected to WhatsApp!");
                connected = true;
                break;
            }
            Update::Disconnected => {
                attempts += 1;
                if attempts >= 5 {
                    println!("       failed to connect after {attempts} attempts. Exiting.");
                    break;
                }
                println!("       connection attempt {attempts} failed, waiting for reconnect...");
            }
            other => {
                println!("       (pre-connect event: {other:?})");
            }
        }
    }

    if !connected {
        println!("       failed to connect. Exiting.");
        return;
    }

    // Step 4: Check authorization status
    println!("\n[4/6] Checking authorization status...");
    match client.is_authorized().await {
        Ok(true) => {
            println!("       already paired!");
            if let Ok(chat) = client.get_me().await {
                println!("       logged in as: {chat}");
            }
        }
        Ok(false) => {
            println!("       not paired. Requesting QR pairing...");
            request_qr_pairing(client.clone()).await;
        }
        Err(e) => {
            println!("       could not check authorization: {e}");
        }
    }

    // Step 5: Print remaining updates
    println!("\n[5/6] Listening for updates (Ctrl+C to exit)...");
    let client_clone = client.clone();
    compio::runtime::spawn(async move {
        while let Some(update) = updates.next().await {
            println!("       update: {update:?}");
        }
        println!("       update stream ended.");
    })
    .detach();

    // Step 6: Block until Ctrl+C, then disconnect gracefully
    // compio does not expose a signal module, so we block forever.
    println!("\n[6/6] Waiting for Ctrl+C...");
    pending::<()>().await;

    println!("\n       Ctrl+C received. Disconnecting...");
    match client_clone.disconnect().await {
        Ok(()) => println!("       disconnected gracefully."),
        Err(e) => println!("       disconnect error (expected in example): {e}"),
    }

    println!("\n=== Example complete ===");
}

/// Request QR pairing and display the QR code in the terminal.
///
/// Returns a PairEventStream and a future that drives the pairing process.
/// The future must be spawned on the runtime.
async fn request_qr_pairing(client: Client) {
    match client.request_qr_pairing().await {
        Ok((mut pair_stream, pair_task)) => {
            println!("       pairing stream obtained. Waiting for QR code...");

            // Run event handling and the pairing task concurrently.
            // join avoids the need to spawn (which would require 'static).
            let handle_events = async {
                while let Some(event) = pair_stream.recv().await {
                    match event {
                        PairEvent::QrCode { code, timeout } => {
                            println!();
                            println!("       SCAN THIS QR CODE WITH WHATSAPP");
                            println!();
                            qr2term::print_qr(&code).unwrap();
                            println!();
                            println!("       URL: {code}");
                            println!("       expires in: {timeout}s");
                            println!();
                            println!("       (Open WhatsApp on your phone ->");
                            println!("        Linked Devices -> Link a Device)");
                            println!();
                        }
                        PairEvent::Success => {
                            println!("       pairing successful!");
                            break;
                        }
                        PairEvent::Error(e) => {
                            println!("       pairing failed: {e}");
                            break;
                        }
                    }
                }
            };

            join(handle_events, pair_task).await;
        }
        Err(e) => {
            println!("       could not start QR pairing: {e}");
            println!("       (expected without a real connection.)");
        }
    }
}
