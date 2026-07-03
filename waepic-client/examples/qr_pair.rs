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

use async_signal::{Signal, Signals};
use futures_util::StreamExt;
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

    println!("[1/6] Creating session and configuration...");

    let session = Arc::new(MemorySession::new());
    session.create().await.expect("failed to create device");

    let config = ClientConfiguration::default();

    println!("       Session: MemorySession");
    println!("       Config:  {:?}", config.device.version);

    println!("\n[2/6] Connecting to WhatsApp...");
    let (client, runner) = Client::connect(session, config);
    compio::runtime::spawn(runner.run()).detach();
    println!("       Client created, connection runner spawned in background.");

    client
        .load_or_create_device()
        .await
        .expect("failed to load device");

    println!("\n[3/6] Starting update stream, waiting for connection...");
    let (mut updates, update_task) = client.stream_updates().expect("client must be connected");
    compio::runtime::spawn(update_task).detach();

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
            request_pairing(client.clone()).await;
        }
        Err(e) => {
            println!("       could not check authorization: {e}");
        }
    }

    println!("\n[5/6] Listening for updates (Ctrl+C to exit)...");
    let client_clone = client.clone();
    compio::runtime::spawn(async move {
        while let Some(update) = updates.next().await {
            println!("       update: {update:?}");
        }
        println!("       update stream ended.");
    })
    .detach();

    println!("\n[6/6] Waiting for Ctrl+C...");
    let mut signals =
        Signals::new([Signal::Int, Signal::Term]).expect("failed to register signal handlers");
    signals.next().await;

    println!("\n       Ctrl+C received. Disconnecting...");
    match client_clone.disconnect().await {
        Ok(()) => println!("       disconnected gracefully."),
        Err(e) => println!("       disconnect error (expected in example): {e}"),
    }

    println!("\n=== Example complete ===");
}

/// Request QR pairing and display the QR code in the terminal.
async fn request_pairing(client: Client) {
    match client.request_pairing().await {
        Ok(mut pair_stream) => {
            println!("       pairing stream obtained. Waiting for QR code...");

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
        }
        Err(e) => {
            println!("       could not start QR pairing: {e}");
            println!("       (expected without a real connection.)");
        }
    }
}
