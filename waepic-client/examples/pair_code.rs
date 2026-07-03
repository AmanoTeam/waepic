//! Pair Code Example
//!
//! Demonstrates phone-number-based pairing by:
//! 1. Creating a session and configuration
//! 2. Connecting to WhatsApp (returns a runner to spawn)
//! 3. Checking authorization status
//! 4. If not paired, listening for pair-code events from the update stream
//! 5. Also starting QR pairing as a fallback
//! 6. Handling Ctrl+C for graceful shutdown
//!
//! # Usage
//!
//! ```bash
//! cargo run --example pair_code -p waepic-client -- +5511999998888
//! ```
//!
//! The phone number should be in international format (with or without + prefix).

use std::sync::Arc;

use async_signal::{Signal, Signals};
use futures_util::StreamExt;
use futures_util::future::join;
use tracing::Level;
use wacore::store::traits::DeviceStore;
use waepic_client::{Client, ClientConfiguration, PairEvent, Update};
use waepic_session::MemorySession;

#[compio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    // Parse phone number from command line
    let args: Vec<String> = std::env::args().collect();
    let phone_number = if args.len() >= 2 {
        args[1].clone()
    } else {
        eprintln!("usage: cargo run --example pair_code -p waepic-client -- <phone_number>");
        eprintln!("example: cargo run --example pair_code -p waepic-client -- +5511999998888");
        std::process::exit(1);
    };

    println!("=== waepic-client pair code example ===");
    println!("phone number: {phone_number}\n");

    // Step 1: Create a session and configuration
    // MemorySession wraps InMemoryBackend and adds chat caching.
    // Session extends Backend, so one object serves both roles.
    println!("[1/7] creating session and configuration...");

    let session = Arc::new(MemorySession::new());

    // Initialize the device in the backend (generates noise keys, identity keys, etc.)
    session.create().await.expect("failed to create device");

    let config = ClientConfiguration::default();

    println!("       session: MemorySession");
    println!("       config:  {:?}", config.device.version);

    // Step 2: Connect to WhatsApp
    // Client::connect returns (Client, ConnectionRunner).
    // The caller must spawn the runner on their runtime.
    println!("\n[2/7] connecting to WhatsApp...");
    let (client, runner) = Client::connect(session, config);
    compio::runtime::spawn(runner.run()).detach();
    println!("       client created, connection runner spawned in background.");

    // Load device state from the session backend
    client
        .load_or_create_device()
        .await
        .expect("failed to load device");

    // Step 3: Start update stream and wait for connection
    // stream_updates() returns (UpdateStream, Future). The future must be
    // spawned to drive the update processing.
    println!("\n[3/7] starting update stream, waiting for connection...");
    let (mut updates, update_task) = client.stream_updates().expect("client must be connected");
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
                    println!("       failed to connect after {attempts} attempts. exiting.");
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
        println!("       failed to connect. exiting.");
        return;
    }

    // Step 4: Check authorization status
    println!("\n[4/7] checking authorization status...");
    match client.is_authorized().await {
        Ok(true) => {
            println!("       already paired!");
            if let Ok(chat) = client.get_me().await {
                println!("       logged in as: {chat}");
            }
        }
        Ok(false) => {
            println!("       not paired. waiting for pair code from server...");
            println!("       (the server will push a pair-code event for phone number: {phone_number})");
        }
        Err(e) => {
            println!("       could not check authorization: {e}");
        }
    }

    // Step 5: Start QR pairing as fallback
    println!("\n[5/7] starting QR pairing as fallback...");
    start_qr_fallback(client.clone()).await;

    // Step 6: Listen for pair code events and other updates
    println!("\n[6/7] listening for updates (Ctrl+C to exit)...");
    let client_clone = client.clone();
    compio::runtime::spawn(async move {
        while let Some(update) = updates.next().await {
            match &update {
                Update::PairingCode { code, timeout } => {
                    println!();
                    println!("       enter this code on your phone");
                    println!();
                    println!("       code: {code}");
                    println!("       expires in: {timeout}s");
                    println!();
                    println!("       (open WhatsApp on your phone ->");
                    println!("        linked devices -> link a device ->");
                    println!("        link with phone number instead)");
                    println!();
                }
                Update::PairSuccess => {
                    println!("       pairing successful!");
                }
                _ => {
                    println!("       update: {update:?}");
                }
            }
        }
        println!("       update stream ended.");
    })
    .detach();

    // Step 7: Handle Ctrl+C for graceful shutdown
    println!("\n[7/7] waiting for Ctrl+C...");
    let mut signals =
        Signals::new([Signal::Int, Signal::Term]).expect("failed to register signal handlers");
    signals.next().await;

    println!("\n       Ctrl+C received. disconnecting...");
    match client_clone.disconnect().await {
        Ok(()) => println!("       disconnected gracefully."),
        Err(e) => println!("       disconnect error (expected in example): {e}"),
    }

    println!("\n=== example complete ===");
}

/// Start QR pairing as a fallback in case pair code doesn't work.
async fn start_qr_fallback(client: Client) {
    match client.request_qr_pairing().await {
        Ok((mut pair_stream, pair_task)) => {
            println!("       QR pairing started as fallback.");

            // Run event handling and the pairing task concurrently.
            // join avoids the need to spawn (which would require 'static).
            let handle_events = async {
                while let Some(event) = pair_stream.recv().await {
                    match event {
                        PairEvent::QrCode { code, timeout } => {
                            println!();
                            println!("       scan this QR code with WhatsApp");
                            println!();
                            qr2term::print_qr(&code).unwrap();
                            println!();
                            println!("       URL: {code}");
                            println!("       expires in: {timeout}s");
                            println!();
                        }
                        PairEvent::Success => {
                            println!("       QR pairing successful!");
                            break;
                        }
                        PairEvent::Error(e) => {
                            println!("       QR pairing failed: {e}");
                            break;
                        }
                    }
                }
            };

            join(handle_events, pair_task).await;
        }
        Err(e) => {
            println!("       could not start QR fallback: {e}");
        }
    }
}
