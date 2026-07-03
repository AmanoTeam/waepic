//! Pair Code Example
//!
//! Demonstrates phone-number-based pairing by:
//! 1. Creating a session and configuration
//! 2. Connecting to WhatsApp (returns a runner to spawn)
//! 3. Checking authorization status
//! 4. If not paired, requesting a pair code via companion_hello
//! 5. Listening for PairSuccess in the update stream
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
use tracing::Level;
use wacore::store::traits::DeviceStore;
use waepic_client::{Client, ClientConfiguration, Update};
use waepic_session::MemorySession;

#[compio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

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

    println!("[1/6] creating session and configuration...");

    let session = Arc::new(MemorySession::new());
    session.create().await.expect("failed to create device");

    let config = ClientConfiguration::default();

    println!("       session: MemorySession");
    println!("       config:  {:?}", config.device.version);

    println!("\n[2/6] connecting to WhatsApp...");
    let (client, runner) = Client::connect(session, config);
    compio::runtime::spawn(runner.run()).detach();
    println!("       client created, connection runner spawned in background.");

    client
        .load_or_create_device()
        .await
        .expect("failed to load device");

    println!("\n[3/6] starting update stream, waiting for connection...");
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

    println!("\n[4/6] checking authorization status...");
    match client.is_authorized().await {
        Ok(true) => {
            println!("       already paired!");
            if let Ok(chat) = client.get_me().await {
                println!("       logged in as: {chat}");
            }
        }
        Ok(false) => {
            println!("       not paired. requesting pair code...");

            match client.request_pair_code(&phone_number).await {
                Ok(code) => {
                    println!();
                    println!("       enter this code on your phone: {code}");
                    println!();
                    println!("       (open WhatsApp on your phone ->");
                    println!("        linked devices -> link a device ->");
                    println!("        link with phone number instead)");
                    println!();
                }
                Err(e) => {
                    println!("       failed to request pair code: {e}");
                    return;
                }
            }
        }
        Err(e) => {
            println!("       could not check authorization: {e}");
        }
    }

    println!("\n[5/6] listening for updates (Ctrl+C to exit)...");
    let client_clone = client.clone();
    compio::runtime::spawn(async move {
        while let Some(update) = updates.next().await {
            match &update {
                Update::PairingCode { code, timeout } => {
                    println!();
                    println!("       server pushed pair code");
                    println!();
                    println!("       code: {code}");
                    println!("       expires in: {timeout}s");
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

    println!("\n[6/6] waiting for Ctrl+C...");
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
