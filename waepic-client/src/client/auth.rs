//! Authentication status, login-check helpers, and pair-code protocol.
//!
//! The pair-code flow uses a two-stage crypto protocol:
//! 1. `companion_hello` - client sends encrypted ephemeral key, server returns pairing ref
//! 2. `companion_finish` - after phone confirms, client performs DH and sends key bundle

use std::sync::{
    Arc,
    atomic::Ordering,
};

use anyhow::anyhow;
use rand::rngs::StdRng;
use wacore::{
    iq::spec::IqSpec,
    libsignal::protocol::KeyPair,
    pair_code::{PairCodeOptions, PairCodeState, PairCodeUtils, resolve_companion_platform},
    request::{InfoQuery, InfoQueryType},
};
use wacore_binary::{Jid, Node, NodeRef, Server, node::NodeContent};

use crate::{Client, Result, client, error::ClientError, peer::Chat};
use waepic_connection::RawEvent;

/// Whether the client has an authenticated session.
#[derive(Clone, Debug)]
pub enum LoginStatus {
    /// The client is authorized with a known JID and push name.
    Authorized {
        /// The user's JID.
        jid: Jid,
        /// The user's push name as stored on the device.
        push_name: String,
    },
    /// The client is not paired / not logged in.
    NotAuthorized,
}

impl Client {
    /// Check whether the session has a stored device with a phone number,
    /// which indicates a completed pairing flow.
    #[tracing::instrument(skip(self))]
    pub async fn is_authorized(&self) -> Result<bool> {
        let device = self
            .inner
            .session
            .load()
            .await
            .map_err(|e| ClientError::Internal(format!("failed to load device: {e}")))?;

        if let Some(d) = device {
            let authorized = d.pn.is_some();
            tracing::trace!(authorized, "checked");

            Ok(authorized)
        } else {
            tracing::trace!("no device stored");
            Ok(false)
        }
    }

    /// Return the current [`LoginStatus`] by inspecting the stored device.
    #[tracing::instrument(skip(self))]
    pub async fn check_login_status(&self) -> Result<LoginStatus> {
        let device = self
            .inner
            .session
            .load()
            .await
            .map_err(|e| ClientError::Internal(format!("failed to load device: {e}")))?;

        match device {
            Some(d) if d.pn.is_some() => {
                let jid = d.pn.clone().expect("pn is Some");
                tracing::trace!(%jid, push_name = %d.push_name, "authorized");

                Ok(LoginStatus::Authorized {
                    jid,
                    push_name: d.push_name,
                })
            }
            _ => {
                tracing::trace!("not authorized");
                Ok(LoginStatus::NotAuthorized)
            }
        }
    }

    /// Return the current user as a [`Chat::User`] variant.
    ///
    /// Requires an authorized session; returns [`ClientError::NotLoggedIn`]
    /// when the client is not paired.
    #[tracing::instrument(skip(self))]
    pub async fn get_me(&self) -> Result<Chat> {
        let device = self
            .inner
            .session
            .load()
            .await
            .map_err(|e| ClientError::Internal(format!("failed to load device: {e}")))?;

        match device {
            Some(d) if d.pn.is_some() => {
                let jid = d.pn.clone().expect("pn is Some");
                let push_name = d.push_name;

                let mut chat = self.chat(jid);
                if let Chat::User(ref mut user) = chat {
                    user.set_push_name(push_name);
                }

                tracing::trace!("returning user chat");
                Ok(chat)
            }
            _ => {
                tracing::trace!("not authorized");
                Err(ClientError::NotLoggedIn)
            }
        }
    }

    /// Request pair-code authentication for phone-number linking.
    ///
    /// Generates an 8-character code locally, sends a `companion_hello` IQ
    /// with an encrypted ephemeral key, and stores state for stage 2
    /// (phone confirmation). Returns the code string to display to the user.
    #[tracing::instrument(skip(self))]
    pub async fn request_pair_code(&self, phone_number: &str) -> Result<String> {
        let sanitized = phone_number
            .chars()
            .filter(char::is_ascii_digit)
            .collect::<String>();
        if sanitized.is_empty() {
            return Err(ClientError::Internal(
                "phone number must contain at least one digit".into(),
            ));
        } else if sanitized.len() < 7 {
            return Err(ClientError::Internal(
                "phone number is too short (must be at least 7 digits)".into(),
            ));
        } else if sanitized.starts_with('0') {
            return Err(ClientError::Internal(
                "phone number must not start with 0 (use international format)".into(),
            ));
        }

        let code = PairCodeUtils::generate_code();
        tracing::debug!(phone = %sanitized, %code, "starting pair code authentication");

        let ephemeral_keypair = KeyPair::generate(&mut rand::make_rng::<StdRng>());

        let device = self.inner.device.read().await;
        let noise_static_pub: [u8; 32] = device
            .noise_key
            .public_key
            .public_key_bytes()
            .try_into()
            .expect("noise key is 32 bytes");

        let ephemeral_pub: [u8; 32] = ephemeral_keypair
            .public_key
            .public_key_bytes()
            .try_into()
            .expect("ephemeral key is 32 bytes");
        let wrapped_ephemeral = PairCodeUtils::encrypt_ephemeral_pub(&ephemeral_pub, &code);

        let (platform_id, platform_display) = resolve_companion_platform(
            &PairCodeOptions::for_phone(&sanitized),
            &device.device_props,
        );
        let platform_id_str = platform_id.to_string();
        drop(device);

        let req_id = format!("{:016x}", rand::random::<u64>());
        let spec = CompanionHelloSpec {
            phone: sanitized.clone(),
            noise_static_pub,
            wrapped_ephemeral,
            platform_id: platform_id_str,
            platform_display,
            show_push_notification: true,
            req_id: req_id.clone(),
        };

        let pairing_ref = self.inner.handle.send_iq(spec).await.map_err(|e| {
            tracing::warn!("companion_hello iq failed: {e}");
            ClientError::Internal(format!("pair code request failed: {e}"))
        })?;
        tracing::debug!("stage 1 complete, waiting for phone confirmation");

        *self.inner.pair_code_state.lock().await = PairCodeState::WaitingForPhoneConfirmation {
            pairing_ref,
            phone_jid: sanitized,
            pair_code: code.clone(),
            ephemeral_keypair: Box::new(ephemeral_keypair),
            code_generation_ts: 0,
            primary_hello_attempt_count: 0,
        };

        let raw_tx = self.inner.raw_tx.clone();
        let handle = self.inner.handle.clone();
        let session = self.inner.session.clone();
        let device = self.inner.device.clone();
        let config = self.inner.config.clone();
        let post_pair_reconnect = Arc::clone(&self.inner.post_pair_reconnect);

        async_global_executor::spawn(async move {
            let mut raw_rx = raw_tx.as_ref().expect("raw_tx").new_receiver();

            loop {
                match raw_rx.recv().await {
                    Ok(RawEvent::Node(node)) => {
                        if client::pair::is_pair_success_node(&node) {
                            tracing::debug!("received pair-success for pair code flow");

                            let dev = device.read().await.clone();
                            match client::pair::handle_pair_success(
                                &handle, &session, &dev, &node, &config,
                            )
                            .await
                            {
                                Ok(()) => {
                                    tracing::info!("pair code pairing completed successfully");
                                    // Signal the update stream to suppress
                                    // Connected/Disconnected during the
                                    // post-pair reconnect window.
                                    post_pair_reconnect
                                        .store(true, Ordering::Release);
                                    return;
                                }
                                Err(e) => {
                                    tracing::error!("pair code pair-success handling failed: {e}");
                                    return;
                                }
                            }
                        }
                    }
                    Ok(RawEvent::Disconnected) => {
                        tracing::debug!("disconnected during pair code pair-success wait");
                        return;
                    }
                    Err(_) => {
                        tracing::debug!(
                            "raw event stream ended during pair code pair-success wait"
                        );
                        return;
                    }
                    _ => {}
                }
            }
        })
        .detach();

        Ok(code)
    }
}

/// IQ spec for the `companion_hello` stage of pair-code authentication.
///
/// Sends a `link_code_companion_reg` node with `stage="companion_hello"` as a
/// `set` IQ. The server responds with a pairing ref used in stage 2.
struct CompanionHelloSpec {
    phone: String,
    noise_static_pub: [u8; 32],
    wrapped_ephemeral: [u8; 80],
    platform_id: String,
    platform_display: String,
    show_push_notification: bool,
    req_id: String,
}

impl IqSpec for CompanionHelloSpec {
    type Response = Vec<u8>;

    fn build_iq(&self) -> InfoQuery<'static> {
        let iq_node = PairCodeUtils::build_companion_hello_iq(
            &self.phone,
            &self.noise_static_pub,
            &self.wrapped_ephemeral,
            &self.platform_id,
            &self.platform_display,
            self.show_push_notification,
            self.req_id.clone(),
        );

        let children = iq_node
            .children()
            .map(|c: &[Node]| c.to_vec())
            .unwrap_or_default();

        InfoQuery {
            query_type: InfoQueryType::Set,
            namespace: "md",
            to: Jid::new("", Server::Pn),
            target: None,
            content: Some(NodeContent::Nodes(children)),
            id: Some(self.req_id.clone()),
            timeout: Some(std::time::Duration::from_secs(30)),
        }
    }

    fn parse_response(&self, response: &NodeRef<'_>) -> anyhow::Result<Self::Response> {
        PairCodeUtils::parse_companion_hello_response(response)
            .ok_or_else(|| anyhow!("server response missing pairing ref"))
    }
}
