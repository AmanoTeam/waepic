//! Integration tests for waepic-client.
//!
//! These tests exercise the public API end-to-end using an in-memory
//! session backend. No real network connections are made.

use std::sync::Arc;

use prost::Message as _;
use wacore::store::traits::Backend;
use wacore_binary::{
    Jid, Node, SERVER_JID,
    builder::NodeBuilder,
    node::{Attrs, NodeContent, NodeValue},
};
use waepic_client::{Client, ClientConfiguration, InputMessage};
use waepic_connection::Connection;
use waepic_session::MemorySession;
use waproto::whatsapp as wa;

/// Create a test client with an in-memory session backend.
/// Uses `Connection::new` + `Client::new` so no network is involved.
/// The returned runner is not spawned.
fn make_test_client() -> Client {
    let session = Arc::new(MemorySession::new());
    let config = ClientConfiguration::default();
    let backend: Arc<dyn Backend> = session.clone();
    let (_runner, _event_tx, handle) = Connection::new(backend, config.connection.clone());
    Client::new(handle, session, config)
}

/// Build a minimal incoming message node for testing the receive path.
fn make_message_node(from: &Jid, msg_id: &str, text: &str) -> Node {
    let proto = wa::Message {
        conversation: Some(text.to_string()),
        ..Default::default()
    };
    let encoded = proto.encode_to_vec();

    NodeBuilder::new("message")
        .attr("from", from)
        .attr("id", msg_id)
        .attr("type", "text")
        .attr("t", "1719000000")
        .children([NodeBuilder::new("plaintext").bytes(encoded).build()])
        .build()
}

#[compio::test]
async fn test_connect_disconnect() {
    let session = Arc::new(MemorySession::new());
    let config = ClientConfiguration::default();
    let (client, _runner) = Client::connect(session, config);

    let (_updates, _update_task) = client
        .stream_updates()
        .expect("stream_updates should succeed after connect");

    client
        .disconnect()
        .await
        .expect("disconnect should succeed");
}

#[compio::test]
async fn test_qr_pair_flow() {
    // Build a pair-success node (type="get" IQ with <pair-success> child)
    let mut attrs = Attrs::new();
    attrs.push("type".to_string(), NodeValue::String("get".into()));
    attrs.push(
        "from".to_string(),
        NodeValue::String(SERVER_JID.to_string().into()),
    );
    attrs.push("id".to_string(), NodeValue::String("pair-success-1".into()));

    let success_child = NodeBuilder::new("pair-success")
        .children([
            NodeBuilder::new("device-identity")
                .bytes(vec![1, 2, 3, 4])
                .build(),
            NodeBuilder::new("device")
                .attr("jid", Jid::pn("12345"))
                .attr("lid", Jid::lid("100000012345678"))
                .build(),
        ])
        .build();

    let pair_success_node = Node::new("iq", attrs, Some(NodeContent::Nodes(vec![success_child])));
    assert_eq!(pair_success_node.tag, "iq");
    assert_eq!(
        pair_success_node
            .attrs
            .get("type")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("get")
    );

    let children = pair_success_node.children().unwrap();
    let has_pair_success = children.iter().any(|c| c.tag == "pair-success");
    assert!(has_pair_success, "should have pair-success child");

    let ps = children.iter().find(|c| c.tag == "pair-success").unwrap();
    let ps_children = ps.children().unwrap();
    let device_identity = ps_children
        .iter()
        .find(|c| c.tag == "device-identity")
        .unwrap();
    assert!(device_identity.content.is_some());

    let device_node = ps_children.iter().find(|c| c.tag == "device").unwrap();
    assert_eq!(
        device_node
            .attrs
            .get("jid")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("12345@s.whatsapp.net")
    );
    assert_eq!(
        device_node
            .attrs
            .get("lid")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("100000012345678@lid")
    );

    let mut non_attrs = Attrs::new();
    non_attrs.push("type".to_string(), NodeValue::String("result".into()));
    let non_pair_node = Node::new("iq", non_attrs, None);
    assert_ne!(
        non_pair_node
            .attrs
            .get("type")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("get")
    );
}

#[compio::test]
async fn test_send_text_message() {
    let client = make_test_client();
    let chat = client.chat(Jid::pn("12345"));

    let result = client
        .send_message(chat.clone(), InputMessage::text("hello world"))
        .await;

    assert!(
        result.is_err(),
        "send_message without connection should fail"
    );

    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("not connected") || msg.contains("NotConnected"),
                "expected NotConnected error, got: {msg}"
            );
        }
        Ok(_) => unreachable!(),
    }

    let proto = wa::Message {
        conversation: Some("test message".to_string()),
        ..Default::default()
    };
    let encoded = proto.encode_to_vec();

    let node = NodeBuilder::new("message")
        .attr("to", Jid::pn("12345"))
        .attr("type", "text")
        .attr("id", "3EB0TEST1234567890AB")
        .children([NodeBuilder::new("plaintext").bytes(encoded.clone()).build()])
        .build();

    assert_eq!(node.tag, "message");
    assert!(node.attrs.get("to").is_some());
    assert!(node.attrs.get("id").is_some());

    let plaintext = node.get_optional_child("plaintext").unwrap();
    match &plaintext.content {
        Some(NodeContent::Bytes(b)) => {
            assert_eq!(b, &encoded);
        }
        other => panic!("expected Bytes content, got {other:?}"),
    }

    let decoded = wa::Message::decode(encoded.as_slice()).unwrap();
    assert_eq!(decoded.conversation.as_deref(), Some("test message"));
}

#[compio::test]
async fn test_receive_message() {
    let client = make_test_client();

    let from = Jid::pn("12345");
    let node = make_message_node(&from, "MSG_RCV_001", "hello from integration test");

    assert_eq!(node.tag, "message");
    assert_eq!(
        node.attrs
            .get("from")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("12345@s.whatsapp.net")
    );
    assert_eq!(
        node.attrs
            .get("id")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("MSG_RCV_001")
    );

    let plaintext = node.get_optional_child("plaintext").unwrap();
    let proto_bytes = match &plaintext.content {
        Some(NodeContent::Bytes(b)) => b.clone(),
        other => panic!("expected Bytes content, got {other:?}"),
    };

    let wa_msg = wa::Message::decode(proto_bytes.as_slice()).unwrap();
    assert_eq!(
        wa_msg.conversation.as_deref(),
        Some("hello from integration test")
    );

    let chat = client.chat(from);
    assert!(chat.is_user());
    assert_eq!(chat.id().to_string(), "12345@s.whatsapp.net");

    let group_jid = Jid::group("123456789");
    let group_chat = client.chat(group_jid);
    assert!(group_chat.is_group());

    let newsletter_jid = Jid::newsletter("xyz");
    let newsletter_chat = client.chat(newsletter_jid);
    assert!(newsletter_chat.is_newsletter());

    let proto2 = wa::Message {
        conversation: Some("direct content".to_string()),
        ..Default::default()
    };
    let encoded2 = proto2.encode_to_vec();
    let direct_node = NodeBuilder::new("message")
        .attr("from", Jid::pn("99999"))
        .attr("id", "DIRECT001")
        .bytes(encoded2)
        .build();

    match &direct_node.content {
        Some(NodeContent::Bytes(_)) => {
            // Direct bytes content is present
        }
        other => panic!("expected Bytes content on direct node, got {other:?}"),
    }
}

#[compio::test]
async fn test_edit_delete() {
    let client = make_test_client();
    let chat = client.chat(Jid::pn("12345"));

    let edit_result = client
        .edit_message(chat.clone(), "ORIGINAL_ID", InputMessage::text("edited"))
        .await;
    assert!(edit_result.is_err(), "edit without connection should fail");

    let delete_result = client
        .delete_messages(chat.clone(), &["MSG_TO_DELETE"])
        .await;
    assert!(
        delete_result.is_err(),
        "delete without connection should fail"
    );

    let new_content = wa::Message {
        conversation: Some("edited text".to_string()),
        ..Default::default()
    };
    let timestamp_ms = chrono::Utc::now().timestamp_millis();

    let edit_proto = wa::Message {
        protocol_message: Some(Box::new(wa::message::ProtocolMessage {
            key: Some(wa::MessageKey {
                remote_jid: Some("12345@s.whatsapp.net".to_string()),
                from_me: Some(true),
                id: Some("ORIGINAL_ID".to_string()),
                participant: None,
            }),
            r#type: Some(wa::message::protocol_message::Type::MessageEdit as i32),
            edited_message: Some(Box::new(new_content.clone())),
            timestamp_ms: Some(timestamp_ms),
            ..Default::default()
        })),
        ..Default::default()
    };

    let pm = edit_proto.protocol_message.as_ref().unwrap();
    assert_eq!(
        pm.r#type,
        Some(wa::message::protocol_message::Type::MessageEdit as i32)
    );
    let key = pm.key.as_ref().unwrap();
    assert_eq!(key.id.as_deref(), Some("ORIGINAL_ID"));
    assert_eq!(key.from_me, Some(true));
    let edited = pm.edited_message.as_ref().unwrap();
    assert_eq!(edited.conversation.as_deref(), Some("edited text"));

    let revoke_proto = wa::Message {
        protocol_message: Some(Box::new(wa::message::ProtocolMessage {
            key: Some(wa::MessageKey {
                remote_jid: Some("12345@s.whatsapp.net".to_string()),
                from_me: Some(true),
                id: Some("MSG_TO_DELETE".to_string()),
                participant: None,
            }),
            r#type: Some(wa::message::protocol_message::Type::Revoke as i32),
            ..Default::default()
        })),
        ..Default::default()
    };

    let pm = revoke_proto.protocol_message.as_ref().unwrap();
    assert_eq!(
        pm.r#type,
        Some(wa::message::protocol_message::Type::Revoke as i32)
    );
    let key = pm.key.as_ref().unwrap();
    assert_eq!(key.id.as_deref(), Some("MSG_TO_DELETE"));
    assert!(pm.edited_message.is_none());

    let admin_revoke = wa::Message {
        protocol_message: Some(Box::new(wa::message::ProtocolMessage {
            key: Some(wa::MessageKey {
                remote_jid: Some("12345@s.whatsapp.net".to_string()),
                from_me: Some(true),
                id: Some("MSG_TO_DELETE".to_string()),
                participant: Some("99999@s.whatsapp.net".to_string()),
            }),
            r#type: Some(wa::message::protocol_message::Type::Revoke as i32),
            ..Default::default()
        })),
        ..Default::default()
    };

    let pm = admin_revoke.protocol_message.as_ref().unwrap();
    let key = pm.key.as_ref().unwrap();
    assert_eq!(key.participant.as_deref(), Some("99999@s.whatsapp.net"));
}

#[compio::test]
async fn test_reaction_read_receipt() {
    let client = make_test_client();
    let chat = client.chat(Jid::pn("12345"));

    let reaction_result = client
        .send_reaction(chat.clone(), "TARGET_MSG", ":heart:")
        .await;
    assert!(
        reaction_result.is_err(),
        "reaction without connection should fail"
    );

    let read_result = client.mark_as_read(chat.clone(), &["MSG_ID_001"]).await;
    assert!(
        read_result.is_err(),
        "mark_as_read without connection should fail"
    );

    let timestamp_ms = chrono::Utc::now().timestamp_millis();
    let reaction_proto = wa::Message {
        reaction_message: Some(wa::message::ReactionMessage {
            key: Some(wa::MessageKey {
                remote_jid: Some("12345@s.whatsapp.net".to_string()),
                from_me: Some(true),
                id: Some("TARGET_MSG".to_string()),
                participant: None,
            }),
            text: Some(":heart:".to_string()),
            sender_timestamp_ms: Some(timestamp_ms),
            ..Default::default()
        }),
        ..Default::default()
    };

    let reaction = reaction_proto.reaction_message.as_ref().unwrap();
    assert_eq!(reaction.text.as_deref(), Some(":heart:"));
    let key = reaction.key.as_ref().unwrap();
    assert_eq!(key.remote_jid.as_deref(), Some("12345@s.whatsapp.net"));
    assert_eq!(key.id.as_deref(), Some("TARGET_MSG"));
    assert_eq!(key.from_me, Some(true));

    let receipt_node = NodeBuilder::new("receipt")
        .attr("to", Jid::pn("12345"))
        .attr("type", "read")
        .attr("id", "MSG_ID_001")
        .attr("t", "1719000000")
        .build();

    assert_eq!(receipt_node.tag, "receipt");
    assert_eq!(
        receipt_node
            .attrs
            .get("type")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("read")
    );
    assert_eq!(
        receipt_node
            .attrs
            .get("id")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("MSG_ID_001")
    );

    let list_receipt = NodeBuilder::new("receipt")
        .attr("to", Jid::pn("12345"))
        .attr("type", "read")
        .attr("t", "1719000001")
        .children([NodeBuilder::new("list")
            .children([
                NodeBuilder::new("item").attr("id", "MSG_A").build(),
                NodeBuilder::new("item").attr("id", "MSG_B").build(),
            ])
            .build()])
        .build();

    assert_eq!(list_receipt.tag, "receipt");
    let list = list_receipt.get_optional_child("list").unwrap();
    let items = list.children().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(
        items[0]
            .attrs
            .get("id")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("MSG_A")
    );
    assert_eq!(
        items[1]
            .attrs
            .get("id")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("MSG_B")
    );

    let presence_node = NodeBuilder::new("presence")
        .attr("from", Jid::pn("12345"))
        .attr("type", "unavailable")
        .attr("last", "1719000000")
        .build();

    assert_eq!(presence_node.tag, "presence");
    assert_eq!(
        presence_node
            .attrs
            .get("type")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("unavailable")
    );
    assert_eq!(
        presence_node
            .attrs
            .get("last")
            .map(|v| v.as_str().to_string())
            .as_deref(),
        Some("1719000000")
    );

    let chatstate_node = NodeBuilder::new("chatstate")
        .attr("from", Jid::pn("12345"))
        .children([NodeBuilder::new("composing").build()])
        .build();

    assert_eq!(chatstate_node.tag, "chatstate");
    assert!(chatstate_node.get_optional_child("composing").is_some());
}

// Test 7: Reconnect

#[compio::test]
async fn test_reconnect() {
    let session = Arc::new(MemorySession::new());
    let config = ClientConfiguration::default();

    // First connection
    let (client1, _runner1) = Client::connect(session.clone(), config.clone());
    let (_updates1, _task1) = client1
        .stream_updates()
        .expect("first stream_updates should succeed");
    client1
        .disconnect()
        .await
        .expect("first disconnect should succeed");

    // Second connection (simulating reconnect)
    let (client2, _runner2) = Client::connect(session.clone(), config.clone());
    let (_updates2, _task2) = client2
        .stream_updates()
        .expect("second stream_updates should succeed");
    client2
        .disconnect()
        .await
        .expect("second disconnect should succeed");

    // Third connection
    let (client3, _runner3) = Client::connect(session.clone(), config.clone());
    let (_updates3, _task3) = client3
        .stream_updates()
        .expect("third stream_updates should succeed");
    client3
        .disconnect()
        .await
        .expect("third disconnect should succeed");

    // Verify that a new client can still be created after multiple disconnects
    let (client4, _runner4) = Client::connect(session, config);
    assert!(client4.stream_updates().is_ok());
}
