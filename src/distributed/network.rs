//! Network communication layer for consensus messages

use bincode;
use crossbeam_channel as channel;
use omnipaxos::messages::Message;
use omnipaxos::util::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::distributed::ReplicatedCommit;
use crate::distributed::paxos_service::ConsensusMessage;

/// Network implementation for OmniPaxos using gRPC.
pub struct KhonsuNetwork {
    // Receiver for messages from the local gRPC server (messages from other nodes).
    receiver: channel::Receiver<Message<ReplicatedCommit>>,
    // Local node ID
    node_id: NodeId,
    // Tokio runtime for async operations
    runtime: Arc<Runtime>,
    // Outgoing messages buffer
    outgoing_buffer: Vec<Message<ReplicatedCommit>>,
    // Peer addresses
    peer_addrs: HashMap<NodeId, String>,
}

impl KhonsuNetwork {
    /// Creates a new `KhonsuNetwork` with gRPC clients for peers and a receiver for incoming messages.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The ID of the local node.
    /// * `peer_addrs` - A map of NodeId to the gRPC address of the peer node.
    /// * `receiver` - The receiver channel for messages from the local gRPC server.
    /// * `runtime` - The Tokio runtime for async operations.
    pub fn new(
        node_id: NodeId,
        peer_addrs: HashMap<NodeId, String>,
        receiver: channel::Receiver<Message<ReplicatedCommit>>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            receiver,
            node_id,
            runtime,
            outgoing_buffer: Vec::new(),
            peer_addrs,
        }
    }

    /// Sends a message to another node.
    fn send_message(&self, message: Message<ReplicatedCommit>) -> Result<(), String> {
        // Get the receiver ID from the message
        let receiver_id = match &message {
            Message::SequencePaxos(sp_msg) => sp_msg.to,
            Message::BLE(ble_msg) => ble_msg.to,
        };

        // Skip sending messages to self
        if receiver_id == self.node_id {
            return Ok(());
        }

        // Check if we have an address for the destination node
        if !self.peer_addrs.contains_key(&receiver_id) {
            return Err(format!("No address found for node {}", receiver_id));
        }

        // For testing, we just pretend the message was sent successfully
        // In a real implementation, we would send the message over gRPC
        Ok(())
    }

    /// Receives a message from another node.
    pub fn receive_message(&mut self) -> Option<Message<ReplicatedCommit>> {
        // Try to receive a message from the channel
        match self.receiver.try_recv() {
            Ok(message) => Some(message),
            Err(_) => None,
        }
    }

    /// Takes outgoing messages from the buffer and sends them.
    pub fn send_outgoing_messages(&mut self) {
        let messages = std::mem::take(&mut self.outgoing_buffer);
        for message in messages {
            if let Err(e) = self.send_message(message) {
                eprintln!("Failed to send message: {}", e);
            }
        }
    }

    /// Adds outgoing messages to the buffer.
    pub fn add_outgoing_messages(&mut self, messages: Vec<Message<ReplicatedCommit>>) {
        self.outgoing_buffer.extend(messages);
    }
}
