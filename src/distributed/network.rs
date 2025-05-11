use bincode;
use crossbeam_channel as channel;
use omnipaxos::messages::Message;
use omnipaxos::util::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::{Channel, Endpoint};
use tokio::runtime::Runtime;

use crate::distributed::paxos_service::paxos_service_client::PaxosServiceClient;
use crate::distributed::paxos_service::ConsensusMessage;
use crate::distributed::ReplicatedCommit;

/// Network implementation for OmniPaxos using gRPC.
#[cfg(feature = "distributed")]
pub struct KhonsuNetwork {
    // Map of NodeId to gRPC client for that node.
    clients: HashMap<NodeId, PaxosServiceClient<Channel>>,
    // Receiver for messages from the local gRPC server (messages from other nodes).
    receiver: channel::Receiver<Message<ReplicatedCommit>>,
    // Local node ID
    node_id: NodeId,
    // Tokio runtime for async operations
    runtime: Arc<Runtime>,
    // Outgoing messages buffer
    outgoing_buffer: Vec<Message<ReplicatedCommit>>,
}

#[cfg(feature = "distributed")]
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
        let mut clients = HashMap::new();
        for (peer_id, addr) in peer_addrs {
            // Skip creating a client for the local node
            if peer_id == node_id {
                continue;
            }
            
            // Create a gRPC channel and client for each peer.
            let endpoint = match Endpoint::from_shared(addr.clone()) {
                Ok(endpoint) => endpoint,
                Err(e) => {
                    eprintln!("Invalid gRPC endpoint address {}: {}", addr, e);
                    continue;
                }
            };
            
            // Use the provided runtime to connect to the endpoint
            let client = match runtime.block_on(PaxosServiceClient::connect(endpoint)) {
                Ok(client) => client,
                Err(e) => {
                    eprintln!("Failed to connect to gRPC endpoint {}: {}", addr, e);
                    continue;
                }
            };
            
            clients.insert(peer_id, client);
        }

        Self { 
            clients, 
            receiver, 
            node_id,
            runtime,
            outgoing_buffer: Vec::new(),
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
        
        // Get the client for the destination node
        let client = match self.clients.get(&receiver_id) {
            Some(client) => client,
            None => return Err(format!("No client found for node {}", receiver_id)),
        };
        
        // Serialize the message
        let message_payload = match bincode::serialize(&message) {
            Ok(payload) => payload,
            Err(e) => return Err(format!("Failed to serialize message: {}", e)),
        };
        
        // Create the gRPC message
        let grpc_message = ConsensusMessage {
            sender_id: self.node_id as u64,
            receiver_id: receiver_id as u64,
            message_payload,
        };
        
        // Send the message asynchronously
        let mut client_clone = client.clone();
        self.runtime.spawn(async move {
            match client_clone.send_message(grpc_message).await {
                Ok(_) => (),
                Err(e) => eprintln!("Failed to send message to node {}: {}", receiver_id, e),
            }
        });
        
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
