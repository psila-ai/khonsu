use bincode;
use crossbeam_channel as channel;
use omnipaxos::messages::Message;
use omnipaxos::util::NodeId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::transport::{Channel, Endpoint};

use crate::distributed::paxos_service::paxos_service_client::PaxosServiceClient;
use crate::distributed::paxos_service::ConsensusMessage;
use crate::distributed::ReplicatedCommit;

#[cfg(feature = "distributed")]
pub struct KhonsuNetwork {
    // Map of NodeId to gRPC client for that node.
    clients: HashMap<NodeId, PaxosServiceClient<Channel>>,
    // Receiver for messages from the local gRPC server (messages from other nodes).
    receiver: channel::Receiver<Message<ReplicatedCommit>>,
}

#[cfg(feature = "distributed")]
impl KhonsuNetwork {
    /// Creates a new `KhonsuNetwork` with gRPC clients for peers and a receiver for incoming messages.
    ///
    /// # Arguments
    ///
    /// * `peer_addrs` - A map of NodeId to the gRPC address of the peer node.
    /// * `receiver` - The receiver channel for messages from the local gRPC server.
    pub fn new(
        peer_addrs: HashMap<NodeId, String>,
        receiver: channel::Receiver<Message<ReplicatedCommit>>,
    ) -> Self {
        let mut clients = HashMap::new();
        for (node_id, addr) in peer_addrs {
            // Create a gRPC channel and client for each peer.
            // This is a simplified example; real-world scenarios might need
            // more sophisticated channel management and error handling.
            let endpoint = Endpoint::from_shared(addr).expect("Invalid gRPC endpoint address"); // TODO: Handle errors properly
                                                                                                // Note: Connecting is an async operation. We'll block for now,
                                                                                                // but this needs to be addressed when integrating with the async runtime.
            let client = futures::executor::block_on(PaxosServiceClient::connect(endpoint))
                .expect("Failed to connect to gRPC endpoint"); // TODO: Handle errors properly
            clients.insert(node_id, client);
        }

        Self { clients, receiver }
    }
}