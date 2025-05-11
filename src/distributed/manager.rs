use crossbeam_channel::{bounded, Receiver, Sender};
use omnipaxos::{
    messages::Message, util::NodeId, ClusterConfig, OmniPaxos, OmniPaxosConfig
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::data_store::txn_buffer::TxnBuffer; // Need to apply decided entries
use crate::dependency_tracking::DependencyTracker; // Need to update tracker on decided entries
use crate::distributed::{
    grpc_server::start_grpc_server, // Import the gRPC server startup function
    network::KhonsuNetwork,
    storage::DistributedCommitStorage,
    ReplicatedCommit,
};
use crate::storage::Storage; // Need to persist decided entries

/// Manages the distributed commit process using OmniPaxos and gRPC.
pub struct DistributedCommitManager {
    omni_paxos: OmniPaxos<ReplicatedCommit, DistributedCommitStorage>,
    node_id: NodeId,
    cluster_config: ClusterConfig,
    // Channels for communication with the OmniPaxos event loop and gRPC server/clients
    omni_paxos_sender: Sender<Message<ReplicatedCommit>>, // Sender to OmniPaxos event loop
    omni_paxos_receiver: Receiver<Message<ReplicatedCommit>>, // Receiver from OmniPaxos event loop (outgoing messages)
    grpc_server_handle: JoinHandle<()>, // Handle for the gRPC server thread/task
    // TODO: Add channels for communication between main thread and manager/event loop
    // TODO: Add gRPC client handles if needed for direct calls from manager
    // TODO: Add references to local Khonsu components (TxnBuffer, DependencyTracker, Storage)
    local_txn_buffer: Arc<TxnBuffer>,
    local_dependency_tracker: Arc<DependencyTracker>,
    local_storage: Arc<dyn Storage>,
}

impl DistributedCommitManager {
    /// Creates and starts a new `DistributedCommitManager`.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The ID of the local node.
    /// * `cluster_config` - The configuration of the cluster.
    /// * `peer_addrs` - A map of NodeId to the gRPC address of the peer node.
    /// * `storage_path` - The path for the RocksDB storage.
    /// * `local_txn_buffer` - Reference to the local transaction buffer.
    /// * `local_dependency_tracker` - Reference to the local dependency tracker.
    /// * `local_storage` - Reference to the local storage implementation.
    pub fn new(
        node_id: NodeId,
        cluster_config: ClusterConfig,
        peer_addrs: HashMap<NodeId, String>,
        storage_path: &Path,
        local_txn_buffer: Arc<TxnBuffer>,
        local_dependency_tracker: Arc<DependencyTracker>,
        local_storage: Arc<dyn Storage>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let omnipaxos_config = OmniPaxosConfig {
            cluster_config: cluster_config.clone(),
            server_config: omnipaxos::ServerConfig {
                pid: node_id,
                ..Default::default()
            },
            ..Default::default()
        };

        let storage = DistributedCommitStorage::new(storage_path)?; // Our RocksDB storage

        // Channels for communication:
        // - Main thread -> OmniPaxos event loop (proposals, messages)
        // - OmniPaxos event loop -> Main thread (decided entries, status)
        // - OmniPaxos event loop -> gRPC server (outgoing messages)
        // - gRPC server -> OmniPaxos event loop (incoming messages)

        // Channel for messages from gRPC server to OmniPaxos event loop
        let (grpc_server_sender, omni_paxos_receiver_from_grpc) =
            bounded::<Message<ReplicatedCommit>>(1024); // TODO: Choose appropriate buffer size

        // Channel for messages from OmniPaxos event loop to gRPC network (outgoing)
        let (omni_paxos_sender_to_network, network_receiver_from_omnipaxos) =
            bounded::<Message<ReplicatedCommit>>(1024); // TODO: Choose appropriate buffer size

        // Channel for messages from main thread to OmniPaxos event loop (proposals)
        let (omni_paxos_sender, omni_paxos_receiver) =
            bounded::<Message<ReplicatedCommit>>(1024); // TODO: Choose appropriate buffer size

        // Create the network layer (gRPC clients)
        let network = KhonsuNetwork::new(
            peer_addrs, /* TODO: Need a receiver for incoming messages from the gRPC server */
        ); // This needs to be connected to the gRPC server

        let mut omnipaxos =
            OmniPaxos::new(omnipaxos_config.cluster_config.clone(), storage, network);

        // Start the gRPC server in a separate Tokio runtime thread
        let grpc_server_addr: SocketAddr = format!("127.0.0.1:50051").parse()?; // TODO: Configure actual address
        let grpc_server_handle = std::thread::spawn(move || {
            futures::executor::block_on(async {
                // The gRPC server needs a sender to the OmniPaxos event loop.
                // This sender should be the `omni_paxos_receiver_from_grpc`'s corresponding sender.
                // This requires careful channel management.
                // For now, let's pass the `grpc_server_sender` to the server startup function.
                if let Err(e) = start_grpc_server(grpc_server_addr, grpc_server_sender).await {
                    eprintln!("gRPC server failed: {:?}", e);
                }
            });
        });

        // Spawn a thread for the OmniPaxos event loop
        let omni_paxos_handle = std::thread::spawn(move || {
            // The OmniPaxos event loop needs:
            // - Receiver for messages from gRPC server (`omni_paxos_receiver_from_grpc`)
            // - Receiver for messages from main thread (`omni_paxos_receiver`)
            // - Sender for outgoing messages to network (`omni_paxos_sender_to_network`)
            // - Access to local Khonsu components to apply decided entries.

            // TODO: Implement the OmniPaxos event loop here
            // This loop will receive messages from the network and the Khonsu instance,
            // pass them to omnipaxos.handle(), call omnipaxos.step(),
            // send outgoing messages via the network sender, and process decided entries.
            loop {
                // TODO: Receive messages from omni_paxos_receiver_from_grpc and omni_paxos_receiver
                // TODO: omnipaxos.handle(msg);
                // TODO: omnipaxos.step();
                // TODO: Send outgoing messages from omnipaxos.outgoing_messages() via omni_paxos_sender_to_network
                // TODO: Process decided entries from omnipaxos.next_decided_entry() and apply to local Khonsu state
                // TODO: Add a small sleep
            }
        });

        Ok(Self {
            omni_paxos,
            node_id,
            cluster_config,
            omni_paxos_sender,
            omni_paxos_receiver, // This receiver is for messages *from* the main thread *to* the event loop.
            grpc_server_handle,
            local_txn_buffer,
            local_dependency_tracker,
            local_storage,
        })
    }

    // TODO: Add a method for the Khonsu struct to propose a transaction commit.
    // This method will send a ReplicatedCommit to the OmniPaxos event loop via omni_paxos_sender.
    // It will also need a mechanism to wait for the decision.

    // TODO: Add a method to handle incoming messages from the gRPC server (if not using channels directly).
    // This method would be called by the gRPC server implementation.

    // TODO: Add a method to run the OmniPaxos event loop (if not in a separate thread).
}
