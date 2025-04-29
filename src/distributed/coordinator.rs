use omnipaxos::{core::OmniPaxos, util::NodeId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::Result; // Using anyhow for a simple error type
use futures::{Future, future::join_all}; // For asynchronous communication and joining futures

use crate::twopc::{TransactionChanges, TwoPhaseCommitParticipant}; // Import necessary types from the twopc module
use crate::distributed::storage::DistributedCommitStorage; // Import DistributedCommitStorage
use crate::Khonsu; // Import local Khonsu instance

/// A unique identifier for a global distributed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlobalTransactionId(u64); // Using a simple u64 for now, will refine later

/// The state of a global distributed transaction in the 2PC protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GlobalTransactionState {
    Initiated,
    Prepared,
    Committed,
    Aborted,
}

/// An entry in the OmniPaxos log representing a global transaction event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TwoPhaseCommitLogEntry {
    /// A new global transaction has been initiated.
    Initiate {
        global_tx_id: GlobalTransactionId,
        participants: Vec<NodeId>,
        changes: HashMap<NodeId, TransactionChanges>, // Changes for each participant
    },
    /// All participants have prepared successfully.
    Prepared {
        global_tx_id: GlobalTransactionId,
    },
    /// The global transaction has been committed.
    Committed {
        global_tx_id: GlobalTransactionId,
    },
    /// The global transaction has been aborted.
    Aborted {
        global_tx_id: GlobalTransactionId,
    },
}


use std::sync::Arc; // For sharing participants

/// Trait for interacting with a remote participant in the two-phase commit protocol.
pub trait RemoteParticipant: Send + Sync {
    /// Sends a prepare request to the remote participant.
    fn prepare(
        &self,
        global_tx_id: GlobalTransactionId,
        changes: TransactionChanges,
    ) -> impl Future<Output = Result<bool>> + Send;

    /// Sends a commit request to the remote participant.
    fn commit(
        &self,
        global_tx_id: GlobalTransactionId,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Sends an abort request to the remote participant.
    fn abort(
        &self,
        global_tx_id: GlobalTransactionId,
    ) -> impl Future<Output = Result<()>> + Send;
}


/// Coordinates the two-phase commit protocol across multiple participants.
pub struct TwoPhaseCommitCoordinator {
    omni_paxos: OmniPaxos<TwoPhaseCommitLogEntry, (), ()>, // Log entry type is TwoPhaseCommitLogEntry
    next_global_tx_id: AtomicU64, // Simple counter for generating unique IDs
    participants: HashMap<NodeId, Box<dyn RemoteParticipant>>, // Map of participant NodeId to their RemoteParticipant implementation
    local_khonsu: Arc<Khonsu>, // Add field for the local Khonsu instance
    // TODO: Add fields for communication layer
}

impl TwoPhaseCommitCoordinator {
    /// Creates a new `TwoPhaseCommitCoordinator`.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The ID of the local node.
    /// * `peers` - A list of peer node IDs in the OmniPaxos cluster.
    /// * `participants` - A map of remote participant NodeIds to their `RemoteParticipant` implementations.
    /// * `local_khonsu` - The local Khonsu instance participating in 2PC.
    pub fn new(
        node_id: NodeId,
        peers: Vec<NodeId>,
        participants: HashMap<NodeId, Box<dyn RemoteParticipant>>,
        local_khonsu: Arc<Khonsu>, // Add parameter for local Khonsu instance
    ) -> Self {
        // Configure OmniPaxos with the correct storage and parameters
        let storage = DistributedCommitStorage::new(); // Create an instance of our storage
        let omni_paxos = OmniPaxos::new(node_id, peers, Some(storage), None); // Use our storage

        Self {
            omni_paxos,
            next_global_tx_id: AtomicU64::new(0),
            participants,
            local_khonsu, // Store the local Khonsu instance
            // TODO: Initialize other fields, including the communication layer
        }
    }

use futures::executor::block_on; // For waiting on futures in a synchronous context (for now)
use omnipaxos::messages::Message; // For polling OmniPaxos

    /// Initiates a new global distributed transaction.
    ///
    /// This involves generating a unique transaction ID and proposing an
    /// `Initiate` log entry to OmniPaxos.
        pub fn initiate_global_transaction(
        &mut self, // Need mutable self to interact with OmniPaxos
        participants: Vec<NodeId>,
        changes: HashMap<NodeId, TransactionChanges>,
    ) -> Result<GlobalTransactionId, Box<dyn std::error::Error>> { // TODO: Define a proper error type
        let global_tx_id = GlobalTransactionId(self.next_global_tx_id.fetch_add(1, Ordering::SeqCst));
        let log_entry = TwoPhaseCommitLogEntry::Initiate {
            global_tx_id,
            participants,
            changes,
        };

        // Propose the log entry to OmniPaxos
        self.omni_paxos.append(log_entry)?;

        // TODO: In a real implementation, this would be part of an event loop
        // processing OmniPaxos messages. For now, we'll block and poll.
        loop {
            // TODO: Process incoming messages for OmniPaxos
            // let messages: Vec<Message<TwoPhaseCommitLogEntry, (), ()>> = self.receive_messages(); // Placeholder

            // for msg in messages {
            //     self.omni_paxos.handle(msg);
            // }

            // Check for decided entries
            if let Some(decided_entries) = self.omni_paxos.next_decided_entry() {
                for entry in decided_entries {
                    if let TwoPhaseCommitLogEntry::Initiate { global_tx_id: decided_tx_id, .. } = entry {
                        if decided_tx_id == global_tx_id {
                            println!("Initiate log entry for transaction {:?} decided.", global_tx_id);
                            return Ok(global_tx_id);
                        }
                    }
                }
            }

            // TODO: Add a small delay to avoid busy-waiting
            // std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Runs the prepare phase of a global distributed transaction.
    ///
    /// This involves sending prepare requests to all participants, collecting
    /// their votes, and proposing the outcome to OmniPaxos.
        pub async fn run_prepare_phase(
        &mut self, // Need mutable self to interact with OmniPaxos
        global_tx_id: GlobalTransactionId,
        changes: HashMap<NodeId, TransactionChanges>,
        local_khonsu: &Khonsu, // Need access to the local Khonsu instance
    ) -> Result<()> { // TODO: Define a proper error type
        let mut prepare_futures = Vec::new();
        let mut participant_nodes = Vec::new();

        for (node_id, participant_changes) in changes {
            participant_nodes.push(node_id);
            if let Some(remote_participant) = self.participants.get(&node_id) {
                // Remote participant
                let future = remote_participant.prepare(global_tx_id, participant_changes);
                prepare_futures.push(future);
            } else {
                // Local participant
                let local_khonsu_clone = Arc::clone(&self.local_khonsu);
                let future = async move {
                    println!("Local participant preparing transaction {:?}", global_tx_id);
                    local_khonsu_clone.prepare_transaction(global_tx_id, participant_changes)
                        .map_err(|e| anyhow::anyhow!("Local participant prepare failed: {:?}", e)) // Convert ParticipantError to anyhow::Result
                };
                prepare_futures.push(Box::pin(future)); // Box and pin the future
            }
        }

        // Wait for all participants to respond
        let results = join_all(prepare_futures).await;

        let all_prepared = results.iter().all(|res| matches!(res, Ok(true)));

        let log_entry = if all_prepared {
            println!("All participants prepared for transaction {:?}.", global_tx_id);
            TwoPhaseCommitLogEntry::Prepared { global_tx_id }
        } else {
            println!("One or more participants failed to prepare for transaction {:?}. Aborting.", global_tx_id);
            TwoPhaseCommitLogEntry::Aborted { global_tx_id }
        };

        // Propose the outcome log entry to OmniPaxos
        self.omni_paxos.append(log_entry.clone())?; // Clone to use in the loop

        // Wait for the log entry to be decided by OmniPaxos
        // TODO: In a real implementation, this would be part of an event loop
        // processing OmniPaxos messages. For now, we'll block and poll.
        loop {
            // TODO: Process incoming messages for OmniPaxos
            // let messages: Vec<Message<TwoPhaseCommitLogEntry, (), ()>> = self.receive_messages(); // Placeholder

            // for msg in messages {
            //     self.omni_paxos.handle(msg);
            // }

            // Check for decided entries
            if let Some(decided_entries) = self.omni_paxos.next_decided_entry() {
                for entry in decided_entries {
                    if entry == log_entry {
                        println!("Outcome log entry for transaction {:?} decided.", global_tx_id);
                        return Ok(());
                    }
                }
            }

            // TODO: Add a small delay to avoid busy-waiting
            // std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Runs the commit phase of a global distributed transaction.
    ///
    /// Runs the commit phase of a global distributed transaction.
    ///
    /// This involves sending commit requests to all participants and waiting
    /// for their acknowledgments.
        pub async fn run_commit_phase(
        &self,
        global_tx_id: GlobalTransactionId,
        // local_khonsu: &Khonsu, // Removed as we use self.local_khonsu
    ) -> Result<()> { // TODO: Define a proper error type
        let mut commit_futures = Vec::new();

        for (node_id, remote_participant) in &self.participants {
            // Remote participant
            let future = remote_participant.commit(global_tx_id);
            commit_futures.push(future);
        }

        // Local participant
        let local_khonsu_clone = Arc::clone(&self.local_khonsu);
        let local_future = async move {
            println!("Local participant committing transaction {:?}", global_tx_id);
            local_khonsu_clone.commit_transaction(global_tx_id)
                .map_err(|e| anyhow::anyhow!("Local participant commit failed: {:?}", e)) // Convert ParticipantError to anyhow::Result
        };
        commit_futures.push(Box::pin(local_future)); // Box and pin the future


        // Wait for all participants to acknowledge commit
        let results = join_all(commit_futures).await;

        // TODO: Check results for errors and handle accordingly.
        // If any participant failed to commit, this is a serious error in 2PC.

        println!("All participants acknowledged commit for transaction {:?}.", global_tx_id);

        // TODO: Optionally log the completion of the commit phase to OmniPaxos.

        Ok(())
    }

    /// Runs the abort phase of a global distributed transaction.
    ///
    /// Runs the abort phase of a global distributed transaction.
    ///
    /// This involves sending abort requests to all participants and waiting
    /// for their acknowledgments.
        pub async fn run_abort_phase(
        &self,
        global_tx_id: GlobalTransactionId,
        // local_khonsu: &Khonsu, // Removed as we use self.local_khonsu
    ) -> Result<()> { // TODO: Define a proper error type
        let mut abort_futures = Vec::new();

        for (node_id, remote_participant) in &self.participants {
            // Remote participant
            let future = remote_participant.abort(global_tx_id);
            abort_futures.push(future);
        }

        // Local participant
        let local_khonsu_clone = Arc::clone(&self.local_khonsu);
        let local_future = async move {
            println!("Local participant aborting transaction {:?}", global_tx_id);
            local_khonsu_clone.abort_transaction(global_tx_id)
                .map_err(|e| anyhow::anyhow!("Local participant abort failed: {:?}", e)) // Convert ParticipantError to anyhow::Result
        };
        abort_futures.push(Box::pin(local_future)); // Box and pin the future


        // Wait for all participants to acknowledge abort
        let results = join_all(abort_futures).await;

        // TODO: Check results for errors and handle accordingly.
        // If any participant failed to abort, this is a serious error in 2PC.

        println!("All participants acknowledged abort for transaction {:?}.", global_tx_id);

        // TODO: Optionally log the completion of the abort phase to OmniPaxos.

        Ok(())
    }
}
