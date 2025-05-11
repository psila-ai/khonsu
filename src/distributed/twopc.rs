//! Two-Phase Commit (2PC) protocol implementation over OmniPaxos.
//!
//! This module implements a distributed transaction commit protocol using OmniPaxos
//! as the consensus mechanism. The protocol ensures that all nodes in the cluster
//! either commit or abort a transaction atomically, even in the presence of node failures.

use crate::distributed::{GlobalTransactionId, ReplicatedCommit, TransactionState};
use crate::errors::KhonsuError;
use omnipaxos::util::NodeId;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Represents the phase of a transaction in the 2PC protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TwoPhaseCommitPhase {
    /// The transaction is in the prepare phase.
    Prepare,
    /// The transaction is in the commit phase.
    Commit,
    /// The transaction is in the abort phase.
    Abort,
}

/// Represents the state of a participant in the 2PC protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantState {
    /// The participant has not yet voted.
    Unknown,
    /// The participant has voted to prepare the transaction.
    Prepared,
    /// The participant has voted to abort the transaction.
    Aborted,
    /// The participant has committed the transaction.
    Committed,
}

/// Represents a participant in the 2PC protocol.
#[derive(Debug, Clone)]
pub struct Participant {
    /// The ID of the participant node.
    pub node_id: NodeId,
    /// The state of the participant.
    pub state: ParticipantState,
    /// The timestamp when the participant last updated its state.
    pub timestamp: u64,
}

/// Represents a transaction in the 2PC protocol.
#[derive(Debug, Clone)]
pub struct TwoPhaseCommitTransaction {
    /// The global transaction ID.
    pub transaction_id: GlobalTransactionId,
    /// The ID of the coordinator node.
    pub coordinator_id: NodeId,
    /// The current phase of the transaction.
    pub phase: TwoPhaseCommitPhase,
    /// The participants in the transaction.
    pub participants: HashMap<NodeId, Participant>,
    /// The timestamp when the transaction was created.
    pub creation_timestamp: u64,
    /// The timestamp when the transaction was decided (committed or aborted).
    pub decision_timestamp: Option<u64>,
    /// The replicated commit entry for this transaction.
    pub replicated_commit: Option<ReplicatedCommit>,
}

impl TwoPhaseCommitTransaction {
    /// Creates a new 2PC transaction.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The global transaction ID.
    /// * `coordinator_id` - The ID of the coordinator node.
    /// * `participant_ids` - The IDs of the participant nodes.
    /// * `replicated_commit` - The replicated commit entry for this transaction.
    pub fn new(
        transaction_id: GlobalTransactionId,
        coordinator_id: NodeId,
        participant_ids: Vec<NodeId>,
        replicated_commit: ReplicatedCommit,
    ) -> Self {
        let creation_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut participants = HashMap::new();
        for node_id in participant_ids {
            participants.insert(
                node_id,
                Participant {
                    node_id,
                    state: ParticipantState::Unknown,
                    timestamp: creation_timestamp,
                },
            );
        }

        Self {
            transaction_id,
            coordinator_id,
            phase: TwoPhaseCommitPhase::Prepare,
            participants,
            creation_timestamp,
            decision_timestamp: None,
            replicated_commit: Some(replicated_commit),
        }
    }

    /// Checks if all participants have prepared the transaction.
    pub fn all_prepared(&self) -> bool {
        self.participants
            .values()
            .all(|p| p.state == ParticipantState::Prepared)
    }

    /// Checks if any participant has aborted the transaction.
    pub fn any_aborted(&self) -> bool {
        self.participants
            .values()
            .any(|p| p.state == ParticipantState::Aborted)
    }

    /// Checks if all participants have committed the transaction.
    pub fn all_committed(&self) -> bool {
        self.participants
            .values()
            .all(|p| p.state == ParticipantState::Committed)
    }

    /// Updates the state of a participant.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The ID of the participant node.
    /// * `state` - The new state of the participant.
    pub fn update_participant(
        &mut self,
        node_id: NodeId,
        state: ParticipantState,
    ) -> Result<(), KhonsuError> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if let Some(participant) = self.participants.get_mut(&node_id) {
            participant.state = state;
            participant.timestamp = timestamp;
            Ok(())
        } else {
            Err(KhonsuError::DistributedCommitError(format!(
                "Participant {} not found in transaction {}",
                node_id, self.transaction_id
            )))
        }
    }

    /// Moves the transaction to the commit phase.
    pub fn move_to_commit_phase(&mut self) {
        self.phase = TwoPhaseCommitPhase::Commit;
    }

    /// Moves the transaction to the abort phase.
    pub fn move_to_abort_phase(&mut self) {
        self.phase = TwoPhaseCommitPhase::Abort;
    }

    /// Sets the decision timestamp.
    pub fn set_decision_timestamp(&mut self) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.decision_timestamp = Some(timestamp);
    }

    /// Gets the transaction state based on the 2PC phase.
    pub fn get_transaction_state(&self) -> TransactionState {
        match self.phase {
            TwoPhaseCommitPhase::Prepare => TransactionState::Prepared,
            TwoPhaseCommitPhase::Commit => TransactionState::Committed,
            TwoPhaseCommitPhase::Abort => TransactionState::Aborted,
        }
    }

    /// Gets the replicated commit entry with the current transaction state.
    pub fn get_replicated_commit(&self) -> Option<ReplicatedCommit> {
        self.replicated_commit.clone().map(|mut rc| {
            rc.state = self.get_transaction_state();
            if let Some(timestamp) = self.decision_timestamp {
                rc.decision_timestamp = Some(timestamp);
            }
            rc
        })
    }
}

/// Manages 2PC transactions.
pub struct TwoPhaseCommitManager {
    /// The ID of the local node.
    pub node_id: NodeId,
    /// The active 2PC transactions.
    pub transactions: Arc<RwLock<HashMap<GlobalTransactionId, TwoPhaseCommitTransaction>>>,
    /// The timeout for 2PC transactions.
    pub transaction_timeout: Duration,
}

impl TwoPhaseCommitManager {
    /// Creates a new 2PC manager.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The ID of the local node.
    /// * `transaction_timeout` - The timeout for 2PC transactions.
    pub fn new(node_id: NodeId, transaction_timeout: Duration) -> Self {
        Self {
            node_id,
            transactions: Arc::new(RwLock::new(HashMap::new())),
            transaction_timeout,
        }
    }

    /// Creates a new 2PC transaction.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The global transaction ID.
    /// * `participant_ids` - The IDs of the participant nodes.
    /// * `replicated_commit` - The replicated commit entry for this transaction.
    pub fn create_transaction(
        &self,
        transaction_id: GlobalTransactionId,
        participant_ids: Vec<NodeId>,
        replicated_commit: ReplicatedCommit,
    ) -> Result<(), KhonsuError> {
        let transaction = TwoPhaseCommitTransaction::new(
            transaction_id.clone(),
            self.node_id,
            participant_ids,
            replicated_commit,
        );

        let mut transactions = self.transactions.write();
        if transactions.contains_key(&transaction_id) {
            return Err(KhonsuError::DistributedCommitError(format!(
                "Transaction {} already exists",
                transaction_id
            )));
        }

        transactions.insert(transaction_id, transaction);
        Ok(())
    }

    /// Gets a 2PC transaction.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The global transaction ID.
    pub fn get_transaction(
        &self,
        transaction_id: &GlobalTransactionId,
    ) -> Option<TwoPhaseCommitTransaction> {
        self.transactions.read().get(transaction_id).cloned()
    }

    /// Updates the state of a participant in a 2PC transaction.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The global transaction ID.
    /// * `node_id` - The ID of the participant node.
    /// * `state` - The new state of the participant.
    pub fn update_participant_state(
        &self,
        transaction_id: &GlobalTransactionId,
        node_id: NodeId,
        state: ParticipantState,
    ) -> Result<(), KhonsuError> {
        let mut transactions = self.transactions.write();
        if let Some(transaction) = transactions.get_mut(transaction_id) {
            transaction.update_participant(node_id, state)?;

            // Check if all participants have prepared
            if transaction.phase == TwoPhaseCommitPhase::Prepare && transaction.all_prepared() {
                transaction.move_to_commit_phase();
                transaction.set_decision_timestamp();
            }

            // Check if any participant has aborted
            if transaction.phase == TwoPhaseCommitPhase::Prepare && transaction.any_aborted() {
                transaction.move_to_abort_phase();
                transaction.set_decision_timestamp();
            }

            Ok(())
        } else {
            Err(KhonsuError::DistributedCommitError(format!(
                "Transaction {} not found",
                transaction_id
            )))
        }
    }

    /// Checks for timed out transactions and aborts them.
    pub fn check_timeouts(&self) -> Vec<GlobalTransactionId> {
        let _now = Instant::now();
        let mut timed_out_transactions = Vec::new();

        let mut transactions = self.transactions.write();
        for (transaction_id, transaction) in transactions.iter_mut() {
            let transaction_age = Duration::from_millis(
                (SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64)
                    - transaction.creation_timestamp,
            );

            if transaction_age > self.transaction_timeout
                && transaction.phase == TwoPhaseCommitPhase::Prepare
            {
                transaction.move_to_abort_phase();
                transaction.set_decision_timestamp();
                timed_out_transactions.push(transaction_id.clone());
            }
        }

        timed_out_transactions
    }

    /// Removes a completed transaction.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The global transaction ID.
    pub fn remove_transaction(
        &self,
        transaction_id: &GlobalTransactionId,
    ) -> Result<(), KhonsuError> {
        let mut transactions = self.transactions.write();
        if transactions.remove(transaction_id).is_some() {
            Ok(())
        } else {
            Err(KhonsuError::DistributedCommitError(format!(
                "Transaction {} not found",
                transaction_id
            )))
        }
    }

    /// Gets all transactions in a specific phase.
    ///
    /// # Arguments
    ///
    /// * `phase` - The phase to filter by.
    pub fn get_transactions_in_phase(
        &self,
        phase: TwoPhaseCommitPhase,
    ) -> Vec<TwoPhaseCommitTransaction> {
        self.transactions
            .read()
            .values()
            .filter(|t| t.phase == phase)
            .cloned()
            .collect()
    }

    /// Gets all transactions where this node is the coordinator.
    pub fn get_coordinated_transactions(&self) -> Vec<TwoPhaseCommitTransaction> {
        self.transactions
            .read()
            .values()
            .filter(|t| t.coordinator_id == self.node_id)
            .cloned()
            .collect()
    }

    /// Gets all transactions where this node is a participant.
    pub fn get_participated_transactions(&self) -> Vec<TwoPhaseCommitTransaction> {
        self.transactions
            .read()
            .values()
            .filter(|t| t.participants.contains_key(&self.node_id))
            .cloned()
            .collect()
    }
}
