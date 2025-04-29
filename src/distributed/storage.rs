use omnipaxos::storage::{Entry, Storage};
use omnipaxos::util::{NodeId, SnapshottedEntry};
use omnipaxos::storage::memory_storage::MemoryStorage; // Can use MemoryStorage as a starting point or for testing

use crate::distributed::coordinator::TwoPhaseCommitLogEntry; // Import the log entry type

/// Storage backend for OmniPaxos, persisting log entries to a local WAL.
// TODO: Implement the OmniPaxos Storage trait
pub struct DistributedCommitStorage {
    // TODO: Add fields for the local WAL implementation
    // Using MemoryStorage as a placeholder for now
    inner: MemoryStorage<TwoPhaseCommitLogEntry, (), ()>,
}

impl DistributedCommitStorage {
    /// Creates a new `DistributedCommitStorage`.
    // TODO: Define parameters for the WAL path, etc.
    pub fn new() -> Self {
        Self {
            inner: MemoryStorage::default(), // Placeholder
        }
    }
}

impl Storage<TwoPhaseCommitLogEntry, (), ()> for DistributedCommitStorage {
    // TODO: Implement the required methods of the Storage trait
    // These methods will interact with the local WAL.

    fn append(&mut self, entry: TwoPhaseCommitLogEntry) -> Result<u64, omnipaxos::storage::StorageError> {
        // TODO: Append the entry to the local WAL and the inner storage
        self.inner.append(entry)
    }

    fn append_entries(&mut self, entries: Vec<TwoPhaseCommitLogEntry>) -> Result<u64, omnipaxos::storage::StorageError> {
        // TODO: Append multiple entries to the local WAL and the inner storage
        self.inner.append_entries(entries)
    }

    fn get_entries(&self, from: u64, to: u64) -> Result<Vec<TwoPhaseCommitLogEntry>, omnipaxos::storage::StorageError> {
        // TODO: Read entries from the local WAL or inner storage
        self.inner.get_entries(from, to)
    }

    fn get_latest_entries(&self, n: u64) -> Result<Vec<TwoPhaseCommitLogEntry>, omnipaxos::storage::StorageError> {
        // TODO: Read the latest entries from the local WAL or inner storage
        self.inner.get_latest_entries(n)
    }

    fn get_entry(&self, idx: u64) -> Result<Option<TwoPhaseCommitLogEntry>, omnipaxos::storage::StorageError> {
        // TODO: Read a single entry from the local WAL or inner storage
        self.inner.get_entry(idx)
    }

    fn get_suffix(&self, from: u64) -> Result<Vec<TwoPhaseCommitLogEntry>, omnipaxos::storage::StorageError> {
        // TODO: Read a suffix of the log from the local WAL or inner storage
        self.inner.get_suffix(from)
    }

    fn get_log_len(&self) -> u64 {
        // TODO: Return the length of the log from the local WAL or inner storage
        self.inner.get_log_len()
    }

    fn get_log_start_idx(&self) -> u64 {
        // TODO: Return the start index of the log from the local WAL or inner storage
        self.inner.get_log_start_idx()
    }

    fn get_snapshot(&self) -> Result<Option<SnapshottedEntry<(), ()>>, omnipaxos::storage::StorageError> {
        // TODO: Implement snapshotting if needed, otherwise return None
        self.inner.get_snapshot()
    }

    fn set_snapshot(&mut self, snapshot: SnapshottedEntry<(), ()>) -> Result<(), omnipaxos::storage::StorageError> {
        // TODO: Implement snapshot loading if needed
        self.inner.set_snapshot(snapshot)
    }

    fn get_first_instance(&self) -> Option<omnipaxos::messages::Message<TwoPhaseCommitLogEntry, (), ()>> {
        // TODO: Implement if needed for recovery
        None
    }

    fn set_first_instance(&mut self, instance: omnipaxos::messages::Message<TwoPhaseCommitLogEntry, (), ()>) {
        // TODO: Implement if needed for recovery
    }

    fn get_last_instance(&self) -> Option<omnipaxos::messages::Message<TwoPhaseCommitLogEntry, (), ()>> {
        // TODO: Implement if needed for recovery
        None
    }

    fn set_last_instance(&mut self, instance: omnipaxos::messages::Message<TwoPhaseCommitLogEntry, (), ()>) {
        // TODO: Implement if needed for recovery
    }

    fn get_decided_idx(&self) -> u64 {
        // TODO: Return the decided index from the local WAL or inner storage
        self.inner.get_decided_idx()
    }

    fn set_decided_idx(&mut self, idx: u64) {
        // TODO: Set the decided index in the local WAL or inner storage
        self.inner.set_decided_idx(idx)
    }

    fn get_accepted_round(&self) -> Option<u64> {
        // TODO: Return the accepted round from the local WAL or inner storage
        self.inner.get_accepted_round()
    }

    fn set_accepted_round(&mut self, round: u64) {
        // TODO: Set the accepted round in the local WAL or inner storage
        self.inner.set_accepted_round(round)
    }

    fn get_leader(&self) -> Option<NodeId> {
        // TODO: Return the leader from the local WAL or inner storage
        self.inner.get_leader()
    }

    fn set_leader(&mut self, leader: NodeId) {
        // TODO: Set the leader in the local WAL or inner storage
        self.inner.set_leader(leader)
    }

    fn get_promise(&self) -> Option<u64> {
        // TODO: Return the promise from the local WAL or inner storage
        self.inner.get_promise()
    }

    fn set_promise(&mut self, promise: u64) {
        // TODO: Set the promise in the local WAL or inner storage
        self.inner.set_promise(promise)
    }

    fn get_accepted_idx(&self) -> u64 {
        // TODO: Return the accepted index from the local WAL or inner storage
        self.inner.get_accepted_idx()
    }

    fn set_accepted_idx(&mut self, idx: u64) {
        // TODO: Set the accepted index in the local WAL or inner storage
        self.inner.set_accepted_idx(idx)
    }

    fn get_trimmed_idx(&self) -> u64 {
        // TODO: Return the trimmed index from the local WAL or inner storage
        self.inner.get_trimmed_idx()
    }

    fn set_trimmed_idx(&mut self, trimmed_idx: u64) {
        // TODO: Set the trimmed index in the local WAL or inner storage
        self.inner.set_trimmed_idx(trimmed_idx)
    }
}
