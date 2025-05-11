use crate::errors::*;
use bincode;
use omnipaxos::storage::{Entry, StopSign, Storage, StorageOp, StorageResult};
use omnipaxos::util::{NodeId, SnapshottedEntry};
use rocksdb::{Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::Path;

use crate::distributed::ReplicatedCommit;
use crate::errors::*;

// Keys for storing OmniPaxos metadata in RocksDB
const CF_DEFAULT: &str = "default";
const KEY_DECIDED_IDX: &str = "decided_idx";
const KEY_ACCEPTED_ROUND: &str = "accepted_round";
const KEY_LEADER: &str = "leader";
const KEY_PROMISE: &str = "promise";
const KEY_ACCEPTED_IDX: &str = "accepted_idx";
const KEY_TRIMMED_IDX: &str = "trimmed_idx";
const KEY_LOG_START_IDX: &str = "log_start_idx"; // To track the start index of the log

/// Storage backend for OmniPaxos, persisting log entries to a local RocksDB WAL.
pub struct DistributedCommitStorage {
    db: DB,
}

impl Storage<ReplicatedCommit> for DistributedCommitStorage {
    fn append(&mut self, entry: ReplicatedCommit) -> Result<u64> {
        let idx = self.get_log_len() + self.get_log_start_idx();
        let key = Self::log_key(idx);
        let encoded: Vec<u8> = bincode::serialize(&entry)
            .map_err(|e| KhonsuError::SerializationError(e.to_string()))?;
        self.db
            .put(&key, encoded)
            .map_err(|e| KhonsuError::StorageError(e.to_string()))?;
        Ok(idx)
    }

    fn append_entries(&mut self, entries: Vec<ReplicatedCommit>) -> Result<u64> {
        let mut batch = WriteBatch::default();
        let mut current_idx = self.get_log_len() + self.get_log_start_idx();
        for entry in entries {
            let key = Self::log_key(current_idx);
            let encoded: Vec<u8> = bincode::serialize(&entry)
                .map_err(|e| KhonsuError::SerializationError(e.to_string()))?;
            batch.put(&key, encoded);
            current_idx += 1;
        }
        self.db
            .write(batch)
            .map_err(|e| KhonsuError::StorageError(e.to_string()))?;
        Ok(current_idx - 1) // Return the index of the last appended entry
    }

    fn get_entries(&self, from: u64, to: u64) -> Result<Vec<ReplicatedCommit>> {
        let mut entries = Vec::new();
        for i in from..to {
            if let Some(entry) = self.get_log_entry(i)? {
                entries.push(entry);
            } else {
                // If an entry is missing in the requested range, something is wrong
                return Err(KhonsuError::StorageError(format!(
                    "Missing log entry at index {}",
                    i
                )));
            }
        }
        Ok(entries)
    }

    fn get_suffix(&self, from: u64) -> Result<Vec<ReplicatedCommit>> {
        let log_len = self.get_log_len();
        let start_idx = self.get_log_start_idx();
        if from >= start_idx + log_len {
            Ok(Vec::new())
        } else {
            self.get_entries(from, start_idx + log_len)
        }
    }

    fn get_log_len(&self) -> u64 {
        // This is tricky with RocksDB. We need to store the log length metadata.
        // For simplicity now, let's assume we store the last appended index.
        // A more robust implementation would iterate or maintain a separate counter.
        // For now, let's use a placeholder or rely on a metadata key.
        // Let's use a metadata key for the last index and calculate length.
        let last_idx = self
            .get_metadata::<u64>("last_log_index")
            .unwrap_or(Some(0))
            .unwrap_or(0);
        let start_idx = self.get_log_start_idx();
        if last_idx < start_idx {
            0
        } else {
            last_idx - start_idx + 1
        }
    }

    fn get_decided_idx(&self) -> u64 {
        self.get_metadata::<u64>(KEY_DECIDED_IDX)
            .unwrap_or(Some(0))
            .unwrap_or(0) // Default to 0
    }

    fn set_decided_idx(&mut self, idx: u64) {
        let _ = self.set_metadata(KEY_DECIDED_IDX, &idx); // Ignore result for simplicity in placeholder
    }

    fn get_accepted_round(&self) -> Option<u64> {
        self.get_metadata::<u64>(KEY_ACCEPTED_ROUND).unwrap_or(None)
    }

    fn set_accepted_round(&mut self, round: u64) {
        let _ = self.set_metadata(KEY_ACCEPTED_ROUND, &round); // Ignore result
    }

    fn get_promise(&self) -> Option<u64> {
        self.get_metadata::<u64>(KEY_PROMISE).unwrap_or(None)
    }

    fn set_promise(&mut self, promise: u64) {
        let _ = self.set_metadata(KEY_PROMISE, &promise); // Ignore result
    }

    fn write_atomically(&mut self, ops: Vec<StorageOp<ReplicatedCommit>>) -> StorageResult<()> {
        todo!()
    }

    fn append_entry(&mut self, entry: ReplicatedCommit) -> StorageResult<()> {
        todo!()
    }

    fn append_on_prefix(
        &mut self,
        from_idx: usize,
        entries: Vec<ReplicatedCommit>,
    ) -> StorageResult<()> {
        todo!()
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        todo!()
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        todo!()
    }

    fn trim(&mut self, idx: usize) -> StorageResult<()> {
        todo!()
    }

    fn set_compacted_idx(&mut self, idx: usize) -> StorageResult<()> {
        todo!()
    }

    fn get_compacted_idx(&self) -> StorageResult<usize> {
        todo!()
    }

    fn set_snapshot(&mut self, snapshot: Option<ReplicatedCommit::Snapshot>) -> StorageResult<()> {
        todo!()
    }

    fn get_snapshot(&self) -> StorageResult<Option<ReplicatedCommit::Snapshot>> {
        todo!()
    }
}

impl DistributedCommitStorage {
    /// Creates a new `DistributedCommitStorage` with a RocksDB backend.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the RocksDB database directory.
    pub fn new(path: &Path) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Define column families for different types of data if needed
        // let cf_names = vec![CF_DEFAULT];
        // let db = DB::open_cf_descriptors(&opts, path, cf_names).map_err(|e| KhonsuError::StorageError(e.to_string()))?;
        let db = DB::open(&opts, path).map_err(|e| KhonsuError::StorageError(e.to_string()))?;

        // Initialize metadata keys if they don't exist
        let mut batch = WriteBatch::default();
        if db
            .get(KEY_DECIDED_IDX)
            .map_err(|e| KhonsuError::StorageError(e.to_string()))?
            .is_none()
        {
            batch.put(
                KEY_DECIDED_IDX,
                bincode::serialize(&0u64)
                    .map_err(|e| KhonsuError::SerializationError(e.to_string()))?,
            );
        }
        if db
            .get(KEY_LOG_START_IDX)
            .map_err(|e| KhonsuError::StorageError(e.to_string()))?
            .is_none()
        {
            batch.put(
                KEY_LOG_START_IDX,
                bincode::serialize(&1u64)
                    .map_err(|e| KhonsuError::SerializationError(e.to_string()))?,
            );
        }
        // Initialize other metadata keys similarly

        db.write(batch)
            .map_err(|e| KhonsuError::StorageError(e.to_string()))?;

        Ok(Self { db })
    }

    // Helper to get a metadata value
    fn get_metadata<T: Deserialize<'static>>(&self, key: &str) -> Result<Option<T>> {
        let value = self
            .db
            .get(key)
            .map_err(|e| KhonsuError::StorageError(e.to_string()))?;
        match value {
            Some(bytes) => bincode::deserialize(&bytes)
                .map_err(|e| KhonsuError::SerializationError(e.to_string()))
                .map(Some),
            None => Ok(None),
        }
    }

    // Helper to set a metadata value
    fn set_metadata<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()> {
        let encoded: Vec<u8> = bincode::serialize(value)
            .map_err(|e| KhonsuError::SerializationError(e.to_string()))?;
        self.db
            .put(key, encoded)
            .map_err(|e| KhonsuError::StorageError(e.to_string()))?;
        Ok(())
    }

    // Helper to get a log entry key
    fn log_key(idx: u64) -> Vec<u8> {
        bincode::serialize(&idx).expect("Failed to serialize log index")
    }

    // Helper to get a log entry from RocksDB
    fn get_log_entry(&self, idx: u64) -> Result<Option<ReplicatedCommit>> {
        let key = Self::log_key(idx);
        let value = self
            .db
            .get(&key)
            .map_err(|e| KhonsuError::StorageError(e.to_string()))?;
        match value {
            Some(bytes) => bincode::deserialize(&bytes)
                .map_err(|e| KhonsuError::SerializationError(e.to_string()))
                .map(Some),
            None => Ok(None),
        }
    }
}
