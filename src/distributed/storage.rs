use crate::errors::*;
use bincode;
use omnipaxos::storage::{StopSign, Storage, StorageOp, StorageResult};
use omnipaxos::ballot_leader_election::Ballot;
use rocksdb::{Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::distributed::ReplicatedCommit;

// Keys for storing OmniPaxos metadata in RocksDB
const KEY_DECIDED_IDX: &str = "decided_idx";
const KEY_ACCEPTED_ROUND: &str = "accepted_round";
const KEY_PROMISE: &str = "promise";
const KEY_COMPACTED_IDX: &str = "compacted_idx";
const KEY_STOPSIGN: &str = "stopsign";
const KEY_SNAPSHOT: &str = "snapshot";
const KEY_LOG_PREFIX: &str = "log_"; // Prefix for log entries

/// Storage backend for OmniPaxos, persisting log entries to a local RocksDB WAL.
pub struct DistributedCommitStorage {
    db: DB,
}

impl Storage<ReplicatedCommit> for DistributedCommitStorage {
    fn append_entry(&mut self, entry: ReplicatedCommit) -> StorageResult<()> {
        let idx = self.get_log_len()? + self.get_compacted_idx()?;
        let key = format!("{}{}", KEY_LOG_PREFIX, idx);
        
        let encoded = bincode::serialize(&entry)
            .map_err(|e| format!("Failed to serialize entry: {}", e))?;
            
        self.db.put(key, encoded)
            .map_err(|e| format!("Failed to write to RocksDB: {}", e))?;
            
        // Update the last log index
        let last_idx_encoded = bincode::serialize(&idx)
            .map_err(|e| format!("Failed to serialize last log index: {}", e))?;
        self.db.put("last_log_index", last_idx_encoded)
            .map_err(|e| format!("Failed to update last log index: {}", e))?;
            
        Ok(())
    }

    fn append_entries(&mut self, entries: Vec<ReplicatedCommit>) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        
        let mut batch = WriteBatch::default();
        let mut current_idx = self.get_log_len()? + self.get_compacted_idx()?;
        
        for entry in entries {
            let key = format!("{}{}", KEY_LOG_PREFIX, current_idx);
            let encoded = bincode::serialize(&entry)
                .map_err(|e| format!("Failed to serialize entry: {}", e))?;
            batch.put(key, encoded);
            current_idx += 1;
        }
        
        // Update the last log index
        let last_idx_encoded = bincode::serialize(&(current_idx - 1))
            .map_err(|e| format!("Failed to serialize last log index: {}", e))?;
        batch.put("last_log_index", last_idx_encoded);
        
        self.db.write(batch)
            .map_err(|e| format!("Failed to write batch to RocksDB: {}", e))?;
            
        Ok(())
    }

    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<ReplicatedCommit>) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        
        let mut batch = WriteBatch::default();
        let mut current_idx = from_idx;
        
        for entry in entries {
            let key = format!("{}{}", KEY_LOG_PREFIX, current_idx);
            let encoded = bincode::serialize(&entry)
                .map_err(|e| format!("Failed to serialize entry: {}", e))?;
            batch.put(key, encoded);
            current_idx += 1;
        }
        
        // Update the last log index
        let last_idx_encoded = bincode::serialize(&(current_idx - 1))
            .map_err(|e| format!("Failed to serialize last log index: {}", e))?;
        batch.put("last_log_index", last_idx_encoded);
        
        self.db.write(batch)
            .map_err(|e| format!("Failed to write batch to RocksDB: {}", e))?;
            
        Ok(())
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<ReplicatedCommit>> {
        let mut entries = Vec::new();
        for i in from..to {
            let key = format!("{}{}", KEY_LOG_PREFIX, i);
            let value = self.db.get(&key)
                .map_err(|e| format!("Failed to read from RocksDB: {}", e))?;
                
            match value {
                Some(bytes) => {
                    let entry = bincode::deserialize(&bytes)
                        .map_err(|e| format!("Failed to deserialize entry: {}", e))?;
                    entries.push(entry);
                },
                None => {
                    // If an entry is missing in the requested range, something is wrong
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Missing log entry at index {}", i),
                    )));
                }
            }
        }
        Ok(entries)
    }

    fn get_suffix(&self, from: usize) -> StorageResult<Vec<ReplicatedCommit>> {
        let log_len = self.get_log_len()?;
        let compacted_idx = self.get_compacted_idx()?;
        
        if from >= compacted_idx + log_len {
            Ok(Vec::new())
        } else {
            self.get_entries(from, compacted_idx + log_len)
        }
    }

    fn write_atomically(&mut self, ops: Vec<StorageOp<ReplicatedCommit>>) -> StorageResult<()> {
        let mut batch = WriteBatch::default();
        
        for op in ops {
            match op {
                StorageOp::AppendEntry(entry) => {
                    let idx = self.get_log_len()? + self.get_compacted_idx()?;
                    let key = format!("{}{}", KEY_LOG_PREFIX, idx);
                    let encoded = bincode::serialize(&entry)
                        .map_err(|e| Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to serialize entry: {}", e),
                        )))?;
                    batch.put(key, encoded);
                    
                    // Update the last log index
                    let last_idx_encoded = bincode::serialize(&idx)
                        .map_err(|e| Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to serialize last log index: {}", e),
                        )))?;
                    batch.put("last_log_index", last_idx_encoded);
                },
                StorageOp::AppendEntries(entries) => {
                    let mut current_idx = self.get_log_len()? + self.get_compacted_idx()?;
                    for entry in entries {
                        let key = format!("{}{}", KEY_LOG_PREFIX, current_idx);
                        let encoded = bincode::serialize(&entry)
                            .map_err(|e| Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Failed to serialize entry: {}", e),
                            )))?;
                        batch.put(key, encoded);
                        current_idx += 1;
                    }
                    
                    // Update the last log index
                    if current_idx > 0 {
                        let last_idx = current_idx - 1;
                        let last_idx_encoded = bincode::serialize(&last_idx)
                            .map_err(|e| Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Failed to serialize last log index: {}", e),
                            )))?;
                        batch.put("last_log_index", last_idx_encoded);
                    }
                },
                StorageOp::AppendOnPrefix(from_idx, entries) => {
                    let mut current_idx = from_idx;
                    for entry in entries {
                        let key = format!("{}{}", KEY_LOG_PREFIX, current_idx);
                        let encoded = bincode::serialize(&entry)
                            .map_err(|e| Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Failed to serialize entry: {}", e),
                            )))?;
                        batch.put(key, encoded);
                        current_idx += 1;
                    }
                    
                    // Update the last log index if needed
                    if current_idx > 0 {
                        let last_idx = current_idx - 1;
                        let current_last_idx = match self.db.get("last_log_index") {
                            Ok(Some(bytes)) => {
                                bincode::deserialize::<usize>(&bytes)
                                    .map_err(|e| Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        format!("Failed to deserialize last log index: {}", e),
                                    )))?
                            },
                            _ => 0,
                        };
                            
                        if last_idx > current_last_idx {
                            let last_idx_encoded = bincode::serialize(&last_idx)
                                .map_err(|e| Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("Failed to serialize last log index: {}", e),
                                )))?;
                            batch.put("last_log_index", last_idx_encoded);
                        }
                    }
                },
                StorageOp::SetDecidedIndex(idx) => {
                    let encoded = bincode::serialize(&idx)
                        .map_err(|e| Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to serialize decided index: {}", e),
                        )))?;
                    batch.put(KEY_DECIDED_IDX, encoded);
                },
                StorageOp::SetCompactedIdx(idx) => {
                    let encoded = bincode::serialize(&idx)
                        .map_err(|e| Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to serialize compacted index: {}", e),
                        )))?;
                    batch.put(KEY_COMPACTED_IDX, encoded);
                },
                StorageOp::SetPromise(promise) => {
                    let encoded = bincode::serialize(&promise)
                        .map_err(|e| Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to serialize promise: {}", e),
                        )))?;
                    batch.put(KEY_PROMISE, encoded);
                },
                StorageOp::SetAcceptedRound(round) => {
                    let encoded = bincode::serialize(&round)
                        .map_err(|e| Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to serialize accepted round: {}", e),
                        )))?;
                    batch.put(KEY_ACCEPTED_ROUND, encoded);
                },
                StorageOp::SetSnapshot(snapshot) => {
                    if let Some(snapshot) = snapshot {
                        let encoded = bincode::serialize(&snapshot)
                            .map_err(|e| Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Failed to serialize snapshot: {}", e),
                            )))?;
                        batch.put(KEY_SNAPSHOT, encoded);
                    } else {
                        batch.delete(KEY_SNAPSHOT);
                    }
                },
                StorageOp::SetStopsign(stopsign) => {
                    if let Some(stopsign) = stopsign {
                        let encoded = bincode::serialize(&stopsign)
                            .map_err(|e| Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Failed to serialize stopsign: {}", e),
                            )))?;
                        batch.put(KEY_STOPSIGN, encoded);
                    } else {
                        batch.delete(KEY_STOPSIGN);
                    }
                },
                StorageOp::Trim(idx) => {
                    // Delete all entries from compacted_idx to idx
                    let compacted_idx = self.get_compacted_idx()?;
                    
                    if idx > compacted_idx {
                        for i in compacted_idx..idx {
                            let key = format!("{}{}", KEY_LOG_PREFIX, i);
                            batch.delete(key);
                        }
                        
                        // Update the compacted index
                        let encoded = bincode::serialize(&idx)
                            .map_err(|e| Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Failed to serialize compacted index: {}", e),
                            )))?;
                        batch.put(KEY_COMPACTED_IDX, encoded);
                    }
                },
            }
        }
        
        self.db.write(batch)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to write batch to RocksDB: {}", e),
            )))?;
            
        Ok(())
    }

    fn get_log_len(&self) -> StorageResult<usize> {
        let last_idx = match self.db.get("last_log_index") {
            Ok(Some(bytes)) => {
                bincode::deserialize::<usize>(&bytes)
                    .map_err(|e| Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to deserialize last log index: {}", e),
                    )))?
            },
            _ => 0,
        };
            
        let compacted_idx = self.get_compacted_idx()?;
        
        if last_idx < compacted_idx {
            Ok(0)
        } else {
            Ok(last_idx - compacted_idx + 1)
        }
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        match self.db.get(KEY_DECIDED_IDX) {
            Ok(Some(bytes)) => {
                Ok(bincode::deserialize::<usize>(&bytes)
                    .map_err(|e| Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to deserialize decided index: {}", e),
                    )) as Box<dyn std::error::Error>)?)
            },
            _ => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Decided index not found",
            )) as Box<dyn std::error::Error>),
        }
    }

    fn set_decided_idx(&mut self, idx: usize) -> StorageResult<()> {
        let encoded = bincode::serialize(&idx)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize decided index: {}", e),
            )))?;
        self.db.put(KEY_DECIDED_IDX, encoded)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to set decided index: {}", e),
            )))?;
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        match self.db.get(KEY_ACCEPTED_ROUND) {
            Ok(Some(bytes)) => {
                Ok(Some(bincode::deserialize::<Ballot>(&bytes)
                    .map_err(|e| Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to deserialize accepted round: {}", e),
                    )) as Box<dyn std::error::Error>)?))
            },
            _ => Ok(None),
        }
    }

    fn set_accepted_round(&mut self, round: Ballot) -> StorageResult<()> {
        let encoded = bincode::serialize(&round)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize accepted round: {}", e),
            )))?;
        self.db.put(KEY_ACCEPTED_ROUND, encoded)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to set accepted round: {}", e),
            )))?;
        Ok(())
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        match self.db.get(KEY_PROMISE) {
            Ok(Some(bytes)) => {
                Ok(Some(bincode::deserialize::<Ballot>(&bytes)
                    .map_err(|e| Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to deserialize promise: {}", e),
                    )) as Box<dyn std::error::Error>)?))
            },
            _ => Ok(None),
        }
    }

    fn set_promise(&mut self, promise: Ballot) -> StorageResult<()> {
        let encoded = bincode::serialize(&promise)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize promise: {}", e),
            )))?;
        self.db.put(KEY_PROMISE, encoded)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to set promise: {}", e),
            )))?;
        Ok(())
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        if let Some(stopsign) = s {
            let encoded = bincode::serialize(&stopsign)
                .map_err(|e| Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to serialize stopsign: {}", e),
                )))?;
            self.db.put(KEY_STOPSIGN, encoded)
                .map_err(|e| Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to set stopsign: {}", e),
                )))?;
        } else {
            // Delete the stopsign key if None
            self.db.delete(KEY_STOPSIGN)
                .map_err(|e| Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to delete stopsign: {}", e),
                )))?;
        }
        Ok(())
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        match self.db.get(KEY_STOPSIGN) {
            Ok(Some(bytes)) => {
                Ok(Some(bincode::deserialize::<StopSign>(&bytes)
                    .map_err(|e| Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to deserialize stopsign: {}", e),
                    )) as Box<dyn std::error::Error>)?))
            },
            _ => Ok(None),
        }
    }

    fn trim(&mut self, idx: usize) -> StorageResult<()> {
        // Trimming means deleting all entries up to idx
        let compacted_idx = self.get_compacted_idx()?;
        
        if idx <= compacted_idx {
            // Already trimmed
            return Ok(());
        }
        
        let mut batch = WriteBatch::default();
        
        // Delete all entries from compacted_idx to idx
        for i in compacted_idx..idx {
            let key = format!("{}{}", KEY_LOG_PREFIX, i);
            batch.delete(key);
        }
        
        // Update the compacted index
        let encoded = bincode::serialize(&idx)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize compacted index: {}", e),
            )))?;
        batch.put(KEY_COMPACTED_IDX, encoded);
        
        self.db.write(batch)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to write batch to RocksDB: {}", e),
            )))?;
            
        Ok(())
    }

    fn set_compacted_idx(&mut self, idx: usize) -> StorageResult<()> {
        let encoded = bincode::serialize(&idx)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize compacted index: {}", e),
            )))?;
        self.db.put(KEY_COMPACTED_IDX, encoded)
            .map_err(|e| Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to set compacted index: {}", e),
            )))?;
        Ok(())
    }

    fn get_compacted_idx(&self) -> StorageResult<usize> {
        match self.db.get(KEY_COMPACTED_IDX) {
            Ok(Some(bytes)) => {
                Ok(bincode::deserialize::<usize>(&bytes)
                    .map_err(|e| Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to deserialize compacted index: {}", e),
                    )) as Box<dyn std::error::Error>)?)
            },
            _ => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Compacted index not found",
            )) as Box<dyn std::error::Error>),
        }
    }

    fn set_snapshot(&mut self, snapshot: Option<<ReplicatedCommit as omnipaxos::storage::Entry>::Snapshot>) -> StorageResult<()> {
        if let Some(snapshot) = snapshot {
            let encoded = bincode::serialize(&snapshot)
                .map_err(|e| Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to serialize snapshot: {}", e),
                )))?;
            self.db.put(KEY_SNAPSHOT, encoded)
                .map_err(|e| Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to set snapshot: {}", e),
                )))?;
        } else {
            // Delete the snapshot key if None
            self.db.delete(KEY_SNAPSHOT)
                .map_err(|e| Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to delete snapshot: {}", e),
                )))?;
        }
        Ok(())
    }

    fn get_snapshot(&self) -> StorageResult<Option<<ReplicatedCommit as omnipaxos::storage::Entry>::Snapshot>> {
        match self.db.get(KEY_SNAPSHOT) {
            Ok(Some(bytes)) => {
                Ok(Some(bincode::deserialize::<<ReplicatedCommit as omnipaxos::storage::Entry>::Snapshot>(&bytes)
                    .map_err(|e| Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to deserialize snapshot: {}", e),
                    )) as Box<dyn std::error::Error>)?))
            },
            _ => Ok(None),
        }
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
        let db = DB::open(&opts, path).map_err(|e| KhonsuError::StorageError(e.to_string()))?;

        // Initialize metadata keys if they don't exist
        let mut batch = WriteBatch::default();
        
        // Initialize decided index
        if db.get(KEY_DECIDED_IDX).map_err(|e| KhonsuError::StorageError(e.to_string()))?.is_none() {
            batch.put(
                KEY_DECIDED_IDX,
                bincode::serialize(&0usize).map_err(|e| KhonsuError::SerializationError(e.to_string()))?,
            );
        }
        
        // Initialize compacted index
        if db.get(KEY_COMPACTED_IDX).map_err(|e| KhonsuError::StorageError(e.to_string()))?.is_none() {
            batch.put(
                KEY_COMPACTED_IDX,
                bincode::serialize(&0usize).map_err(|e| KhonsuError::SerializationError(e.to_string()))?,
            );
        }
        
        // Initialize last log index
        if db.get("last_log_index").map_err(|e| KhonsuError::StorageError(e.to_string()))?.is_none() {
            batch.put(
                "last_log_index",
                bincode::serialize(&0usize).map_err(|e| KhonsuError::SerializationError(e.to_string()))?,
            );
        }

        db.write(batch).map_err(|e| KhonsuError::StorageError(e.to_string()))?;

        Ok(Self { db })
    }

    // Helper to get a metadata value
    fn get_metadata<T: for<'a> Deserialize<'a>>(&self, key: &str) -> Result<Option<T>> {
        let value = self.db.get(key).map_err(|e| KhonsuError::StorageError(e.to_string()))?;
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
        self.db.put(key, encoded)
            .map_err(|e| KhonsuError::StorageError(e.to_string()))?;
        Ok(())
    }

    // Helper to get a log entry from RocksDB
    fn get_log_entry(&self, idx: usize) -> Result<Option<ReplicatedCommit>> {
        let key = format!("{}{}", KEY_LOG_PREFIX, idx);
        let value = self.db.get(key).map_err(|e| KhonsuError::StorageError(e.to_string()))?;
        match value {
            Some(bytes) => bincode::deserialize(&bytes)
                .map_err(|e| KhonsuError::SerializationError(e.to_string()))
                .map(Some),
            None => Ok(None),
        }
    }
    
    // Helper to recover transaction state
    pub fn recover_transaction_state(&self, txn_id: &crate::distributed::GlobalTransactionId) -> Result<Option<crate::distributed::TransactionState>> {
        // Scan the log for the transaction
        let compacted_idx = self.get_metadata::<usize>(KEY_COMPACTED_IDX)
            .map_err(|e| KhonsuError::StorageError(format!("Failed to get compacted index: {}", e)))?
            .unwrap_or(0);
            
        let last_idx = self.get_metadata::<usize>("last_log_index")
            .map_err(|e| KhonsuError::StorageError(format!("Failed to get last log index: {}", e)))?
            .unwrap_or(0);
            
        for idx in compacted_idx..=last_idx {
            if let Some(entry) = self.get_log_entry(idx)? {
                if entry.transaction_id == *txn_id {
                    return Ok(Some(entry.state));
                }
            }
        }
        
        Ok(None)
    }
}
