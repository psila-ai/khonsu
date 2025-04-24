use std::sync::Arc;

use khonsu::{Khonsu, TransactionIsolation, conflict::resolution::ConflictResolution};

mod mock_storage;

// Configure tests to run single-threaded
#[cfg(test)]
#[cfg(not(loom))] // Exclude when using Loom for concurrency testing
#[test]
fn test_basic_khonsu_creation() {
    let storage = Arc::new(mock_storage::MockStorage::new());
    let khonsu = Khonsu::new(storage, TransactionIsolation::ReadCommitted, ConflictResolution::Fail);

    // Assert that the Khonsu instance is created and transaction IDs are incrementing
    assert_eq!(khonsu.start_transaction().id(), 0);
    assert_eq!(khonsu.start_transaction().id(), 1);
}

// TODO: Add more test cases for transaction operations (read, write, delete, commit, rollback).
// TODO: Add test cases for different isolation levels and conflict resolution strategies.
// TODO: Add test cases for concurrent transactions.
