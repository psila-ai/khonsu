#![cfg(feature = "distributed")]

// Import the distributed test modules
mod distributed;

// Re-export the test modules
#[test]
fn test_single_node_distributed_transaction() {
    distributed::basic_distributed::test_single_node_distributed_transaction();
}

// This file serves as an entry point for all distributed tests.
// To run all distributed tests, use:
// cargo test --features distributed -- --test-threads=1
