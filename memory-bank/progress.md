# Progress: Khonsu - Software Transactional Memory

## What Works

- Initial project setup and memory bank documentation.
- Core STM data structures (`VersionedValue`, `RwLock<HashMap>`-based Transaction Buffer (`TxnBuffer`)).
- `Transaction` struct and methods (`read`, `write`, `delete`, `commit`, `rollback`).
- `Khonsu` struct and transaction start API.
- Commit process with conflict detection (OCC for ReadCommitted/RepeatableRead) and resolution strategies (`Fail`, `Ignore`, `Replace`, `Append`).
- Rollback functionality.
- `Storage` trait definition and `MockStorage` implementation integrated into commit.
- Helper functions for manual Arrow array manipulation (`merge_record_batches`).
- Defined error types in `errors.rs`.
- **Serializable Snapshot Isolation (SSI) Implementation:**
    - `DependencyTracker` now tracks transaction states (`Active`, `Committed`, `Aborted`), start/commit timestamps, and recently committed write sets (`TransactionInfo`).
    - `validate_serializability` function implements SSI backward (SI read) and forward (dangerous RW structure) checks.
    - Transaction lifecycle (`new`, `commit`, `rollback`) integrates with `DependencyTracker` for state updates.
    - Basic garbage collection for old transaction info in `DependencyTracker`.
    - `record_read` and `record_write` update active dependencies in `item_dependencies`.
    - `remove_active_dependencies` cleans up `item_dependencies` on commit/abort.
- **Distributed Commit Implementation:**
    - Added `distributed` feature flag and necessary dependencies (OmniPaxos, crossbeam-channel, RocksDB, gRPC-related).
    - Defined Protocol Buffer Messages for network communication.
    - Set up Build Script for protobuf compilation.
    - Defined Replicated Commit Data Structure (`ReplicatedCommit`) in `src/distributed/mod.rs`.
    - Implemented Omnipaxos Storage Backend (`DistributedCommitStorage`) in `src/distributed/storage.rs` using `rocksdb` as the local WAL with atomic operations, ensuring durability for PITR.
    - Implemented Omnipaxos Network Layer (`KhonsuNetwork`) in `src/distributed/network.rs` using gRPC clients.
    - Implemented gRPC Server within `src/distributed` to receive network messages.
    - Created `DistributedCommitManager` struct within `src/distributed` to encapsulate OmniPaxos logic, gRPC server/clients, and communication channels.
    - Integrated `DistributedCommitManager` with Khonsu (Khonsu holds an instance).
    - Modified Transaction Commit for Distributed Workflow (Transaction sends proposals to Manager).
    - Implemented Error Handling and Recovery within the Manager, leveraging `rocksdb` WAL and handling network failures.
    - Implemented Application of Decided Entries within the Manager's event loop, durably recording application in the `rocksdb` WAL for PITR.
    - Implemented Two-Phase Commit (2PC) over OmniPaxos for transaction coordination.
    - Added crash resistance and recovery mechanisms.
    - Implemented global transaction IDs that are unique across the cluster.
    - Added serialization support for Arrow RecordBatches using IPC format.
    - Created a channel extension system for node identification.
- **Basic Test Suite:** Core transaction operations and specific SSI conflict scenarios (`wrw`, `rw`, `ww`, dependency removal) are tested and passing after aligning test expectations with SSI behavior.

## What's Left to Build

1. **Enhance Crash Recovery:**
   - Add more comprehensive recovery tests for various failure scenarios
   - Implement automatic recovery of in-progress transactions after node restart

2. **Optimize Performance:**
   - Profile and optimize the distributed commit path
   - Reduce serialization overhead for large RecordBatches
   - Implement batching for network communication

3. **Add Monitoring and Metrics:**
   - Add metrics for transaction throughput, latency, and success/failure rates
   - Implement monitoring for node health and cluster status

4. **Improve Distributed Transaction Routing:**
   - Implement smarter routing of transactions to nodes based on data locality
   - Add support for read-only transactions that don't need consensus

5. **Refine SSI Implementation:**
   - Improve backward validation for deleted items (currently heuristic)
   - Optimize SSI validation performance
   - Refine garbage collection strategy and memory usage for `DependencyTracker`

6. **Comprehensive Testing:**
   - Add more targeted tests for various SSI conflict scenarios
   - Add tests for network partitions and other failure modes
   - Implement stress testing for the distributed system

## Current Status

The project has successfully implemented distributed commits using OmniPaxos for consensus and Two-Phase Commit (2PC) for transaction coordination. The implementation is crash-resistant and can recover from node failures while maintaining consistency. All distributed commit related code is encapsulated within the `src/distributed` module and gated by the `distributed` feature flag. The system uses RocksDB as the local write-ahead log (WAL) for durable storage of transaction changes and OmniPaxos log entries, and gRPC for network communication between nodes. The constraint regarding the use of an asynchronous runtime has been relaxed for the `distributed` module to accommodate gRPC. The next steps include enhancing crash recovery, optimizing performance, adding monitoring and metrics, improving distributed transaction routing, refining the SSI implementation, and adding more comprehensive tests.

## Known Issues

- The backward validation check for items deleted concurrently relies on a heuristic and needs improvement (requires tracking deletion timestamps or similar).
- Performance implications of SSI validation and `DependencyTracker` state management haven't been thoroughly evaluated.
- Garbage collection for `DependencyTracker` is basic and might need refinement based on usage patterns.
- The use of gRPC necessitates the use of an asynchronous runtime (e.g., Tokio) within the `distributed` module. This is a necessary exception to the general "No Async Runtime" constraint for the core STM logic.
- Serialization overhead for large RecordBatches might impact performance in distributed scenarios.
- Recovery from certain complex failure scenarios (e.g., network partitions during commit) might not be fully tested.

## Evolution of Project Decisions

- **Distributed Commit Implementation:** Successfully implemented distributed commits using OmniPaxos for consensus and Two-Phase Commit (2PC) for transaction coordination.
- **Integration of 2PC and OmniPaxos:** Implemented a hybrid approach where 2PC is used for transaction coordination, but the 2PC protocol itself is made fault-tolerant by replicating its state through OmniPaxos.
- **Crash Tolerance and Recovery:** Designed the system to be crash-tolerant, with all transaction state and OmniPaxos log entries persisted to RocksDB.
- **Global Transaction IDs:** Implemented a scheme for generating globally unique transaction IDs that combine the node ID, a local transaction ID, and a timestamp.
- **Serialization of Arrow RecordBatches:** Used Arrow's IPC format to serialize RecordBatches for network transmission.
- **Channel Extension System:** Created a channel extension system for node identification to support distributed transactions.
- Shifted from simple cycle detection/OCC for Serializable to implementing standard Serializable Snapshot Isolation (SSI).
- Refactored validation logic, centralizing SSI checks in `DependencyTracker::validate_serializability` and simplifying `detect_conflicts`.
- Adjusted test expectations to match the guarantees provided by SSI.
- `DependencyTracker` now holds more state (`TransactionInfo`) to support SSI validation.
- `TxnBuffer` implementation confirmed as `RwLock<HashMap>`, not `SkipMap` as mentioned in older memory bank entries.
- Decided to use `omnipaxos` for the full distributed commit process instead of a 2PC approach with `omnipaxos` for voting.
- Renamed the `twopc` feature flag to `distributed`.
- Switched from `tokio::sync::mpsc` to `crossbeam-channel` for inter-thread communication (though gRPC might change this).
- **Constraint:** All distributed commit related code must be encapsulated within the `src/distributed` module and gated by the `distributed` feature flag, managed by a `DistributedCommitManager`.
- **DistributedCommitStorage Implementation:** The `DistributedCommitStorage` will use `rocksdb` as the local WAL with atomic operations.
- **Persistence, Crash Tolerance, and PITR:** Explicitly prioritizing persistence and crash tolerance through the `rocksdb` WAL to enable Point-in-Time Recovery.
- **Network Implementation:** Decided to use gRPC for inter-node communication.
- **Async Runtime for gRPC:** The constraint "No Async Runtime" is relaxed for the `distributed` module to allow the use of Tokio for the gRPC network layer.
