# Active Context: Khonsu - Software Transactional Memory

## Current Work Focus

The focus has shifted to implementing the distributed commit functionality using `omnipaxos` for full consensus and replication of transaction commits, gated by the `distributed` feature flag. A key constraint is to keep all distributed commit related code, including the OmniPaxos instance management and event loop, entirely within the `src/distributed` module. This involves creating a `DistributedCommitManager` within the `distributed` module that encapsulates this logic and interacts with the local Khonsu instance. A critical requirement is to ensure persistence and crash tolerance for operations and transactions to support Point-in-Time Recovery (PITR). The implementation will leverage `rocksdb` as the local write-ahead log (WAL) for durable storage of transaction changes and OmniPaxos log entries, and use gRPC for the network communication between nodes. The constraint regarding the use of an asynchronous runtime has been relaxed for the `distributed` module to accommodate gRPC.

## Recent Changes

- **Implemented Serializable Snapshot Isolation (SSI):**
    - Modified `DependencyTracker` to track transaction states (`Active`, `Committed`, `Aborted`), start/commit timestamps, and recently committed write sets (`TransactionInfo`).
    - Implemented `validate_serializability` in `DependencyTracker` to perform SSI checks:
        - **Backward Validation:** Checks reads against concurrently committed writes (SI read check).
        - **Forward Validation:** Detects dangerous RW dependency structures involving active and recently committed transactions.
    - Integrated transaction state tracking (`register_txn`, `mark_committed`, `mark_aborted`) into the transaction lifecycle (`Transaction::new`, `commit`, `rollback`).
    - Added basic garbage collection for old transaction info in `DependencyTracker`.
- **Refactored Validation Logic:**
    - `Transaction::commit` now calls only `validate_serializability` for `Serializable` isolation.
    - `detect_conflicts` is simplified to handle only `ReadCommitted` and `RepeatableRead` using standard OCC checks against the `TxnBuffer`.
- **Adjusted Tests:** Modified assertions in `tests/basic_transaction.rs` (specifically `test_serializable_wrw_conflict`, `test_serializable_rw_conflict`, `test_serializable_ww_conflict`) to align with expected SSI behavior. All tests now pass.
- Addressed various compilation errors and warnings during refactoring.

## Next Steps

1.  **Ensure `distributed` feature flag and necessary dependencies are added:** Verify `Cargo.toml` including `omnipaxos`, `crossbeam-channel`, `serde`, `rocksdb`, and gRPC-related dependencies (`tonic`, `prost`, `prost-derive`, `tokio` with required features).
2.  **Define Protocol Buffer Messages:** Define `.proto` files for OmniPaxos communication messages.
3.  **Set up Build Script:** Create `build.rs` to compile `.proto` files and generate Rust code.
4.  **Define Replicated Commit Data Structure:** Ensure `ReplicatedCommit` is defined in `src/distributed/mod.rs`.
5.  **Implement Omnipaxos Storage Backend (`DistributedCommitStorage`):** Implement the `omnipaxos::storage::Storage` trait in `src/distributed/storage.rs` using `rocksdb` as the local WAL with atomic operations, ensuring durability for PITR.
6.  **Implement Omnipaxos Network Layer (`KhonsuNetwork`):** Implement the `omnipaxos::network::Network` trait in `src/distributed/network.rs` using gRPC clients to send messages.
7.  **Implement gRPC Server:** Implement a gRPC server within `src/distributed` to receive network messages and pass them to the OmniPaxos instance. This will run asynchronously using Tokio.
8.  **Create `DistributedCommitManager`:** Design and implement a struct within `src/distributed` to encapsulate the OmniPaxos instance, configuration, gRPC server, gRPC client(s), and communication channels.
9.  **Integrate `DistributedCommitManager` with Khonsu:** Modify the `Khonsu` struct to hold an instance of `DistributedCommitManager` (gated by `distributed` feature).
10. **Modify Transaction Commit for Distributed Workflow:** Update `Transaction::commit` to send commit proposals to the `DistributedCommitManager` and wait for the decision when the `distributed` feature is enabled.
11. **Implement Error Handling and Recovery:** Add robust error handling and recovery mechanisms within the Manager, leveraging the `rocksdb` WAL and handling network failures.
12. **Implement Application of Decided Entries:** Implement the logic within the `DistributedCommitManager`'s event loop to apply decided transaction commits to the local Khonsu instance and durably record this application in the `rocksdb` WAL for PITR.
13. **Write Comprehensive Distributed Commit Tests:** Add tests for various distributed scenarios using `omnipaxos` and gRPC, including failures and rollbacks, and verify recovery using the `rocksdb` WAL.

## Active Decisions and Considerations

- **Constraint: Persistence, Crash Tolerance, and PITR:** Ensure all committed operations and transactions are durably persisted to allow for recovery to any point in time using the `rocksdb` WAL.
- **Constraint: Distributed logic in `src/distributed` module:** All code related to distributed commit, including OmniPaxos instance management, event loop, and communication, must reside entirely within the `src/distributed` module and be gated by the `distributed` feature flag. The `Khonsu` struct will interact with this module through a defined interface.
- **DistributedCommitStorage Implementation:** Use `rocksdb` as the local WAL for the `DistributedCommitStorage` implementation, ensuring atomic operations for log manipulation and durability for PITR.
- **Network Implementation:** Use gRPC for inter-node communication. This requires defining protobuf messages, generating code, and implementing gRPC clients and a server.
- **Async Runtime:** The constraint "No Async Runtime (e.g., Tokio)" is relaxed for the `distributed` module to allow the use of Tokio for the gRPC network layer.
- **SSI Accuracy vs. Performance:** Balancing the strictness and completeness of SSI checks (especially backward validation for deletions) with performance overhead.
- **Garbage Collection Strategy:** Determining the optimal frequency and mechanism for cleaning up old transaction information in the `DependencyTracker`.
- **`commit_ts` as Version:** The current implementation assumes the version stored in `TxnBuffer` *is* the `commit_ts` of the writing transaction. This needs to be consistently maintained.
- **Memory Usage:** Monitor memory usage of the `DependencyTracker`, particularly the storage of committed write sets.
- **Omnipaxos Replicated Data Structure:** Carefully design the data structure that represents a transaction commit to be replicated by `omnipaxos`.
- **Inter-thread Communication:** Using `crossbeam-channel` for communication between the main Khonsu thread and the `DistributedCommitManager`'s event loop(s).

## Learnings and Project Insights

- Standard SSI requires careful implementation of both backward (SI read) and forward (dangerous structure) validation checks.
- Test expectations must align precisely with the chosen isolation model's guarantees (SSI vs. stricter active conflict checks vs. pure OCC).
- Tracking transaction states and timestamps is essential for correct SSI validation.
- Refactoring validation logic into the `DependencyTracker` improves modularity for the `Serializable` level.
- Shifting from 2PC to pure Omnipaxos for distributed commit simplifies coordination but requires careful integration with local transaction management and WALs.
- The existing `distributed` module structure designed for 2PC will need to be adapted or replaced for the Omnipaxos-centric approach.
- Using `crossbeam-channel` provides a suitable mechanism for inter-thread communication without a full async runtime (though gRPC might change this).
- Enforcing the constraint of containing distributed logic within a dedicated module requires a different architectural pattern, likely involving a manager struct within the distributed module.
- Implementing the OmniPaxos storage trait with `rocksdb` requires careful consideration of `rocksdb`'s API for log-like operations and atomicity to ensure durability and support PITR.
- Achieving PITR requires not only durable storage of the consensus log but also a mechanism to durably record the application of decided entries to the local state.
- Implementing a real network layer with gRPC adds complexity, requiring protobuf definitions, code generation, and handling asynchronous communication.
- The "No Async Runtime" constraint is relaxed for the `distributed` module to allow the use of Tokio for the gRPC network layer.
