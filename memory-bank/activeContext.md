# Active Context: Khonsu - Software Transactional Memory

## Current Work Focus

The focus has shifted to implementing the distributed commit functionality using `omnipaxos` for full consensus and replication of transaction commits, gated by the `distributed` feature flag. A key constraint is to keep all distributed commit related code, including the OmniPaxos instance management and event loop, entirely within the `src/distributed` module. This involves creating a `DistributedCommitManager` within the `distributed` module that encapsulates this logic and interacts with the local Khonsu instance. The implementation will leverage local write-ahead logs (WALs) for durable storage of transaction changes and use `crossbeam-channel` for inter-thread communication.

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

1.  **Ensure `distributed` feature flag and `omnipaxos`, `crossbeam-channel`, `serde` dependencies are added:** Verify `Cargo.toml`.
2.  **Define Replicated Commit Data Structure:** Ensure `ReplicatedCommit` is defined in `src/distributed/mod.rs`.
3.  **Implement Omnipaxos Storage Backend:** Implement the `omnipaxos::storage::Storage` trait in `src/distributed/storage.rs` using a local WAL.
4.  **Implement Omnipaxos Network Layer:** Implement the `omnipaxos::network::Network` trait in `src/distributed/network.rs` using `crossbeam-channel`.
5.  **Create `DistributedCommitManager`:** Design and implement a struct within `src/distributed` to encapsulate the OmniPaxos instance, configuration, event loop, and communication channels.
6.  **Integrate `DistributedCommitManager` with Khonsu:** Modify the `Khonsu` struct to hold an instance of `DistributedCommitManager` (gated by `distributed` feature).
7.  **Modify Transaction Commit for Distributed Workflow:** Update `Transaction::commit` to send commit proposals to the `DistributedCommitManager` and wait for the decision when the `distributed` feature is enabled.
8.  **Implement Error Handling and Recovery:** Add robust error handling and recovery mechanisms within the `DistributedCommitManager`.
9.  **Implement Application of Decided Entries:** Implement the logic within the `DistributedCommitManager`'s event loop to apply decided transaction commits to the local Khonsu instance and WAL.
10. **Write Comprehensive Distributed Commit Tests:** Add tests for various distributed scenarios using `omnipaxos`, including failures and rollbacks.
11. **Refine SSI Implementation:** (Remaining from previous focus) Improve backward validation for deletions, optimize SSI validation, and refine garbage collection.
12. **Write Comprehensive SSI Tests:** (Remaining from previous focus) Add more targeted tests for SSI.
13. **Refine Memory Reclamation:** (Remaining from previous focus) Review overall memory management.

## Active Decisions and Considerations

- **Constraint: Distributed logic in `src/distributed` module:** All code related to distributed commit, including OmniPaxos instance management, event loop, and communication, must reside within the `src/distributed` module and be gated by the `distributed` feature flag. The `Khonsu` struct will interact with this module through a defined interface.
- **SSI Accuracy vs. Performance:** Balancing the strictness and completeness of SSI checks (especially backward validation for deletions) with performance overhead.
- **Garbage Collection Strategy:** Determining the optimal frequency and mechanism for cleaning up old transaction information in the `DependencyTracker`.
- **`commit_ts` as Version:** The current implementation assumes the version stored in `TxnBuffer` *is* the `commit_ts` of the writing transaction. This needs to be consistently maintained.
- **Memory Usage:** Monitor memory usage of the `DependencyTracker`, particularly the storage of committed write sets.
- **Omnipaxos Replicated Data Structure:** Carefully design the data structure that represents a transaction commit to be replicated by `omnipaxos`.
- **Inter-thread Communication:** Using `crossbeam-channel` for communication between the main Khonsu thread and the OmniPaxos event loop thread managed by the `DistributedCommitManager`.

## Learnings and Project Insights

- Standard SSI requires careful implementation of both backward (SI read) and forward (dangerous structure) validation checks.
- Test expectations must align precisely with the chosen isolation model's guarantees (SSI vs. stricter active conflict checks vs. pure OCC).
- Tracking transaction states and timestamps is essential for correct SSI validation.
- Refactoring validation logic into the `DependencyTracker` improves modularity for the `Serializable` level.
- Shifting from 2PC to pure Omnipaxos for distributed commit simplifies coordination but requires careful integration with local transaction management and WALs.
- The existing `distributed` module structure designed for 2PC will need to be adapted or replaced for the Omnipaxos-centric approach.
- Using `crossbeam-channel` provides a suitable mechanism for inter-thread communication without a full async runtime.
- Enforcing the constraint of containing distributed logic within a dedicated module requires a different architectural pattern, likely involving a manager struct within the distributed module.
