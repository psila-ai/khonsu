# Active Context: Khonsu - Software Transactional Memory

## Current Work Focus

The focus has shifted to implementing the distributed commit functionality using a two-phase commit (2PC) protocol, gated by the `twopc` feature flag. This involves integrating the `omnipaxos` crate for the voting phase and managing a shared transaction log built on local write-ahead logs (WALs).

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

1.  **Add `twopc` feature flag and `omnipaxos` dependency:** Modify `Cargo.toml` to include the feature flag and the necessary dependency.
2.  **Design and Implement Two-Phase Commit Coordinator:** Create the `TwoPhaseCommitCoordinator` struct to manage the distributed commit process.
3.  **Implement Shared Transaction Log:** Design and implement the shared transaction log mechanism, leveraging local WALs and `omnipaxos` for consistency.
4.  **Implement 2PC Protocol Logic:** Implement the initiate, prepare, and commit/abort phases within the `TwoPhaseCommitCoordinator`.
5.  **Implement `TwoPhaseCommitParticipant` for Khonsu:** Complete the implementation of the trait for the local Khonsu instance, including WAL interactions for voting and completion.
6.  **Integrate Coordinator with Khonsu:** Modify the `Khonsu` struct to interact with the `TwoPhaseCommitCoordinator`.
7.  **Implement Error Handling and Recovery:** Add robust error handling and recovery mechanisms based on the shared log and local WALs.
8.  **Write Comprehensive Distributed Commit Tests:** Add tests for various distributed scenarios, including failures.
9.  **Refine SSI Implementation:** (Remaining from previous focus) Improve backward validation for deletions, optimize SSI validation, and refine garbage collection.
10. **Write Comprehensive SSI Tests:** (Remaining from previous focus) Add more targeted tests for SSI.
11. **Refine Memory Reclamation:** (Remaining from previous focus) Review overall memory management.

## Active Decisions and Considerations

- **SSI Accuracy vs. Performance:** Balancing the strictness and completeness of SSI checks (especially backward validation for deletions) with performance overhead.
- **Garbage Collection Strategy:** Determining the optimal frequency and mechanism for cleaning up old transaction information in the `DependencyTracker`.
- **`commit_ts` as Version:** The current implementation assumes the version stored in `TxnBuffer` *is* the `commit_ts` of the writing transaction. This needs to be consistently maintained.
- **Memory Usage:** Monitor memory usage of the `DependencyTracker`, particularly the storage of committed write sets.

## Learnings and Project Insights

- Standard SSI requires careful implementation of both backward (SI read) and forward (dangerous structure) validation checks.
- Test expectations must align precisely with the chosen isolation model's guarantees (SSI vs. stricter active conflict checks vs. pure OCC).
- Tracking transaction states and timestamps is essential for correct SSI validation.
- Refactoring validation logic into the `DependencyTracker` improves modularity for the `Serializable` level.
