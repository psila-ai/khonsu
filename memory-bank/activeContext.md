# Active Context: Khonsu - Software Transactional Memory

## Current Work Focus

The initial implementation of Serializable Snapshot Isolation (SSI) for the `Serializable` transaction isolation level is complete, and the existing test suite now passes with adjustments made to align test expectations with standard SSI behavior. The focus shifts towards refining the SSI implementation, adding more comprehensive tests, and potentially moving towards distributed commit features.

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

1.  **Refine SSI Implementation:**
    *   Improve the heuristic backward check for deleted items (requires deletion timestamp tracking).
    *   Optimize SSI validation checks (e.g., data structures, lock contention).
    *   Refine garbage collection logic in `DependencyTracker`.
2.  **Write Comprehensive SSI Tests:** Add more targeted tests for various SSI conflict scenarios, including different interleavings and edge cases.
3.  **Implement TwoPhaseCommitParticipant:** Complete the implementation of the `TwoPhaseCommitParticipant` trait for distributed commit integration.
4.  **Refine Memory Reclamation:** Review memory reclamation, especially concerning the `TransactionInfo` and `ItemDependency` structures.
5.  **Distributed Commit Functionality:** Implement the full distributed commit protocol (future phase).

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
