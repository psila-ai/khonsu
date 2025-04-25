# Active Context: Khonsu - Software Transactional Memory

## Current Work Focus

The current focus is on implementing full Serializable isolation, which requires introducing transaction dependency tracking and a validation algorithm. This is a significant expansion of the initial core STM implementation.

## Recent Changes

- Completed the initial core STM implementation steps as previously outlined.
- Addressed user feedback regarding `TxnBuffer::insert` and the `read` method `TODO`.
- Implemented basic conflict detection logic for ReadCommitted, RepeatableRead, and Serializable (excluding full serializability checks).
- Updated `src/arrow_utils.rs` to support additional data types and removed unused functions.
- Added basic test cases for core transaction operations.

## Next Steps

1.  **Implement Transaction Dependency Tracking:** Design and implement data structures and logic to track read and write dependencies between transactions.
2.  **Integrate Dependency Tracking:** Modify the transaction lifecycle (start, read, write, commit) to record dependencies.
3.  **Implement Serializable Validation Algorithm:** Develop an algorithm (e.g., based on cycle detection in a dependency graph) to check for serializability violations during commit.
4.  **Integrate Serializable Validation:** Modify the `detect_conflicts` function to use the dependency tracking and validation algorithm for Serializable transactions.
5.  **Refine Conflict Detection:** Review and refine existing conflict detection logic based on the new dependency tracking mechanisms.
6.  **Write Comprehensive Tests:** Add comprehensive test cases specifically for Serializable isolation and conflict scenarios.
7.  **Implement TwoPhaseCommitParticipant:** Complete the implementation of the `TwoPhaseCommitParticipant` trait for distributed commit integration (revisiting this after core serializability).
8.  **Refine Memory Reclamation:** Review memory reclamation in light of new data structures for dependency tracking.
9.  **Distributed Commit Functionality:** Implement the full distributed commit protocol (future phase).

## Active Decisions and Considerations

- Implementing the lock-free commit process atomically and efficiently using the `SkipMap`-based TxnBuffer.
- Designing the exact structure of `TransactionChanges` for the `TwoPhaseCommitParticipant` trait.
- Ensuring minimal allocation throughout the implementation.
- Handling potential global counter overflow (though considered improbable with `u64`).
- Precisely implementing the validation logic for each isolation level, especially `Serializable`.
- Writing effective tests for concurrent lock-free code.
- Ensuring the `Storage` trait remains free of transaction-specific logic and is solely for persistence.
- The distinction and interaction between the in-memory TxnBuffer and the persistent Storage.

## Learnings and Project Insights

- The choice of `crossbeam-skiplist::SkipMap` and `crossbeam-queue::SegQueue` provides a strong foundation for lock-free internals in the TxnBuffer.
- Integrating with external systems via the `Storage` and `TwoPhaseCommitParticipant` traits promotes extensibility.
- Careful code organization into small files will be crucial for managing complexity.
- Manual Arrow manipulation requires a good understanding of the Arrow data structures.
- Achieving true lock-freedom without syscalls in critical paths requires meticulous implementation.
- A dedicated error file improves code organization and clarity.
- Keeping the `Storage` trait focused simplifies integration with various persistence layers.
- Clearly defining the roles of the in-memory Transaction Buffer and persistent Storage is essential for understanding the system architecture.
