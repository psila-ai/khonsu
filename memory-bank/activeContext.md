# Active Context: Khonsu - Software Transactional Memory

## Current Work Focus

The current focus is on finalizing the detailed plan for the Khonsu STM library, incorporating specific lock-free data structures and external integration traits, and clarifying the role of the in-memory Transaction Buffer (TxnBuffer) versus persistent Storage.

## Recent Changes

- Created the `memory-bank` directory and populated initial core documentation files.
- Developed a detailed plan for the STM architecture, including core components, data management, concurrency control, transaction lifecycle, and external traits.
- Incorporated specific lock-free data structures (`crossbeam-skiplist::SkipMap`, `crossbeam-queue::SegQueue`) and the `TwoPhaseCommitParticipant` trait into the plan.
- Defined the code organization strategy using multiple small files and specified the test location (`tests/`).
- Updated `systemPatterns.md` and `techContext.md` to reflect the refined plan.
- Renamed the in-memory Shared Data Store to Transaction Buffer (TxnBuffer) for clarity.

## Next Steps

1. Implement the core data structures: `VersionedValue` and the `SkipMap`-based Transaction Buffer (TxnBuffer).
2. Implement the `Transaction` struct and its methods (`read`, `write`, `delete`).
3. Implement the `Khonsu` struct and the transaction start API.
4. Implement the commit process, including validation against the TxnBuffer, conflict detection, resolution, and atomically applying changes to the TxnBuffer.
5. Implement the rollback functionality.
6. Implement the logic for different `TransactionIsolation` levels.
7. Implement the `Storage` trait and integrate it into the commit process for persistence.
8. Implement the `TwoPhaseCommitParticipant` trait for distributed commit integration.
9. Implement helper functions for manual Arrow array manipulation in `arrow_utils.rs`.
10. Define and implement error types in `errors.rs`.
11. Write comprehensive tests in the `tests/` directory, ensuring single-threaded execution.
12. Refine memory reclamation using `crossbeam-epoch`.

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
