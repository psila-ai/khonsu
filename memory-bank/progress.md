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
- **Serializable Snapshot Isolation (SSI) Implementation (Initial):**
    - `DependencyTracker` now tracks transaction states (`Active`, `Committed`, `Aborted`), timestamps, and recently committed write sets (`TransactionInfo`).
    - `validate_serializability` function implements SSI backward (SI read) and forward (dangerous RW structure) checks.
    - Transaction lifecycle (`new`, `commit`, `rollback`) integrates with `DependencyTracker` for state updates.
    - Basic garbage collection for old transaction info in `DependencyTracker`.
    - `record_read` and `record_write` update active dependencies in `item_dependencies`.
    - `remove_active_dependencies` cleans up `item_dependencies` on commit/abort.
- **Basic Test Suite:** Core transaction operations and specific SSI conflict scenarios (`wrw`, `rw`, `ww`, dependency removal) are tested and passing after aligning test expectations with SSI behavior.

## What's Left to Build

- **Refine SSI Implementation:**
    - Improve backward validation for deleted items (currently heuristic).
    - Optimize SSI validation performance.
    - Refine garbage collection strategy and memory usage for `DependencyTracker`.
- **Comprehensive SSI Tests:** Add more tests covering diverse interleavings, edge cases, and concurrent scenarios for SSI.
- **Implement `TwoPhaseCommitParticipant`:** Complete the trait implementation for distributed commit protocols.
- **Refine Memory Reclamation:** Review overall memory management, especially for `DependencyTracker`.
- **Distributed Commit Functionality:** Implement the full distributed commit protocol (future phase).
- **Address `TODO`s:** Review and address any remaining `TODO` comments in the codebase.

## Current Status

The project has implemented an initial version of Serializable Snapshot Isolation (SSI) for the `Serializable` isolation level. The core logic is in place within `DependencyTracker` and integrated into the `Transaction` lifecycle. The existing test suite passes after adjustments to align with SSI principles. Focus is now on refining the SSI implementation and expanding test coverage.

## Known Issues

- The backward validation check for items deleted concurrently relies on a heuristic and needs improvement (requires tracking deletion timestamps or similar).
- Performance implications of SSI validation and `DependencyTracker` state management haven't been thoroughly evaluated.
- Garbage collection for `DependencyTracker` is basic and might need refinement based on usage patterns.

## Evolution of Project Decisions

- Shifted from simple cycle detection/OCC for Serializable to implementing standard Serializable Snapshot Isolation (SSI).
- Refactored validation logic, centralizing SSI checks in `DependencyTracker::validate_serializability` and simplifying `detect_conflicts`.
- Adjusted test expectations to match the guarantees provided by SSI.
- `DependencyTracker` now holds more state (`TransactionInfo`) to support SSI validation.
- `TxnBuffer` implementation confirmed as `RwLock<HashMap>`, not `SkipMap` as mentioned in older memory bank entries.
