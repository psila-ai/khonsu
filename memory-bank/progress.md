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

-   **Distributed Commit Implementation:**
    -   Add `twopc` feature flag and `omnipaxos` dependency.
    -   Design and Implement Two-Phase Commit Coordinator.
    -   Implement Shared Transaction Log (based on local WALs and `omnipaxos`).
    -   Implement 2PC Protocol Logic (initiate, prepare, commit/abort phases).
    -   Implement `TwoPhaseCommitParticipant` for Khonsu (including WAL interactions).
    -   Integrate Coordinator with Khonsu.
    -   Implement Error Handling and Recovery for distributed transactions.
    -   Write Comprehensive Distributed Commit Tests.
-   **Refine SSI Implementation:**
    -   Improve backward validation for deleted items (currently heuristic).
    -   Optimize SSI validation performance.
    -   Refine garbage collection strategy and memory usage for `DependencyTracker`.
-   **Comprehensive SSI Tests:** Add more targeted tests for various SSI conflict scenarios.
-   **Refine Memory Reclamation:** Review overall memory management.
-   **Address `TODO`s:** Review and address any remaining `TODO` comments in the codebase.

## Current Status

The project has shifted focus to implementing the distributed commit functionality using a two-phase commit (2PC) protocol, gated by the `twopc` feature flag and utilizing `omnipaxos` and local WALs. The plan for this implementation is documented. Remaining tasks include completing the distributed commit implementation, further refining the SSI implementation, adding more comprehensive tests for both features, and refining memory reclamation.

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
