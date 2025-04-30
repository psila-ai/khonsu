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

-   **Distributed Commit Implementation (using Omnipaxos and `DistributedCommitManager`):**
    -   Ensure `distributed` feature flag and `omnipaxos`, `crossbeam-channel`, `serde` dependencies are added.
    -   Define Replicated Commit Data Structure (`ReplicatedCommit`) in `src/distributed/mod.rs`.
    -   Implement Omnipaxos Storage Backend (`DistributedCommitStorage`) in `src/distributed/storage.rs` using a local WAL.
    -   Implement Omnipaxos Network Layer (`KhonsuNetwork`) in `src/distributed/network.rs` using `crossbeam-channel`.
    -   Create `DistributedCommitManager` struct within `src/distributed` to encapsulate OmniPaxos logic.
    -   Integrate `DistributedCommitManager` with Khonsu (Khonsu holds an instance).
    -   Modify Transaction Commit for Distributed Workflow (Transaction sends proposals to Manager).
    -   Implement Error Handling and Recovery within the Manager.
    -   Implement Application of Decided Entries within the Manager's event loop.
    -   Write Comprehensive Distributed Commit Tests.
-   **Refine SSI Implementation:**
    -   Improve backward validation for deleted items (currently heuristic).
    -   Optimize SSI validation performance.
    -   Refine garbage collection strategy and memory usage for `DependencyTracker`.
-   **Comprehensive SSI Tests:** Add more targeted tests for various SSI conflict scenarios.
-   **Refine Memory Reclamation:** Review overall memory management.
-   **Address `TODO`s:** Review and address any remaining `TODO` comments in the codebase.

## Current Status

The project has shifted focus to implementing the distributed commit functionality using `omnipaxos` for full consensus and replication, gated by the `distributed` feature flag and utilizing local WALs and `crossbeam-channel` for inter-thread communication. A key constraint is to keep all distributed commit related code within the `src/distributed` module using a `DistributedCommitManager`. The revised plan for this implementation is documented. Remaining tasks include completing the distributed commit implementation, further refining the SSI implementation, adding more comprehensive tests for both features, and refining memory reclamation.

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
- Decided to use `omnipaxos` for the full distributed commit process instead of a 2PC approach with `omnipaxos` for voting.
- Renamed the `twopc` feature flag to `distributed`.
- Switched from `tokio::sync::mpsc` to `crossbeam-channel` for inter-thread communication.
- **Constraint:** All distributed commit related code must be encapsulated within the `src/distributed` module and gated by the `distributed` feature flag, managed by a `DistributedCommitManager`.
