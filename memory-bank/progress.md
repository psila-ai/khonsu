# Progress: Khonsu - Software Transactional Memory

## What Works

- Initial project setup and memory bank documentation.
- Core STM data structures (`VersionedValue`, `SkipMap`-based Transaction Buffer (TxnBuffer)).
- Basic `Transaction` struct and methods (`read`, `write`, `delete`, `commit`, `rollback`).
- `Khonsu` struct and transaction start API.
- Basic commit process with conflict detection and resolution strategies (`Fail`, `Ignore`, `Replace`, `Append`).
- Rollback functionality.
- Basic logic for different `TransactionIsolation` levels within conflict detection.
- `Storage` trait definition and a `MockStorage` implementation integrated into the commit process.
- Helper functions for manual Arrow array manipulation (`merge_record_batches`) with support for basic data types.
- Defined error types in `errors.rs`.
- Basic test cases for core transaction operations.
- Transaction dependency tracking data structures (`DependencyTracker`, `ItemDependency`, `DataItem`) defined.
- Integration of `DependencyTracker` into `Khonsu` and `Transaction`.
- Placeholder methods for recording dependencies (`record_read`, `record_write`) and cycle detection (`check_for_cycles`) in `DependencyTracker`.

## What's Left to Build

- Full implementation of transaction dependency recording logic in `DependencyTracker` (handling concurrent updates and version tracking).
- Implementation of the serializability validation algorithm (cycle detection) in `DependencyTracker::check_for_cycles`.
- Integration of serializability validation into the `Transaction::commit` process for Serializable transactions.
- Implementation of methods for removing dependencies of committed or aborted transactions from the `DependencyTracker`.
- Comprehensive test cases for different isolation levels, conflict resolution strategies, and concurrent scenarios, especially for Serializable isolation.
- Complete implementation of the `TwoPhaseCommitParticipant` trait.
- Refinement of memory reclamation, particularly for the dependency tracking data structures.
- Full distributed commit functionality (future phase).

## Current Status

The project has completed the initial core STM implementation and is currently focused on implementing full Serializable isolation by adding transaction dependency tracking and validation.

## Known Issues

- Compilation errors in `src/dependency_tracking.rs` related to using `errors::Error` and potential issues with the read-modify-write loop for updating `ItemDependency` in the `SkipMap` (due to limitations of `crossbeam-skiplist` version 0.1).

## Evolution of Project Decisions

- Decision to use Rust and Arrow remains central.
- Commitment to lock-free internals and minimum allocation continues to influence design.
- Decision made to implement full Serializable isolation now, requiring the introduction of transaction dependency tracking and a validation algorithm, which is a significant architectural addition.
- Using `crossbeam-skiplist` version 0.1 presents challenges for atomic read-modify-write operations needed for dependency tracking, requiring careful implementation with potential limitations.
