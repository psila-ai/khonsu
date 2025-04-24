# Progress: Khonsu - Software Transactional Memory

## What Works

- Initial project understanding and documentation setup.

## What's Left to Build

- Core STM data structures (`VersionedValue`, `SkipMap`-based Transaction Buffer (TxnBuffer)).
- Transaction struct and methods.
- Khonsu struct and transaction start API.
- Lock-free commit process (validation against TxnBuffer, conflict handling, atomic application to TxnBuffer).
- Rollback functionality.
- Implementation of `TransactionIsolation` levels logic.
- `Storage` trait implementation and integration for persistence.
- `TwoPhaseCommitParticipant` trait implementation.
- Manual Arrow array manipulation helper functions.
- Define and implement error types in `errors.rs`.
- Comprehensive test suite in `tests/`.
- Refine memory reclamation using `crossbeam-epoch`.
- Distributed commit functionality (future phase).

## Current Status

The project is in the initial planning and documentation phase. No code has been written yet.

## Known Issues

- None at this stage.

## Evolution of Project Decisions

- The decision to use Rust and Arrow is established.
- The requirement for lock-free internals and minimum allocation will heavily influence data structure and algorithm choices.
- The specific lock-free techniques and data structures will be decided during the planning phase.
