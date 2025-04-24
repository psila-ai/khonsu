# Project Brief: Khonsu - Software Transactional Memory

## Core Requirements

- Implement Software Transactional Memory (STM) in Rust.
- Lock-free internals for high performance.
- Minimize memory allocation.
- Integrate with Arrow RecordBatches for row-level operations (create, update, delete).
- Support multiple RecordBatches per transaction.
- Implement conflict detection during transaction commit.
- Provide conflict resolution strategies: append, ignore, replace, fail.
- Ensure complete rollback to the previous state on transaction abort.
- Support configurable transaction isolation levels: ReadCommitted, RepeatableRead, Serializable.
- Design for future extension to support distributed commit.
- Adhere to constraints: no Arrow compute feature, implement arithmetic scalar operations manually, run tests single-threaded.

## Goals

- Develop a high-performance, reliable STM library suitable for a database system.
- Provide a clear and efficient API for transaction management and data manipulation using RecordBatches.
- Ensure correctness and data consistency under concurrent access.
- Lay the groundwork for distributed transaction capabilities.
