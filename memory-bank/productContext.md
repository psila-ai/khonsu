# Product Context: Khonsu - Software Transactional Memory

## Purpose

Khonsu is being developed as a core component for a database system. Its primary purpose is to provide a robust and efficient mechanism for managing concurrent data access and modifications.

## Problem Solved

The library addresses the challenge of ensuring data consistency and integrity in a multi-threaded environment without relying on traditional locking mechanisms that can introduce contention and reduce performance. It enables atomic operations on data, crucial for reliable database transactions.

## How it Should Work

- Transactions should operate on data stored in a shared, mutable state.
- Modifications within a transaction are initially staged and not immediately visible to other transactions.
- During commit, the changes are validated against the current state to detect conflicts.
- Based on the configured isolation level and conflict resolution strategy, conflicts are handled appropriately.
- Successful transactions are applied atomically, making their changes visible.
- Failed transactions or explicit rollbacks discard staged changes.
- **Persistence and Recovery:** Committed transactions and operations are durably persisted to RocksDB, enabling crash tolerance and Point-in-Time Recovery (PITR). The system can recover from node failures while maintaining consistency.
- **Distributed Transactions:** The system supports distributed transactions across multiple nodes, with OmniPaxos for consensus and Two-Phase Commit (2PC) for transaction coordination. This ensures that all nodes either commit or abort a transaction atomically, even in the presence of node failures.

## User Experience Goals

- Provide a simple and intuitive API for starting, committing, and rolling back transactions.
- Allow users to perform create, update, and delete operations on data represented as Arrow RecordBatches within a transaction.
- Offer flexibility in handling conflicts through configurable resolution strategies.
- Ensure high performance and low overhead for transaction processing.
