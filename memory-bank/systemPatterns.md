# System Patterns: Khonsu - Software Transactional Memory

## Architecture

The core system follows a typical STM architecture with a shared in-memory Transaction Buffer (TxnBuffer). For distributed transactions, the system leverages `omnipaxos` for achieving consensus on transaction commits across multiple nodes, communicating via gRPC, with Two-Phase Commit (2PC) for transaction coordination. A key architectural constraint is that all distributed commit related logic, including the OmniPaxos instance and its event loop, is encapsulated within a `DistributedCommitManager` residing in the `src/distributed` module. This module uses an asynchronous runtime (Tokio) to support gRPC, which is an exception to the general constraint. Each node runs its own `DistributedCommitManager` instance, which interacts with the local Khonsu instance and manages stable storage using `rocksdb` as a write-ahead log (WAL) to ensure persistence and support crash tolerance and Point-in-Time Recovery (PITR). Transaction commits are proposed to the `DistributedCommitManager` for replication and ordering via OmniPaxos. The system is designed to be crash-resistant, with all transaction state and OmniPaxos log entries persisted to RocksDB, allowing nodes to recover their state after a crash and continue participating in the cluster.

## Key Technical Decisions

- **Concurrency Control:** Utilize `parking_lot::RwLock` to protect the shared `TxnBuffer`. While the initial goal was lock-free internals, the current implementation uses a standard reader-writer lock for the main data buffer. Atomic operations from `std::sync::atomic` are used for the transaction counter. `crossbeam-queue::SegQueue` is used for internal queuing where needed.
- **Data Representation:** Data will be managed in chunks compatible with Arrow RecordBatches. The internal representation should allow for efficient updates and versioning.
- **Versioning:** Implement a mechanism for versioning data or transactions to support conflict detection and rollback. This involves assigning a version (commit timestamp) to each committed change.
- **Conflict Detection:** During commit, compare the read and written data versions of the committing transaction against the current state in the Transaction Buffer (TxnBuffer) to identify conflicts with other concurrently committed transactions.
- **Conflict Resolution:** Implement the specified strategies (append, ignore, replace, fail) based on the type of conflict and configuration.
- **Rollback:** Design the transaction mechanism to easily discard staged changes and revert to the state before the transaction began. This is simplified by operating on private copies/views.
- **Transaction Dependency Tracking:** Implement a mechanism to track read and write dependencies between transactions. This is essential for enforcing Serializable isolation and involves using a `DependencyTracker` to record which transactions read or wrote which data items. The `DependencyTracker` currently uses `crossbeam-skiplist::SkipMap`, but the read-modify-write operations on `ItemDependency` within the SkipMap are not truly atomic with `crossbeam-skiplist 0.1`.

## Component Relationships

- **Transaction Manager:** Orchestrates transactions, handles start, commit, and rollback requests. Interacts with the Transaction Buffer (TxnBuffer).
- **Transaction Buffer (TxnBuffer):** The central in-memory repository for the data, implemented using a `HashMap` protected by `parking_lot::RwLock` for concurrent access and updates. Manages data versioning.
- **Transaction:** Represents an ongoing unit of work. Holds the transaction's read set, write set (staged changes), and configuration (isolation level, resolution strategy). Interacts with the `DistributedCommitManager` for distributed commits.
- **Storage Trait:** An external trait that the STM system will interact with to **persist** committed data to durable storage. This trait needs to support atomic writes of RecordBatches and will not contain any transaction-specific logic.
- **DistributedCommitManager:** A new component within the `src/distributed` module that encapsulates the OmniPaxos instance, its configuration, gRPC network communication, and event loop. It uses `rocksdb` as its local WAL for persistence and recovery. It receives commit proposals from local transactions, proposes them to OmniPaxos, processes decided entries, and applies changes to the local Khonsu instance and WAL.
- **Local Khonsu Instance:** Each node will have a local Khonsu instance managing its portion of the data and interacting with its local WAL. It holds an instance of the `DistributedCommitManager` when the `distributed` feature is enabled.

## Critical Implementation Paths

- **Transaction Start:** Creating a new transaction with its own view/copy of the relevant data based on the isolation level, referencing the Transaction Buffer (TxnBuffer).
- **Data Access (Read/Write):** Efficiently reading data into the transaction's private space from the Transaction Buffer (TxnBuffer) and staging writes without affecting the buffer immediately.
- **Commit Process (Distributed):** Local validation, sending the commit proposal to the `DistributedCommitManager`, waiting for the OmniPaxos decision, and returning the outcome. This path must ensure durability and crash tolerance and involves gRPC communication.
- **Commit Process (Local - handled by DistributedCommitManager):** Receiving decided commit entries from OmniPaxos (via gRPC server), applying the transaction changes to the Transaction Buffer (TxnBuffer), and durably persisting these changes and the fact of their application to the local WAL (using `rocksdb`) to support PITR.
- **Rollback Process:** Discarding staged changes and cleaning up transaction resources, including handling rollbacks for transactions not decided by `omnipaxos`. Recovery from crashes using the `rocksdb` WAL to restore the state up to a consistent point.
