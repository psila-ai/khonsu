# System Patterns: Khonsu - Software Transactional Memory

## Architecture

The system will follow a typical STM architecture. There will be a shared, globally accessible **Transaction Buffer (TxnBuffer)** in memory that holds the current state of the data for active transactions. Transactions will operate on private copies or views of this data.

## Key Technical Decisions

- **Lock-Free Internals:** Utilize atomic operations and specific lock-free data structures from the `crossbeam` crate: `crossbeam-skiplist::SkipMap` for the Transaction Buffer (TxnBuffer) and `crossbeam-queue::SegQueue` for internal queuing. Avoid traditional mutexes or locks and system calls in the core transaction path.
- **Data Representation:** Data will be managed in chunks compatible with Arrow RecordBatches. The internal representation should allow for efficient updates and versioning.
- **Versioning:** Implement a mechanism for versioning data or transactions to support conflict detection and rollback. This could involve multi-version concurrency control (MVCC) principles.
- **Conflict Detection:** During commit, compare the read and written data versions of the committing transaction against the current state in the Transaction Buffer (TxnBuffer) to identify conflicts with other concurrently committed transactions.
- **Conflict Resolution:** Implement the specified strategies (append, ignore, replace, fail) based on the type of conflict and configuration.
- **Rollback:** Design the transaction mechanism to easily discard staged changes and revert to the state before the transaction began. This is simplified by operating on private copies/views.
- **Transaction Dependency Tracking:** Implement a mechanism to track read and write dependencies between transactions. This is essential for enforcing Serializable isolation and involves using a `DependencyTracker` to record which transactions read or wrote which data items.

## Component Relationships

- **Transaction Manager:** Orchestrates transactions, handles start, commit, and rollback requests. Interacts with the Transaction Buffer (TxnBuffer).
- **Transaction Buffer (TxnBuffer):** The central in-memory repository for the data, implemented using `crossbeam-skiplist::SkipMap` for concurrent, lock-free access and updates. Manages data versioning.
- **Transaction:** Represents an ongoing unit of work. Holds the transaction's read set, write set (staged changes), and configuration (isolation level, resolution strategy).
- **Storage Trait:** An external trait that the STM system will interact with to **persist** committed data to durable storage. This trait needs to support atomic writes of RecordBatches and will not contain any transaction-specific logic.
- **TwoPhaseCommitParticipant Trait:** An external trait implemented by the `Khonsu` instance to participate in distributed commit protocols, inspired by `omnipaxos`.

## Critical Implementation Paths

- **Transaction Start:** Creating a new transaction with its own view/copy of the relevant data based on the isolation level, referencing the Transaction Buffer (TxnBuffer).
- **Data Access (Read/Write):** Efficiently reading data into the transaction's private space from the Transaction Buffer (TxnBuffer) and staging writes without affecting the buffer immediately.
- **Commit Process:** The core logic involving validation against the Transaction Buffer (TxnBuffer), conflict detection, resolution, and atomically applying changes to the Transaction Buffer (TxnBuffer).
- **Rollback Process:** Discarding staged changes and cleaning up transaction resources.
