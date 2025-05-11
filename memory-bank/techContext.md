# Technical Context: Khonsu - Software Transactional Memory

## Technologies Used

- **Rust:** The primary programming language for its memory safety, performance, and concurrency features.
- **Arrow:** Used for representing and manipulating tabular data (rows and columns) in the form of RecordBatches.
- **Atomic Operations:** Leveraging Rust's `std::sync::atomic` for lock-free programming.
- **OmniPaxos:** Used for achieving consensus on transaction commits in the distributed commit process (gated by the `distributed` feature). Combined with Two-Phase Commit (2PC) for transaction coordination to create a crash-resistant distributed transaction system.
- **Crossbeam Channels:** Used for inter-thread communication, particularly between the main Khonsu thread and the OmniPaxos event loop thread managed by the `DistributedCommitManager` (gated by the `distributed` feature).
- **RocksDB:** Used as the local write-ahead log (WAL) for the `DistributedCommitStorage` implementation, ensuring persistence and supporting crash tolerance and PITR (gated by the `distributed` feature). All transaction state and OmniPaxos log entries are persisted to RocksDB, allowing nodes to recover their state after a crash.
- **gRPC:** Used for inter-node network communication in the distributed commit process (gated by the `distributed` feature). Implemented with Tokio for async operations.
- **Arrow IPC Format:** Used for serializing Arrow RecordBatches for network transmission in the distributed commit process.

## Development Setup

- Standard Rust development environment with Cargo.
- Requires setting up protobuf compilation for gRPC.

## Technical Constraints

- **`distributed` Feature Flag:** Distributed commit functionality is gated behind the `distributed` feature flag.
- **Distributed Logic Encapsulation:** All code related to distributed commit, including OmniPaxos instance management, event loop, and communication, must reside entirely within the `src/distributed` module and be gated by the `distributed` feature flag.
- **Persistence, Crash Tolerance, and PITR:** Committed operations and transactions must be durably persisted to allow for recovery to any point in time. The `rocksdb` WAL is crucial for achieving this.
- **DistributedCommitStorage Implementation:** The `DistributedCommitStorage` must use `rocksdb` as the local WAL and ensure atomic operations for log manipulation as required by the OmniPaxos storage trait, contributing to persistence and PITR.
- **Local Write-Ahead Logs (WALs):** Assumes the presence of durable local WALs at each node for logging transaction states and changes (implemented using `rocksdb`).
- **No `arrow-compute` feature:** Arithmetic and scalar operations on Arrow data must be implemented manually using Arrow's array manipulation capabilities.
- **Async Runtime for gRPC:** The constraint "No Async Runtime (e.g., Tokio)" is relaxed for the `distributed` module to allow the use of Tokio for the gRPC network layer.
- **Single-threaded tests:** Tests should be configured to run on a single thread to avoid potential issues with test infrastructure and focus on the correctness of the STM implementation itself under controlled conditions.
- **Lock-Free Internals:** Avoid using standard library mutexes, RwLocks, or other blocking synchronization primitives and system calls in the core STM logic.
- **Minimum Allocation:** Design data structures and algorithms to minimize dynamic memory allocation, especially in performance-critical paths.

## Dependencies

- `arrow`: For data representation.
- `omnipaxos`: Used for achieving consensus on transaction commits in the distributed commit process (gated by the `distributed` feature).
- `crossbeam-channel`: Used for inter-thread communication (gated by the `distributed` feature).
- `rocksdb`: Used as the local WAL for distributed commit storage, ensuring persistence and PITR support (gated by the `distributed` feature).
- `tonic`: gRPC framework (gated by the `distributed` feature).
- `prost`: Protocol Buffers encoding/decoding (gated by the `distributed` feature).
- `prost-derive`: Derive macros for Prost (gated by the `distributed` feature).
- `tokio`: Asynchronous runtime for gRPC (gated by the `distributed` feature, with necessary features).
- `parking_lot`: For `RwLock` used in `TxnBuffer`.
- `crossbeam-epoch`: For safe memory reclamation in lock-free data structures (used in DependencyTracker).
- `crossbeam-skiplist`: For the skip map used in the DependencyTracker.
- `crossbeam-queue`: For lock-free queues (`SegQueue`) used internally.
- `futures`: If an executor is needed, the `executor` feature will be used (potentially in conjunction with Tokio for gRPC).
- `std::sync::atomic`: For atomic operations (used for transaction counter).
- **Concurrent Data Structures:** Utilizing `parking_lot::RwLock` for `TxnBuffer` and `crossbeam-skiplist` for `DependencyTracker`.
- **Concurrent Algorithms:** Implementing concurrent algorithms for tasks like cycle detection in the transaction dependency graph.

## Tool Usage Patterns

- Use `cargo build` and `cargo test` for building and testing.
- Need to run `cargo build` to trigger the `build.rs` script for protobuf compilation.
- Utilize Rust's profiling tools if necessary to identify performance bottlenecks related to allocation or contention.
