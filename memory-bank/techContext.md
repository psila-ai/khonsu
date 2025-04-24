# Technical Context: Khonsu - Software Transactional Memory

## Technologies Used

- **Rust:** The primary programming language for its memory safety, performance, and concurrency features.
- **Arrow:** Used for representing and manipulating tabular data (rows and columns) in the form of RecordBatches.
- **Atomic Operations:** Leveraging Rust's `std::sync::atomic` for lock-free programming.

## Development Setup

- Standard Rust development environment with Cargo.

## Technical Constraints

- **No `arrow-compute` feature:** Arithmetic and scalar operations on Arrow data must be implemented manually using Arrow's array manipulation capabilities.
- **No Async Runtime (e.g., Tokio):** The project will not use `tokio` or any other full async runtime. If an executor is needed, the `futures` crate with the `executor` feature will be used.
- **Single-threaded tests:** Tests should be configured to run on a single thread to avoid potential issues with test infrastructure and focus on the correctness of the STM implementation itself under controlled conditions.
- **Lock-Free Internals:** Avoid using standard library mutexes, RwLocks, or other blocking synchronization primitives and system calls in the core STM logic.
- **Minimum Allocation:** Design data structures and algorithms to minimize dynamic memory allocation, especially in performance-critical paths.

## Dependencies

- `arrow`: For data representation.
- `crossbeam-epoch`: For safe memory reclamation in lock-free data structures.
- `crossbeam-skiplist`: For the lock-free skip map used in the Shared Data Store.
- `crossbeam-queue`: For lock-free queues (`SegQueue`) used internally.
- `futures`: If an executor is needed, the `executor` feature will be used.
- `std::sync::atomic`: For atomic operations.

## Tool Usage Patterns

- Use `cargo build` and `cargo test` for building and testing.
- Utilize Rust's profiling tools if necessary to identify performance bottlenecks related to allocation or contention.
