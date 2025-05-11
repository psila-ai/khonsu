# Active Context: Khonsu - Software Transactional Memory

## Current Work Focus

The focus has been on implementing the distributed commit functionality using `omnipaxos` for full consensus and replication of transaction commits, combined with Two-Phase Commit (2PC) for transaction coordination, all gated by the `distributed` feature flag. We've successfully implemented a crash-resistant distributed transaction system that can recover from node failures while maintaining consistency. The implementation keeps all distributed commit related code within the `src/distributed` module, with a `DistributedCommitManager` that encapsulates the OmniPaxos instance, 2PC logic, and network communication. We've implemented persistence and crash tolerance using RocksDB as the local write-ahead log (WAL) for durable storage of transaction changes and OmniPaxos log entries, and used gRPC for network communication between nodes.

## Recent Changes

- **Implemented Distributed Commits with OmniPaxos and 2PC:**
    - Created a `DistributedCommitManager` that encapsulates OmniPaxos instance, 2PC logic, and network communication
    - Implemented RocksDB-backed storage for OmniPaxos (`DistributedCommitStorage`) that provides durability and crash recovery
    - Implemented network communication using gRPC and Tokio for async operations
    - Integrated Two-Phase Commit (2PC) over OmniPaxos for transaction coordination
    - Added crash resistance and recovery mechanisms
    - Modified `Transaction::commit` to use the distributed commit system when the `distributed` feature is enabled
    - Implemented global transaction IDs that are unique across the cluster
    - Added serialization support for Arrow RecordBatches using IPC format
    - Created a channel extension system for node identification

- **Implemented Serializable Snapshot Isolation (SSI):**
    - Modified `DependencyTracker` to track transaction states (`Active`, `Committed`, `Aborted`), start/commit timestamps, and recently committed write sets (`TransactionInfo`).
    - Implemented `validate_serializability` in `DependencyTracker` to perform SSI checks:
        - **Backward Validation:** Checks reads against concurrently committed writes (SI read check).
        - **Forward Validation:** Detects dangerous RW dependency structures involving active and recently committed transactions.
    - Integrated transaction state tracking (`register_txn`, `mark_committed`, `mark_aborted`) into the transaction lifecycle (`Transaction::new`, `commit`, `rollback`).
    - Added basic garbage collection for old transaction info in `DependencyTracker`.

- **Refactored Validation Logic:**
    - `Transaction::commit` now calls only `validate_serializability` for `Serializable` isolation.
    - `detect_conflicts` is simplified to handle only `ReadCommitted` and `RepeatableRead` using standard OCC checks against the `TxnBuffer`.

- **Removed Unused Code:**
    - Cleaned up unused imports and variables in the distributed module
    - Removed unused fields from structs
    - Fixed compiler warnings

## Next Steps

1. **Enhance Crash Recovery:**
   - Add more comprehensive recovery tests for various failure scenarios
   - Implement automatic recovery of in-progress transactions after node restart

2. **Improve Distributed Serializable Isolation:**
   - Implement global dependency tracking across nodes for true serializable isolation
   - Add timestamp synchronization or vector clocks for consistent ordering
   - Enhance validation protocols to check for conflicts across all nodes
   - Update documentation to clarify which isolation levels are fully supported in distributed mode
   - Add examples specific to distributed operation with appropriate isolation levels

3. **Optimize Performance:**
   - Profile and optimize the distributed commit path
   - Reduce serialization overhead for large RecordBatches
   - Implement batching for network communication

4. **Add Monitoring and Metrics:**
   - Add metrics for transaction throughput, latency, and success/failure rates
   - Implement monitoring for node health and cluster status

5. **Improve Distributed Transaction Routing:**
   - Implement smarter routing of transactions to nodes based on data locality
   - Add support for read-only transactions that don't need consensus

6. **Refine SSI Implementation:**
   - Improve backward validation for deleted items (currently heuristic)
   - Optimize SSI validation performance
   - Refine garbage collection strategy and memory usage for `DependencyTracker`

7. **Comprehensive Testing:**
   - Add more targeted tests for various SSI conflict scenarios
   - Add tests for network partitions and other failure modes
   - Implement stress testing for the distributed system

## Active Decisions and Considerations

- **Integration of 2PC and OmniPaxos:** We've implemented a hybrid approach where 2PC is used for transaction coordination, but the 2PC protocol itself is made fault-tolerant by replicating its state through OmniPaxos. This provides the best of both worlds: the simplicity of 2PC for transaction coordination and the fault tolerance of OmniPaxos for consensus.

- **Crash Tolerance and Recovery:** The system is designed to be crash-tolerant, with all transaction state and OmniPaxos log entries persisted to RocksDB. When a node restarts after a crash, it can recover its state from the WAL and continue participating in the cluster.

- **Constraint: Distributed logic in `src/distributed` module:** All code related to distributed commit, including OmniPaxos instance management, event loop, and communication, resides entirely within the `src/distributed` module and is gated by the `distributed` feature flag. The `Khonsu` struct interacts with this module through a defined interface.

- **DistributedCommitStorage Implementation:** We use `rocksdb` as the local WAL for the `DistributedCommitStorage` implementation, ensuring atomic operations for log manipulation and durability for PITR.

- **Network Implementation:** We use gRPC for inter-node communication, with protobuf messages for serialization and Tokio for async operations.

- **Async Runtime:** The constraint "No Async Runtime (e.g., Tokio)" is relaxed for the `distributed` module to allow the use of Tokio for the gRPC network layer.

- **Global Transaction IDs:** We've implemented a scheme for generating globally unique transaction IDs that combine the node ID, a local transaction ID, and a timestamp.

- **Serialization of Arrow RecordBatches:** We use Arrow's IPC format to serialize RecordBatches for network transmission, which provides efficient serialization and deserialization.

- **Memory Usage:** We need to monitor memory usage of the `DependencyTracker`, particularly the storage of committed write sets, and the OmniPaxos log.

## Learnings and Project Insights

- **Combining 2PC and OmniPaxos:** By using OmniPaxos to replicate the state of the 2PC protocol, we've created a fault-tolerant distributed transaction system that can recover from node failures while maintaining consistency.

- **Crash Recovery:** Implementing proper crash recovery requires careful consideration of all possible failure scenarios and ensuring that all necessary state is persisted to durable storage.

- **Network Communication:** Using gRPC for network communication provides a reliable and efficient way to communicate between nodes, but it requires careful handling of async operations and error cases.

- **Serialization:** Efficient serialization of Arrow RecordBatches is crucial for performance, especially for large datasets. Using Arrow's IPC format provides a good balance of efficiency and compatibility.

- **Global Transaction IDs:** Generating globally unique transaction IDs that are consistent across the cluster requires careful design to avoid collisions and ensure proper ordering.

- **Standard SSI requires careful implementation of both backward (SI read) and forward (dangerous structure) validation checks.

- **Test expectations must align precisely with the chosen isolation model's guarantees (SSI vs. stricter active conflict checks vs. pure OCC).

- **Tracking transaction states and timestamps is essential for correct SSI validation.

- **Refactoring validation logic into the `DependencyTracker` improves modularity for the `Serializable` level.

- **Enforcing the constraint of containing distributed logic within a dedicated module requires a different architectural pattern, likely involving a manager struct within the distributed module.

- **Implementing the OmniPaxos storage trait with `rocksdb` requires careful consideration of `rocksdb`'s API for log-like operations and atomicity to ensure durability and support PITR.

- **Achieving PITR requires not only durable storage of the consensus log but also a mechanism to durably record the application of decided entries to the local state.

- **Implementing a real network layer with gRPC adds complexity, requiring protobuf definitions, code generation, and handling asynchronous communication.
