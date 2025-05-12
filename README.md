<h1 align="center">
    <img src="art/khonsu.jpg" width="400" height="400"/>
</h1>
<div align="center">
 <strong>
   Khonsu: Software Transactional Memory for Rust
 </strong>
<hr>
</div>

<div align="center">
</div>

A high-performance Software Transactional Memory (STM) library for Rust, designed for concurrent data access and manipulation using Arrow RecordBatches. Khonsu supports both standalone and distributed operation modes.

## Features

*   Lock-free internals (where possible)
*   Integration with Arrow RecordBatches
*   Support for multiple RecordBatches per transaction
*   Configurable transaction isolation levels (ReadCommitted, RepeatableRead, Serializable)
*   Pluggable conflict resolution strategies (Fail, Ignore, Replace, Append)
*   Support for atomic commit and rollback
*   Distributed transaction capabilities:
    * Two-Phase Commit (2PC) protocol for distributed consensus
    * Multi-Paxos based replication for fault tolerance
    * Crash recovery with automatic state reconstruction
    * Consistent hashing for transaction distribution

## Installation

To use Khonsu in your Rust project, add it as a dependency in your `Cargo.toml`:

```toml
# Basic usage (without distributed features)
[dependencies]
khonsu = "0.1.0" # Replace with the actual version

# With distributed features enabled
[dependencies]
khonsu = { version = "0.1.0", features = ["distributed"] }
```

## Basic Example (Non-Distributed)

Here's a small example demonstrating basic usage without distributed features:

```rust
use khonsu::prelude::*;
use std::sync::Arc;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

// Define a simple in-memory storage implementation
#[derive(Default)]
struct MockStorage;

impl Storage for MockStorage {
    fn apply_mutations(&self, _mutations: Vec<StorageMutation>) -> Result<()> {
        // In a real implementation, this would write to durable storage
        Ok(())
    }
}

fn main() -> Result<()> {
    // 1. Set up Khonsu with a storage implementation
    let storage = Arc::new(MockStorage::default());
    let khonsu = Khonsu::new(
        storage,
        TransactionIsolation::Serializable, // Choose your desired isolation level
        ConflictResolution::Fail,           // Choose your desired conflict resolution
    );

    // Define a simple schema and record batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let id_array = Arc::new(StringArray::from(vec!["key1"]));
    let value_array = Arc::new(Int64Array::from(vec![100]));
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![id_array, value_array]).unwrap();

    // 2. Start a transaction
    let mut txn = khonsu.start_transaction();

    // 3. Perform operations within the transaction
    txn.write("my_data_key".to_string(), record_batch.clone())?;
    println!("Wrote data in transaction {}", txn.id());

    // 4. Attempt to commit the transaction
    match txn.commit() {
        Ok(()) => println!("Transaction {} committed successfully.", txn.id()),
        Err(e) => {
            eprintln!("Transaction {} failed to commit: {:?}", txn.id(), e);
            // Handle conflict or other error
            // Note: Rollback is automatic on drop if commit fails
        }
    }

    // 5. Read the data in a new transaction
    let mut read_txn = khonsu.start_transaction();
    if let Some(read_batch) = read_txn.read(&"my_data_key".to_string())? {
        println!("Read data in transaction {}: {:?}", read_txn.id(), read_batch);
        assert_eq!(*read_batch, record_batch); // Verify read data matches original
    } else {
        println!("Data not found in transaction {}", read_txn.id());
    }

    Ok(())
}
```

## Distributed Example

When the `distributed` feature is enabled, Khonsu can operate in a distributed mode across multiple nodes. Here's how to set up and use Khonsu in a distributed environment:

```rust
use khonsu::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use omnipaxos::ClusterConfig;

// Define a simple storage implementation
#[derive(Default)]
struct MockStorage;

impl Storage for MockStorage {
    fn apply_mutations(&self, _mutations: Vec<StorageMutation>) -> Result<()> {
        // In a real implementation, this would write to durable storage
        Ok(())
    }
}

fn main() -> Result<()> {
    // 1. Set up a distributed Khonsu node
    
    // Define the node's identity and cluster configuration
    let node_id = 1; // This node's ID
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3], // A 3-node cluster
        flexible_quorum: None,
    };
    
    // Define peer addresses for gRPC communication
    let mut peer_addrs = HashMap::new();
    peer_addrs.insert(2, "127.0.0.1:50052".to_string());
    peer_addrs.insert(3, "127.0.0.1:50053".to_string());
    
    // Create a storage path for the distributed commit log
    let storage_path = PathBuf::from("/tmp/khonsu-node1");
    
    // Create the distributed configuration
    let dist_config = KhonsuDistConfig {
        node_id,
        cluster_config,
        peer_addrs,
        storage_path,
    };
    
    // Create the Khonsu instance with distributed capabilities
    let storage = Arc::new(MockStorage::default());
    let khonsu = Khonsu::new(
        storage,
        TransactionIsolation::ReadCommitted, // Only ReadCommitted is fully supported in distributed mode
        ConflictResolution::Fail,
        Some(dist_config), // Pass the distributed configuration
    );
    
    // 2. Create a record batch to store
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let id_array = Arc::new(StringArray::from(vec!["key1"]));
    let value_array = Arc::new(Int64Array::from(vec![100]));
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![id_array, value_array]).unwrap();
    
    // 3. Start a distributed transaction
    let mut txn = khonsu.start_transaction();
    
    // 4. Perform operations within the transaction
    txn.write("distributed_key".to_string(), record_batch.clone())?;
    println!("Wrote data in distributed transaction {}", txn.id());
    
    // 5. Commit the transaction - this will be replicated to all nodes in the cluster
    match txn.commit() {
        Ok(()) => println!("Transaction {} committed and replicated to all nodes", txn.id()),
        Err(e) => {
            eprintln!("Transaction {} failed: {:?}", txn.id(), e);
            // Handle distributed commit error
        }
    }
    
    // 6. Read the data in a new transaction
    let mut read_txn = khonsu.start_transaction();
    if let Some(read_batch) = read_txn.read(&"distributed_key".to_string())? {
        println!("Read data from distributed storage: {:?}", read_batch);
    } else {
        println!("Data not found in distributed storage");
    }
    
    Ok(())
}
```

## Distributed Architecture

When operating in distributed mode, Khonsu uses a combination of techniques to ensure data consistency across nodes:

1. **Two-Phase Commit (2PC)**: Ensures atomicity of transactions across multiple nodes
2. **Multi-Paxos Consensus**: Provides fault tolerance and consistency for the distributed commit log
3. **RocksDB Storage**: Persists the commit log for crash recovery
4. **gRPC Communication**: Enables network communication between nodes

### Isolation Levels in Distributed Mode

While Khonsu supports multiple isolation levels, there are some considerations for distributed operation:

- **Read Committed**: Fully supported in distributed mode
- **Repeatable Read**: Supported with limitations in distributed settings
- **Serializable**: Supported with limitations in distributed settings

### Crash Recovery

Khonsu's distributed mode is designed to be crash-resistant. When a node crashes and restarts:

1. It recovers its state from the persistent storage
2. It rejoins the cluster and catches up on missed transactions
3. It participates in new distributed transactions

## Development

To build and test Khonsu:

```bash
# Build with default features (no distributed capabilities)
cargo build

# Build with distributed features
cargo build --features distributed

# Run tests (single-threaded to avoid test interference)
cargo test -- --test-threads=1

# Run distributed tests
cargo test --features distributed -- --test-threads=1
```

## References of Khonsu Implementation
Below you can find the research papers and various serializable checking mechanisms that is implemented in Khonsu.
```bibtex
@inproceedings{bailis2014highly,
  title={Highly available transactions: Virtues and limitations},
  author={Bailis, Peter and Ghodsi, Ali and Hellerstein, Joseph M and Stoica, Ion},
  booktitle={Proceedings of the VLDB Endowment},
  volume={7},
  number={3},
  pages={245--256},
  year={2014},
  organization={VLDB Endowment}
}

@article{fekete2005serializable,
  title={Serializable isolation for snapshot databases},
  author={Fekete, Alan and Greenwood, David and Kingston, Maurice and Rice, Jeff and Storage, Andrew},
  journal={Proc. 29th VLDB Endowment},
  volume={32},
  pages={12},
  year={2005}
}

@article{herlihy2003composable,
  title={Composable memory transactions},
  author={Herlihy, Maurice and Luchangco, Victor and Moir, Mark and Scherer, William N},
  journal={ACM SIGPLAN Notices},
  volume={38},
  number={10},
  pages={80--96},
  year={2003},
  publisher={ACM}
}
```

## Future Work

- Refine the SSI implementation, including improved handling of deletions and performance optimizations.
- Implement comprehensive SSI test cases.
- Refine memory reclamation strategies.
- Enhance distributed transaction capabilities with more advanced partitioning strategies.
- Improve cross-node transaction validation for higher isolation levels.

## License

All rights reserved.
