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

A high-performance Software Transactional Memory (STM) library for Rust, designed for concurrent data access and manipulation using Arrow RecordBatches.

## Features

*   Lock-free internals (where possible)
*   Integration with Arrow RecordBatches
*   Support for multiple RecordBatches per transaction
*   Configurable transaction isolation levels (ReadCommitted, RepeatableRead, Serializable)
*   Pluggable conflict resolution strategies (Fail, Ignore, Replace, Append)
*   Support for atomic commit and rollback
*   Designed for distributed commit capabilities

## Usage

To use Khonsu in your Rust project, add it as a dependency in your `Cargo.toml`:

```toml
[dependencies]
khonsu = "0.1.0" # Replace with the actual version
```

## Example

Here's a small example demonstrating basic usage:

```rust
use khonsu::prelude::*;
use khonsu::storage::MockStorage; // Using MockStorage for the example
use std::sync::Arc;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

fn main() -> Result<()> {
    // 1. Set up Khonsu with a storage implementation
    let storage = Arc::new(MockStorage::new());
    let khonsu = Khonsu::new(
        storage.clone(),
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
        RecordBatch::try_new(Arc::clone(&schema), vec![id_array, value_array]).unwrap();

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
            // Handle conflict or other error, potentially roll back
            // txn.rollback(); // Rollback is automatic on drop if commit fails
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

## Development

To build and test Khonsu:

```bash
cargo build
cargo test -- --test-threads=1
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
- Complete the `TwoPhaseCommitParticipant` trait implementation for distributed commit.
- Refine memory reclamation strategies.
- Implement the full distributed commit protocol.
- Address any remaining `TODO` comments in the codebase.

## License

All rights reserved.
