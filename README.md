<h1 align="center">
    <img src="https://github.com/Jet-Engine/khonsu/raw/master/art/khonsu.jpg" width="300" height="300"/>
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
*   Designed for future distributed commit capabilities

## Usage

To use Khonsu in your Rust project, add it as a dependency in your `Cargo.toml`:

```toml
[dependencies]
khonsu = "0.1.0" # Replace with the actual version
arrow = { version = "55.0.0", features = ["prettyprint"] } # Example Arrow dependency
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

## License

All rights reserved.
