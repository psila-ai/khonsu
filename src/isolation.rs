// Define the TransactionIsolation enum here as it's a core part of the public API
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Defines the isolation levels supported by the Khonsu Software Transactional Memory system.
///
/// Transaction isolation levels determine how concurrent transactions interact with each other
/// and the degree to which one transaction's operations are visible to others. Higher isolation
/// levels provide stronger guarantees about data consistency but can potentially reduce
/// concurrency and performance.
pub enum TransactionIsolation {
    /// **Read Committed:**
    ///
    /// This isolation level guarantees that any data read is committed at the moment it is read.
    /// However, if the same transaction reads the same data item multiple times, it may see
    /// different values if another transaction modifies and commits that data item between the reads.
    /// This level prevents dirty reads but allows non-repeatable reads and phantom reads.
    ///
    /// With `ReadCommitted`, values are always read directly from the in-memory cache (`TxnBuffer`)
    /// each time they are accessed within a transaction.
    ///
    /// In distributed mode, Read Committed isolation is fully supported and works correctly across nodes.
    ReadCommitted,
    /// **Repeatable Read:**
    ///
    /// This isolation level guarantees that if a transaction reads a data item, any subsequent
    /// reads of that same data item within the same transaction will return the same value.
    /// This prevents non-repeatable reads. However, it does not prevent phantom reads, where
    /// new rows might appear in a range of data read by the transaction if another transaction
    /// inserts new rows into that range.
    ///
    /// With `RepeatableRead`, the first time a data item is read within a transaction, its
    /// version is recorded in the transaction's read set. Subsequent reads of the same item
    /// within that transaction will return the value from the transaction's private view
    /// or staged changes, ensuring repeatable reads.
    ///
    /// In distributed mode, Repeatable Read has limitations as the local transaction's read set
    /// may not be properly synchronized across nodes, potentially leading to inconsistent reads
    /// in some scenarios.
    RepeatableRead,
    /// **Serializable:**
    ///
    /// This is the highest isolation level and guarantees that transactions execute in a manner
    /// that produces the same result as if they had been executed one after another serially.
    /// This prevents dirty reads, non-repeatable reads, and phantom reads.
    ///
    /// Khonsu implements Serializable isolation using Serializable Snapshot Isolation (SSI).
    /// Read access with this level behaves similarly to `RepeatableRead`, using a snapshot
    /// of the data. However, during the commit process, SSI validation checks are performed
    /// using a dependency tracker to detect serialization anomalies. If a transaction
    /// cannot be serialized with respect to other concurrent transactions, it will be aborted.
    ///
    /// In distributed mode, Serializable isolation has significant limitations. The current
    /// implementation lacks global dependency tracking across nodes, synchronized timestamps,
    /// and cross-node validation protocols. This can lead to serialization anomalies such as
    /// write skew in distributed settings. For truly serializable isolation in distributed
    /// environments, additional mechanisms would be needed.
    Serializable,
}
