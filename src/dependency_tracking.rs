use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use parking_lot::RwLock;

use crate::errors::*;

/// Represents a transaction for dependency tracking purposes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DependencyTransaction {
    pub id: u64,
    // Potentially other relevant transaction information
}

/// Represents a data item for dependency tracking purposes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataItem {
    pub key: String,
    // Potentially other relevant data item information (e.g., version)
}

/// Tracks readers and writers for a specific data item, including versions.
#[derive(Debug, Clone)]
struct ItemDependency {
    // Map from reader transaction ID to the version they read.
    readers: HashMap<u64, u64>,
    // Map from writer transaction ID to the version they wrote.
    // Using a HashMap to potentially track multiple writers for WW conflicts,
    // although the current SkipMap structure might make atomic updates tricky.
    writers: HashMap<u64, u64>,
}

impl ItemDependency {
    fn new() -> Self {
        Self {
            readers: HashMap::new(),
            writers: HashMap::new(),
        }
    }
}

/// A concurrent data structure to track transaction dependencies for serializability.
/// This structure maps data items to the transactions that have read or written them.
pub struct DependencyTracker {
    // Map from DataItem key to the transactions that have read or wrote it.
    item_dependencies: RwLock<HashMap<String, ItemDependency>>,
    // TODO: Need a way to track dependencies between transactions based on this item-level information.
    // This might involve building a transaction dependency graph during validation.
}

impl DependencyTracker {
    /// Creates a new `DependencyTracker`.
    pub fn new() -> Self {
        Self {
            item_dependencies: RwLock::new(HashMap::new()),
        }
    }

    /// Records that a transaction read a data item at a specific version.
    pub fn record_read(&self, reader_id: u64, data_item: DataItem, read_version: u64) {
        let mut item_dependencies = self.item_dependencies.write();
        let item_dep = item_dependencies
            .entry(data_item.key.clone())
            .or_insert_with(ItemDependency::new);

        // Modify the ItemDependency: add the reader and the version they read.
        item_dep.readers.insert(reader_id, read_version);

        println!(
            "Recorded read: Transaction {} read {:?} at version {}",
            reader_id, data_item, read_version
        );
    }

    /// Records that a transaction intends to write to a data item at a specific version.
    /// The write_version here is the version *being written*, which will become the new latest version.
    pub fn record_write(&self, writer_id: u64, data_item: DataItem, write_version: u64) {
        let mut item_dependencies = self.item_dependencies.write();
        let item_dep = item_dependencies
            .entry(data_item.key.clone())
            .or_insert_with(ItemDependency::new);

        // Modify the ItemDependency: add the writer and the version they are writing.
        // This allows tracking multiple potential writers for WW conflicts.
        item_dep.writers.insert(writer_id, write_version);

        println!(
            "Recorded write: Transaction {} intends to write to {:?} at version {}",
            writer_id, data_item, write_version
        );
    }

    /// Checks for serializability violations (cycles) involving the committing transaction.
    /// This method is called during the commit of a Serializable transaction.
    /// Returns `Ok(true)` if no cycle is detected, `Ok(false)` if a cycle is detected.
    /// This implementation builds a dependency graph based on read/write sets and versions
    /// and performs DFS to detect cycles.
    pub fn check_for_cycles(&self, _committing_tx_id: u64) -> Result<bool> {
        // Build the transaction dependency graph for serializability validation.
        // An edge Tx_A -> Tx_B exists if Tx_A must serialize before Tx_B.
        // This occurs if they access the same data item and at least one access is a write,
        // and their access order creates a dependency that violates serializability if not ordered correctly.

        let item_dependencies = self.item_dependencies.read();
        let mut graph: HashMap<u64, HashSet<u64>> = HashMap::new();

        // Iterate over item dependencies to build graph edges.
        for (_item_key, item_dep) in item_dependencies.iter() {
            // Consider dependencies between any two transactions (Tx_A and Tx_B) that accessed this item.
            let accessing_txns: HashSet<u64> = item_dep
                .readers
                .keys()
                .chain(item_dep.writers.keys())
                .copied()
                .collect();

            for tx_a_id in &accessing_txns {
                for tx_b_id in &accessing_txns {
                    if tx_a_id == tx_b_id {
                        continue; // No dependency on self
                    }

                    // Check for dependencies that create serialization order constraints:

                    // Case 1: Write-Read Dependency (WR)
                    // If Tx_A wrote item X, and Tx_B read item X, and Tx_B read a version of X
                    // that was written by Tx_A or an earlier transaction, then Tx_A must serialize before Tx_B.
                    // This is implicitly handled by the conflict detection logic checking read versions.
                    // However, for cycle detection, we need to represent the potential for a cycle.
                    // If Tx_A wrote X and Tx_B read X, and Tx_A commits after Tx_B started, this is a WR conflict.
                    // The dependency is Tx_A -> Tx_B.
                    if item_dep.writers.contains_key(tx_a_id) && item_dep.readers.contains_key(tx_b_id) {
                         // Add edge Tx_A -> Tx_B
                         graph.entry(*tx_a_id).or_insert_with(HashSet::new).insert(*tx_b_id);
                    }


                    // Case 2: Read-Write Dependency (RW)
                    // If Tx_A read item X, and Tx_B wrote item X, and Tx_B's write conflicts with Tx_A's read
                    // (i.e., Tx_B commits after Tx_A read X), then Tx_A must serialize before Tx_B.
                    // The dependency is Tx_A -> Tx_B.
                     if item_dep.readers.contains_key(tx_a_id) && item_dep.writers.contains_key(tx_b_id) {
                         // Add edge Tx_A -> Tx_B
                         graph.entry(*tx_a_id).or_insert_with(HashSet::new).insert(*tx_b_id);
                     }


                    // Case 3: Write-Write Dependency (WW)
                    // If Tx_A wrote item X, and Tx_B wrote item X, their relative serialization order matters.
                    // If Tx_A commits before Tx_B, then Tx_A -> Tx_B. If Tx_B commits before Tx_A, then Tx_B -> Tx_A.
                    // For cycle detection, a WW conflict on the same item between Tx_A and Tx_B implies a potential
                    // dependency in either direction depending on commit order. To detect cycles, we can add
                    // edges in both directions (Tx_A <-> Tx_B) or rely on the fact that if a cycle exists
                    // involving WW conflicts, it will be detected regardless of the arbitrary edge direction
                    // chosen for WW pairs. Let's add edges in both directions for simplicity in graph building.
                     if item_dep.writers.contains_key(tx_a_id) && item_dep.writers.contains_key(tx_b_id) {
                         // Add edge Tx_A -> Tx_B
                         graph.entry(*tx_a_id).or_insert_with(HashSet::new).insert(*tx_b_id);
                         // Add edge Tx_B -> Tx_A
                         graph.entry(*tx_b_id).or_insert_with(HashSet::new).insert(*tx_a_id);
                     }
                }
            }
        }

        // Perform DFS to detect cycles.
        // A cycle in this graph indicates a serializability violation.
        let mut visited: HashSet<u64> = HashSet::new();
        let mut recursion_stack: HashSet<u64> = HashSet::new();

        // Iterate through all transactions that are part of the graph and check for cycles.
        // We need to check all components of the graph, not just starting from the committing transaction,
        // as a cycle anywhere in the graph violates serializability.
        let all_tx_ids: HashSet<u64> = graph.keys().copied().collect();

        for tx_id in &all_tx_ids {
            if !visited.contains(tx_id) {
                if self.dfs_check_cycle(*tx_id, &graph, &mut visited, &mut recursion_stack) {
                    // Cycle detected.
                    println!("Serializable validation failed: Cycle detected in dependency graph.");
                    return Ok(false); // Serializable violation
                }
            }
        }

        // If the committing transaction is not in the graph but has dependencies recorded,
        // we should still consider it. However, the current graph building includes all
        // transactions with recorded dependencies.

        println!("Serializable validation successful: No cycle detected in dependency graph.");
        Ok(true) // No cycle detected
    }

    // Helper function for DFS-based cycle detection.
    fn dfs_check_cycle(
        &self,
        tx_id: u64,
        graph: &HashMap<u64, HashSet<u64>>,
        visited: &mut HashSet<u64>,
        recursion_stack: &mut HashSet<u64>,
    ) -> bool {
        if recursion_stack.contains(&tx_id) {
            // Found a cycle.
            return true;
        }

        if visited.contains(&tx_id) {
            // Already visited this node in the current DFS path.
            return false;
        }

        // Mark as visited and add to recursion stack.
        visited.insert(tx_id);
        recursion_stack.insert(tx_id);

        // Visit neighbors.
        if let Some(neighbors) = graph.get(&tx_id) {
            for neighbor_id in neighbors {
                if self.dfs_check_cycle(*neighbor_id, graph, visited, recursion_stack) {
                    return true; // Cycle found in a descendant.
                }
            }
        }

        // Remove from recursion stack before returning.
        recursion_stack.remove(&tx_id);
        false // No cycle found in this path.
    }

    // TODO: Add methods for removing dependencies of committed or aborted transactions.
    // This is crucial for preventing the dependency tracker from growing indefinitely.
    // This will involve iterating through item_dependencies and removing the
    // committed/aborted transaction's ID from readers and writers.
    // This also needs to be done carefully to be concurrent-safe with the SkipMap.
    // This implementation is a best effort given the limitations of crossbeam-skiplist 0.1.
    pub fn remove_transaction_dependencies(&self, tx_id: u64) {
        let mut item_dependencies = self.item_dependencies.write();
        // Iterate through all item dependencies.
        // We need to collect the keys first because we cannot modify the HashMap while iterating over it.
        let keys_to_check: Vec<String> = item_dependencies
            .keys()
            .cloned()
            .collect();

        for item_key in keys_to_check {
            if let Some(item_dep) = item_dependencies.get_mut(&item_key) {
                let mut modified = false;
                // Remove from readers.
                if item_dep.readers.remove(&tx_id).is_some() {
                    modified = true;
                }

                // Remove from writers.
                if item_dep.writers.remove(&tx_id).is_some() {
                    modified = true;
                }

                if modified {
                    if item_dep.readers.is_empty() && item_dep.writers.is_empty() {
                        // If no more dependencies for this item, remove the item from the HashMap.
                        item_dependencies.remove(&item_key);
                    }
                }
            }
        }
        println!("Removed dependencies for transaction {}", tx_id);
    }
}

// Need to add this module to src/lib.rs later.
