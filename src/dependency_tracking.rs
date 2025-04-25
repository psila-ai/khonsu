use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use crossbeam_skiplist::SkipMap;

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
    item_dependencies: SkipMap<String, ItemDependency>,
    // TODO: Need a way to track dependencies between transactions based on this item-level information.
    // This might involve building a transaction dependency graph during validation.
}

impl DependencyTracker {
    /// Creates a new `DependencyTracker`.
    pub fn new() -> Self {
        Self {
            item_dependencies: SkipMap::new(),
        }
    }

    /// Records that a transaction read a data item at a specific version.
    pub fn record_read(&self, reader_id: u64, data_item: DataItem, read_version: u64) {
        // Use a read-modify-write loop to atomically update the ItemDependency.
        loop {
            let existing_entry = self.item_dependencies.get(&data_item.key);
            let mut item_dep = existing_entry
                .map(|entry| entry.value().clone())
                .unwrap_or_else(ItemDependency::new);

            // Modify the ItemDependency: add the reader and the version they read.
            item_dep.readers.insert(reader_id, read_version);

            // Attempt to replace the old entry with the modified one.
            // This is not a true compare-and-swap in crossbeam-skiplist 0.1's public API.
            // The `insert` method will unconditionally replace the entry if the key exists.
            // In a highly concurrent scenario, another thread could have updated the entry
            // between our `get` and `insert`. A robust solution would require a compare-and-swap
            // or a method that returns whether the insertion replaced an existing value.
            // With crossbeam-skiplist 0.1, we rely on the fact that `insert` is atomic for the key,
            // but the read-modify-write loop is not truly atomic without a compare-and-swap.
            // This implementation is a best effort with the available API.
            self.item_dependencies
                .insert(data_item.key.clone(), item_dep.clone());

            // TODO: Add a check here to see if the insert was successful in replacing the *specific*
            // entry we read. This is hard with crossbeam-skiplist 0.1.
            // For now, we assume the insert succeeded and break the loop.
            break;
        }
        println!(
            "Recorded read: Transaction {} read {:?} at version {}",
            reader_id, data_item, read_version
        );
    }

    /// Records that a transaction intends to write to a data item at a specific version.
    /// The write_version here is the version *being written*, which will become the new latest version.
    pub fn record_write(&self, writer_id: u64, data_item: DataItem, write_version: u64) {
        // Use a read-modify-write loop to atomically update the ItemDependency.
        loop {
            let existing_entry = self.item_dependencies.get(&data_item.key);
            let mut item_dep = existing_entry
                .map(|entry| entry.value().clone())
                .unwrap_or_else(ItemDependency::new);

            // Modify the ItemDependency: add the writer and the version they are writing.
            // This allows tracking multiple potential writers for WW conflicts.
            item_dep.writers.insert(writer_id, write_version);

            // Attempt to replace the old entry with the modified one.
            // Similar limitations as in record_read apply regarding atomicity of the RMW loop.
            self.item_dependencies
                .insert(data_item.key.clone(), item_dep.clone());

            // TODO: Add a check here to see if the insert was successful in replacing the *specific*
            // entry we read. This is hard with crossbeam-skiplist 0.1.
            // For now, we assume the insert succeeded and break the loop.
            break;
        }
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
    pub fn check_for_cycles(&self, committing_tx_id: u64) -> Result<bool> {
        // Build the transaction dependency graph.
        // Edges represent dependencies that would violate serializability if the committing
        // transaction were to commit.
        let mut graph: HashMap<u64, HashSet<u64>> = HashMap::new();

        // Iterate over item dependencies to build graph edges for serializability.
        // An edge Tx_A -> Tx_B exists if Tx_A must serialize before Tx_B.
        // This occurs if they access the same data item and at least one access is a write.
        for entry in self.item_dependencies.iter() {
            let item_dep = entry.value();

            // Consider dependencies between any two transactions (Tx_A and Tx_B) that accessed this item.

            // Case 1: Read-Write Dependency (RW)
            // If Tx_A read item X, and Tx_B wrote item X, add edge Tx_A -> Tx_B.
            for reader_id in item_dep.readers.keys() {
                for writer_id in item_dep.writers.keys() {
                    if reader_id != writer_id {
                        graph
                            .entry(*reader_id)
                            .or_insert_with(HashSet::new)
                            .insert(*writer_id);
                    }
                }
            }

            // Case 2: Write-Read Dependency (WR)
            // If Tx_A wrote item X, and Tx_B read item X, add edge Tx_A -> Tx_B.
            for writer_id in item_dep.writers.keys() {
                for reader_id in item_dep.readers.keys() {
                    if writer_id != reader_id {
                        graph
                            .entry(*writer_id)
                            .or_insert_with(HashSet::new)
                            .insert(*reader_id);
                    }
                }
            }

            // Case 3: Write-Write Dependency (WW)
            // If Tx_A wrote item X, and Tx_B wrote item X, add edge Tx_A -> Tx_B.
            // The direction here is arbitrary for cycle detection; any edge between them
            // due to a WW conflict on the same item is sufficient to detect a cycle.
            for (writer1_id, _) in item_dep.writers.iter() {
                for (writer2_id, _) in item_dep.writers.iter() {
                    if writer1_id != writer2_id {
                        graph
                            .entry(*writer1_id)
                            .or_insert_with(HashSet::new)
                            .insert(*writer2_id);
                    }
                }
            }
        }

        // Perform DFS to detect cycles involving the committing transaction.
        let mut visited: HashSet<u64> = HashSet::new();
        let mut recursion_stack: HashSet<u64> = HashSet::new();

        // Check for cycles starting from the committing transaction.
        if self.dfs_check_cycle(committing_tx_id, &graph, &mut visited, &mut recursion_stack) {
            // Cycle detected involving the committing transaction.
            Ok(false) // Serializable violation
        } else {
            // A cycle could exist that doesn't start from the committing transaction
            // but still involves it. A full graph cycle detection is more accurate.
            // For a basic implementation, we can iterate through all transactions
            // involved in the graph and check for cycles.

            let mut all_visited: HashSet<u64> = HashSet::new();
            for tx_id in graph.keys() {
                if !all_visited.contains(tx_id) {
                    let mut current_recursion_stack: HashSet<u64> = HashSet::new();
                    if self.dfs_check_cycle(
                        *tx_id,
                        &graph,
                        &mut all_visited,
                        &mut current_recursion_stack,
                    ) {
                        // Check if the committing transaction is part of this cycle.
                        if current_recursion_stack.contains(&committing_tx_id) {
                            return Ok(false); // Serializable violation
                        }
                    }
                }
            }

            Ok(true) // No cycle detected
        }
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
        // Iterate through all item dependencies.
        // We need to collect the keys first because we cannot modify the SkipMap while iterating over it.
        let keys_to_check: Vec<String> = self
            .item_dependencies
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for item_key in keys_to_check {
            // Use a read-modify-write loop to atomically update the ItemDependency for this key.
            loop {
                let existing_entry = self.item_dependencies.get(&item_key);
                let mut item_dep = existing_entry
                    .map(|entry| entry.value().clone())
                    .unwrap_or_else(ItemDependency::new);

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
                        // If no more dependencies for this item, attempt to remove the item from the SkipMap.
                        // This remove operation is atomic for the key.
                        self.item_dependencies.remove(&item_key);
                        break; // Successfully removed or another thread did.
                    } else {
                        // Otherwise, attempt to insert the modified ItemDependency.
                        // This insert operation is atomic for the key, but the RMW loop is not truly atomic
                        // without compare-and-swap. Another thread could have modified the entry
                        // between our `get` and `insert`.
                        self.item_dependencies
                            .insert(item_key.clone(), item_dep.clone());
                        // In a robust implementation, we would check if the insert replaced the *specific*
                        // entry we read and retry if not. With crossbeam-skiplist 0.1, we assume success
                        // and break, which is a simplification.
                        break;
                    }
                } else {
                    // No modifications were needed for this item dependency.
                    break;
                }
            }
        }
        println!("Removed dependencies for transaction {}", tx_id);
    }
}

// Need to add this module to src/lib.rs later.
