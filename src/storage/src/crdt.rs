//! CRDT (Conflict-free Replicated Data Types) implementations
//!
//! This module provides CRDT types for multi-master replication scenarios
//! where traditional operations like INCR/DECR can have consistency issues.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// G-Counter (Grow-only Counter)
///
/// A CRDT counter that only supports increment operations.
/// Each node maintains its own count, and the total is the sum of all nodes.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GCounter {
    /// Node ID -> count mapping
    counts: HashMap<String, u64>,
}

impl GCounter {
    pub fn new() -> Self {
        GCounter {
            counts: HashMap::new(),
        }
    }

    /// Increment the counter for a specific node
    pub fn increment(&mut self, node_id: &str) {
        *self.counts.entry(node_id.to_string()).or_insert(0) += 1;
    }

    /// Increment the counter for a specific node by a given amount
    pub fn increment_by(&mut self, node_id: &str, amount: u64) {
        *self.counts.entry(node_id.to_string()).or_insert(0) += amount;
    }

    /// Get the total count (sum of all nodes)
    pub fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    /// Get the count for a specific node
    pub fn node_count(&self, node_id: &str) -> u64 {
        *self.counts.get(node_id).unwrap_or(&0)
    }

    /// Merge with another G-Counter (take max of each node's count)
    pub fn merge(&mut self, other: &GCounter) {
        for (node_id, &count) in &other.counts {
            let current = self.counts.entry(node_id.clone()).or_insert(0);
            *current = (*current).max(count);
        }
    }

    /// Get the internal state for serialization
    pub fn state(&self) -> &HashMap<String, u64> {
        &self.counts
    }

    /// Create from a state map
    pub fn from_state(counts: HashMap<String, u64>) -> Self {
        GCounter { counts }
    }
}

/// PN-Counter (Positive-Negative Counter)
///
/// A CRDT counter that supports both increment and decrement operations.
/// It uses two G-Counters: one for increments (P) and one for decrements (N).
/// The value is P - N.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PNCounter {
    /// Positive counter (increments)
    p: GCounter,
    /// Negative counter (decrements)
    n: GCounter,
}

impl PNCounter {
    pub fn new() -> Self {
        PNCounter {
            p: GCounter::new(),
            n: GCounter::new(),
        }
    }

    /// Increment the counter for a specific node
    pub fn increment(&mut self, node_id: &str) {
        self.p.increment(node_id);
    }

    /// Increment the counter for a specific node by a given amount
    pub fn increment_by(&mut self, node_id: &str, amount: u64) {
        self.p.increment_by(node_id, amount);
    }

    /// Decrement the counter for a specific node
    pub fn decrement(&mut self, node_id: &str) {
        self.n.increment(node_id);
    }

    /// Decrement the counter for a specific node by a given amount
    pub fn decrement_by(&mut self, node_id: &str, amount: u64) {
        self.n.increment_by(node_id, amount);
    }

    /// Get the total value (P - N)
    pub fn value(&self) -> i64 {
        self.p.value() as i64 - self.n.value() as i64
    }

    /// Merge with another PN-Counter
    pub fn merge(&mut self, other: &PNCounter) {
        self.p.merge(&other.p);
        self.n.merge(&other.n);
    }

    /// Get the positive counter state
    pub fn p_state(&self) -> &HashMap<String, u64> {
        self.p.state()
    }

    /// Get the negative counter state
    pub fn n_state(&self) -> &HashMap<String, u64> {
        self.n.state()
    }

    /// Create from P and N state maps
    pub fn from_state(p_counts: HashMap<String, u64>, n_counts: HashMap<String, u64>) -> Self {
        PNCounter {
            p: GCounter::from_state(p_counts),
            n: GCounter::from_state(n_counts),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcounter_basic() {
        let mut counter = GCounter::new();
        counter.increment("node1");
        counter.increment("node1");
        counter.increment("node2");

        assert_eq!(counter.value(), 3);
        assert_eq!(counter.node_count("node1"), 2);
        assert_eq!(counter.node_count("node2"), 1);
    }

    #[test]
    fn test_gcounter_merge() {
        let mut counter1 = GCounter::new();
        counter1.increment("node1");
        counter1.increment("node1");

        let mut counter2 = GCounter::new();
        counter2.increment("node1"); // less than counter1
        counter2.increment("node2");

        counter1.merge(&counter2);

        // After merge: node1=2 (max of 2, 1), node2=1
        assert_eq!(counter1.value(), 3);
        assert_eq!(counter1.node_count("node1"), 2);
        assert_eq!(counter1.node_count("node2"), 1);
    }

    #[test]
    fn test_pncounter_basic() {
        let mut counter = PNCounter::new();
        counter.increment("node1");
        counter.increment("node1");
        counter.increment("node1");
        counter.decrement("node1");

        assert_eq!(counter.value(), 2); // 3 - 1 = 2
    }

    #[test]
    fn test_pncounter_negative() {
        let mut counter = PNCounter::new();
        counter.decrement("node1");
        counter.decrement("node1");

        assert_eq!(counter.value(), -2);
    }

    #[test]
    fn test_pncounter_merge() {
        let mut counter1 = PNCounter::new();
        counter1.increment("node1");
        counter1.increment("node1");
        counter1.decrement("node2");

        let mut counter2 = PNCounter::new();
        counter2.increment("node1"); // less than counter1
        counter2.increment("node3");
        counter2.decrement("node2"); // same as counter1

        counter1.merge(&counter2);

        // After merge:
        // P: node1=2 (max of 2, 1), node3=1
        // N: node2=1 (max of 1, 1)
        // Value = 3 - 1 = 2
        assert_eq!(counter1.value(), 2);
    }

    #[test]
    fn test_pncounter_concurrent_updates() {
        // Simulate concurrent updates from different nodes
        let mut node1_view = PNCounter::new();
        let mut node2_view = PNCounter::new();

        // Node 1 increments
        node1_view.increment("node1");
        node1_view.increment("node1");

        // Node 2 increments
        node2_view.increment("node2");
        node2_view.decrement("node2");

        // Merge both views
        node1_view.merge(&node2_view);
        node2_view.merge(&node1_view);

        // Both should converge to the same value
        assert_eq!(node1_view.value(), node2_view.value());
        assert_eq!(node1_view.value(), 2); // 2 + 1 - 1 = 2
    }
}
