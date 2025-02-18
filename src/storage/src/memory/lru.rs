use std::collections::HashMap;

/// LRU (Least Recently Used) tracker with O(1) operations
/// Uses a HashMap + doubly linked list pattern
pub struct LRUTracker {
    /// Maps key to node index
    map: HashMap<String, usize>,
    /// Node storage (arena allocation)
    nodes: Vec<LRUNode>,
    /// Index of most recently used node
    head: Option<usize>,
    /// Index of least recently used node (eviction candidate)
    tail: Option<usize>,
    /// Free list for recycling removed nodes
    free_list: Vec<usize>,
}

struct LRUNode {
    key: String,
    prev: Option<usize>,
    next: Option<usize>,
    /// Whether this node is active (not in free list)
    active: bool,
}

impl LRUTracker {
    /// Create a new LRU tracker
    pub fn new() -> Self {
        LRUTracker {
            map: HashMap::new(),
            nodes: Vec::new(),
            head: None,
            tail: None,
            free_list: Vec::new(),
        }
    }

    /// Create with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        LRUTracker {
            map: HashMap::with_capacity(capacity),
            nodes: Vec::with_capacity(capacity),
            head: None,
            tail: None,
            free_list: Vec::new(),
        }
    }

    /// Number of tracked keys
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Touch a key (mark as recently used) - O(1)
    /// If key doesn't exist, it's added
    pub fn touch(&mut self, key: &str) {
        if let Some(&idx) = self.map.get(key) {
            // Key exists - move to head
            self.move_to_head(idx);
        } else {
            // Key doesn't exist - insert at head
            self.insert(key.to_string());
        }
    }

    /// Get the least recently used key - O(1)
    pub fn get_lru(&self) -> Option<&str> {
        self.tail.map(|idx| self.nodes[idx].key.as_str())
    }

    /// Remove a key from tracking - O(1)
    pub fn remove(&mut self, key: &str) -> bool {
        if let Some(idx) = self.map.remove(key) {
            self.unlink(idx);
            self.nodes[idx].active = false;
            self.nodes[idx].key.clear(); // Free memory
            self.free_list.push(idx);
            true
        } else {
            false
        }
    }

    /// Pop the least recently used key - O(1)
    pub fn pop_lru(&mut self) -> Option<String> {
        let tail_idx = self.tail?;
        let key = self.nodes[tail_idx].key.clone();
        self.remove(&key);
        Some(key)
    }

    /// Check if a key is being tracked
    pub fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    /// Insert a new key at the head
    fn insert(&mut self, key: String) {
        let idx = self.allocate_node(key.clone());

        // Link at head
        self.nodes[idx].prev = None;
        self.nodes[idx].next = self.head;

        if let Some(old_head) = self.head {
            self.nodes[old_head].prev = Some(idx);
        }

        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }

        self.map.insert(key, idx);
    }

    /// Allocate a node (reuse from free list or create new)
    fn allocate_node(&mut self, key: String) -> usize {
        if let Some(idx) = self.free_list.pop() {
            self.nodes[idx] = LRUNode {
                key,
                prev: None,
                next: None,
                active: true,
            };
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(LRUNode {
                key,
                prev: None,
                next: None,
                active: true,
            });
            idx
        }
    }

    /// Move a node to the head (most recently used)
    fn move_to_head(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return; // Already at head
        }

        // Unlink from current position
        self.unlink(idx);

        // Link at head
        self.nodes[idx].prev = None;
        self.nodes[idx].next = self.head;

        if let Some(old_head) = self.head {
            self.nodes[old_head].prev = Some(idx);
        }

        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    /// Unlink a node from the list (but don't remove from map)
    fn unlink(&mut self, idx: usize) {
        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;

        // Update neighbors
        if let Some(prev_idx) = prev {
            self.nodes[prev_idx].next = next;
        } else {
            // Was head
            self.head = next;
        }

        if let Some(next_idx) = next {
            self.nodes[next_idx].prev = prev;
        } else {
            // Was tail
            self.tail = prev;
        }

        self.nodes[idx].prev = None;
        self.nodes[idx].next = None;
    }

    /// Get all keys in LRU order (most recent first) - for debugging
    #[cfg(test)]
    pub fn keys_in_order(&self) -> Vec<String> {
        let mut result = Vec::new();
        let mut current = self.head;

        while let Some(idx) = current {
            result.push(self.nodes[idx].key.clone());
            current = self.nodes[idx].next;
        }

        result
    }
}

impl Default for LRUTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_basic() {
        let mut lru = LRUTracker::new();

        // Insert A, B, C
        lru.touch("A");
        lru.touch("B");
        lru.touch("C");

        assert_eq!(lru.len(), 3);
        assert_eq!(lru.keys_in_order(), vec!["C", "B", "A"]);
        assert_eq!(lru.get_lru(), Some("A"));
    }

    #[test]
    fn test_lru_touch_existing() {
        let mut lru = LRUTracker::new();

        lru.touch("A");
        lru.touch("B");
        lru.touch("C");

        // Touch A - should move to head
        lru.touch("A");

        assert_eq!(lru.keys_in_order(), vec!["A", "C", "B"]);
        assert_eq!(lru.get_lru(), Some("B"));
    }

    #[test]
    fn test_lru_remove() {
        let mut lru = LRUTracker::new();

        lru.touch("A");
        lru.touch("B");
        lru.touch("C");

        lru.remove("B");

        assert_eq!(lru.len(), 2);
        assert_eq!(lru.keys_in_order(), vec!["C", "A"]);
    }

    #[test]
    fn test_lru_pop() {
        let mut lru = LRUTracker::new();

        lru.touch("A");
        lru.touch("B");
        lru.touch("C");

        assert_eq!(lru.pop_lru(), Some("A".to_string()));
        assert_eq!(lru.pop_lru(), Some("B".to_string()));
        assert_eq!(lru.pop_lru(), Some("C".to_string()));
        assert_eq!(lru.pop_lru(), None);
    }

    #[test]
    fn test_lru_node_reuse() {
        let mut lru = LRUTracker::new();

        // Add and remove to build up free list
        for i in 0..100 {
            lru.touch(&format!("key{}", i));
        }
        for i in 0..50 {
            lru.remove(&format!("key{}", i));
        }

        // Nodes should be reused
        let nodes_before = lru.nodes.len();

        for i in 100..150 {
            lru.touch(&format!("key{}", i));
        }

        // Should have reused nodes from free list
        assert!(lru.nodes.len() <= nodes_before + 10);
    }
}
