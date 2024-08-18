use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use log::info;

use crate::types::{DatabaseId, DataType, StoredValue, current_timestamp_ms};

/// Number of shards (must be power of 2 for efficient hashing)
const DEFAULT_SHARD_COUNT: usize = 64;

/// A single shard containing a portion of the data
struct Shard {
    data: HashMap<DatabaseId, HashMap<String, StoredValue>>,
}

impl Shard {
    fn new() -> Self {
        Shard {
            data: HashMap::new(),
        }
    }
}

/// Sharded storage for high-concurrency access
///
/// Data is distributed across multiple shards based on the hash of (database_id, key).
/// Each shard has its own RwLock, allowing concurrent access to different keys.
pub struct ShardedStorage {
    shards: Vec<Arc<RwLock<Shard>>>,
    shard_count: usize,
}

impl ShardedStorage {
    /// Create a new sharded storage with the default number of shards (64)
    pub fn new() -> Self {
        Self::with_shard_count(DEFAULT_SHARD_COUNT)
    }

    /// Create a new sharded storage with a specific number of shards
    pub fn with_shard_count(count: usize) -> Self {
        // Ensure shard count is a power of 2
        let count = count.next_power_of_two();

        let shards = (0..count)
            .map(|_| Arc::new(RwLock::new(Shard::new())))
            .collect();

        info!("ShardedStorage initialized with {} shards", count);

        ShardedStorage {
            shards,
            shard_count: count,
        }
    }

    /// Get the shard index for a given database and key
    #[inline]
    fn shard_index(&self, db_id: &DatabaseId, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        db_id.hash(&mut hasher);
        key.hash(&mut hasher);
        (hasher.finish() as usize) & (self.shard_count - 1)
    }

    /// Get a read lock on the shard for a given key
    #[inline]
    async fn get_shard(&self, db_id: &DatabaseId, key: &str) -> tokio::sync::RwLockReadGuard<'_, Shard> {
        let idx = self.shard_index(db_id, key);
        self.shards[idx].read().await
    }

    /// Get a write lock on the shard for a given key
    #[inline]
    async fn get_shard_mut(&self, db_id: &DatabaseId, key: &str) -> tokio::sync::RwLockWriteGuard<'_, Shard> {
        let idx = self.shard_index(db_id, key);
        self.shards[idx].write().await
    }

    // ==================== Basic Operations ====================

    /// Set a key-value pair
    pub async fn set(&self, db_id: DatabaseId, key: &str, value: String) {
        self.set_with_ttl(db_id, key, value, None).await;
    }

    /// Set a key-value pair with optional TTL
    pub async fn set_with_ttl(&self, db_id: DatabaseId, key: &str, value: String, ttl: Option<Duration>) {
        let stored = if let Some(ttl_duration) = ttl {
            StoredValue::with_ttl(value, ttl_duration)
        } else {
            StoredValue::new(value)
        };

        let mut shard = self.get_shard_mut(&db_id, key).await;
        let db = shard.data.entry(db_id).or_insert_with(HashMap::new);
        db.insert(key.to_string(), stored);
    }

    /// Get a value by key
    pub async fn get(&self, db_id: &DatabaseId, key: &str) -> Option<String> {
        let shard = self.get_shard(db_id, key).await;
        let stored = shard.data.get(db_id)?.get(key)?;
        if stored.is_expired() {
            None
        } else {
            stored.value()
        }
    }

    /// Get TTL for a key
    pub async fn get_ttl(&self, db_id: &DatabaseId, key: &str) -> Option<Option<u64>> {
        let shard = self.get_shard(db_id, key).await;
        let stored = shard.data.get(db_id)?.get(key)?;
        if stored.is_expired() {
            None
        } else {
            Some(stored.ttl_seconds())
        }
    }

    /// Get timestamp for a key (for LWW comparison)
    pub async fn get_timestamp(&self, db_id: &DatabaseId, key: &str) -> Option<u64> {
        let shard = self.get_shard(db_id, key).await;
        let stored = shard.data.get(db_id)?.get(key)?;
        if stored.is_expired() {
            None
        } else {
            Some(stored.timestamp)
        }
    }

    /// Set a value only if the incoming timestamp is newer (LWW)
    pub async fn set_if_newer(
        &self,
        db_id: DatabaseId,
        key: &str,
        value: String,
        ttl: Option<Duration>,
        incoming_timestamp: u64,
    ) -> bool {
        let mut shard = self.get_shard_mut(&db_id, key).await;
        let db = shard.data.entry(db_id).or_insert_with(HashMap::new);

        // Check existing timestamp
        if let Some(existing) = db.get(key) {
            if !existing.is_expired() && incoming_timestamp <= existing.timestamp {
                // Existing value is newer or same, skip this write
                return false;
            }
        }

        // Incoming value is newer, apply it
        let stored = if let Some(ttl_duration) = ttl {
            StoredValue::with_ttl_and_timestamp(value, ttl_duration, incoming_timestamp)
        } else {
            StoredValue::new_with_timestamp(value, incoming_timestamp)
        };

        db.insert(key.to_string(), stored);
        true
    }

    /// Remove a key
    pub async fn remove(&self, db_id: &DatabaseId, key: &str) -> Option<String> {
        let mut shard = self.get_shard_mut(db_id, key).await;
        let result = shard.data.get_mut(db_id)?.remove(key);
        result.and_then(|sv| sv.value())
    }

    /// Check if a key exists
    pub async fn exists(&self, db_id: &DatabaseId, key: &str) -> bool {
        let shard = self.get_shard(db_id, key).await;
        if let Some(db) = shard.data.get(db_id) {
            if let Some(stored) = db.get(key) {
                return !stored.is_expired();
            }
        }
        false
    }

    /// Get the type of a key
    pub async fn key_type(&self, db_id: &DatabaseId, key: &str) -> Option<&'static str> {
        let shard = self.get_shard(db_id, key).await;
        let db = shard.data.get(db_id)?;
        let stored = db.get(key)?;
        if stored.is_expired() {
            None
        } else {
            Some(stored.type_name())
        }
    }

    // ==================== Counter Operations ====================

    /// Increment a counter by a specific amount
    pub async fn incr_by(&self, db_id: DatabaseId, key: &str, delta: i64) -> i64 {
        let mut shard = self.get_shard_mut(&db_id, key).await;
        let db = shard.data.entry(db_id).or_insert_with(HashMap::new);

        if let Some(stored) = db.get_mut(key) {
            if stored.is_expired() {
                stored.data = DataType::Counter(delta);
                stored.expires_at = None;
                return delta;
            }

            match &mut stored.data {
                DataType::Counter(n) => {
                    *n += delta;
                    *n
                }
                DataType::String(s) => {
                    if let Ok(n) = s.parse::<i64>() {
                        let new_val = n + delta;
                        stored.data = DataType::Counter(new_val);
                        new_val
                    } else {
                        0
                    }
                }
                _ => 0,
            }
        } else {
            db.insert(key.to_string(), StoredValue::new_counter(delta));
            delta
        }
    }

    /// Increment by 1
    pub async fn incr(&self, db_id: DatabaseId, key: &str) -> i64 {
        self.incr_by(db_id, key, 1).await
    }

    /// Decrement by 1
    pub async fn decr(&self, db_id: DatabaseId, key: &str) -> i64 {
        self.incr_by(db_id, key, -1).await
    }

    // ==================== CRDT Counter Operations ====================

    /// Increment a CRDT counter by a specific amount for the given node
    pub async fn crdt_incr_by(&self, db_id: DatabaseId, key: &str, node_id: &str, amount: u64) -> i64 {
        let mut shard = self.get_shard_mut(&db_id, key).await;
        let db = shard.data.entry(db_id).or_insert_with(HashMap::new);

        if let Some(stored) = db.get_mut(key) {
            if stored.is_expired() {
                let mut counter = crate::crdt::PNCounter::new();
                counter.increment_by(node_id, amount);
                stored.data = DataType::CRDTCounter(counter.clone());
                stored.expires_at = None;
                stored.timestamp = current_timestamp_ms();
                counter.value()
            } else if let DataType::CRDTCounter(counter) = &mut stored.data {
                counter.increment_by(node_id, amount);
                stored.timestamp = current_timestamp_ms();
                counter.value()
            } else {
                0
            }
        } else {
            let mut counter = crate::crdt::PNCounter::new();
            counter.increment_by(node_id, amount);
            let value = counter.value();
            db.insert(key.to_string(), StoredValue {
                data: DataType::CRDTCounter(counter),
                expires_at: None,
                timestamp: current_timestamp_ms(),
            });
            value
        }
    }

    /// Decrement a CRDT counter by a specific amount for the given node
    pub async fn crdt_decr_by(&self, db_id: DatabaseId, key: &str, node_id: &str, amount: u64) -> i64 {
        let mut shard = self.get_shard_mut(&db_id, key).await;
        let db = shard.data.entry(db_id).or_insert_with(HashMap::new);

        if let Some(stored) = db.get_mut(key) {
            if stored.is_expired() {
                let mut counter = crate::crdt::PNCounter::new();
                counter.decrement_by(node_id, amount);
                stored.data = DataType::CRDTCounter(counter.clone());
                stored.expires_at = None;
                stored.timestamp = current_timestamp_ms();
                counter.value()
            } else if let DataType::CRDTCounter(counter) = &mut stored.data {
                counter.decrement_by(node_id, amount);
                stored.timestamp = current_timestamp_ms();
                counter.value()
            } else {
                0
            }
        } else {
            let mut counter = crate::crdt::PNCounter::new();
            counter.decrement_by(node_id, amount);
            let value = counter.value();
            db.insert(key.to_string(), StoredValue {
                data: DataType::CRDTCounter(counter),
                expires_at: None,
                timestamp: current_timestamp_ms(),
            });
            value
        }
    }

    /// Get the value of a CRDT counter
    pub async fn crdt_get(&self, db_id: &DatabaseId, key: &str) -> Option<i64> {
        let shard = self.get_shard(db_id, key).await;
        let stored = shard.data.get(db_id)?.get(key)?;
        if stored.is_expired() {
            None
        } else if let DataType::CRDTCounter(counter) = &stored.data {
            Some(counter.value())
        } else {
            None
        }
    }

    /// Get the full CRDT counter state (for replication)
    pub async fn crdt_get_state(&self, db_id: &DatabaseId, key: &str) -> Option<crate::crdt::PNCounter> {
        let shard = self.get_shard(db_id, key).await;
        let stored = shard.data.get(db_id)?.get(key)?;
        if stored.is_expired() {
            None
        } else if let DataType::CRDTCounter(counter) = &stored.data {
            Some(counter.clone())
        } else {
            None
        }
    }

    /// Merge a CRDT counter state from a remote node
    pub async fn crdt_merge(&self, db_id: DatabaseId, key: &str, remote_counter: &crate::crdt::PNCounter) {
        let mut shard = self.get_shard_mut(&db_id, key).await;
        let db = shard.data.entry(db_id).or_insert_with(HashMap::new);

        if let Some(stored) = db.get_mut(key) {
            if stored.is_expired() {
                stored.data = DataType::CRDTCounter(remote_counter.clone());
                stored.expires_at = None;
                stored.timestamp = current_timestamp_ms();
            } else if let DataType::CRDTCounter(counter) = &mut stored.data {
                counter.merge(remote_counter);
                stored.timestamp = current_timestamp_ms();
            }
        } else {
            db.insert(key.to_string(), StoredValue {
                data: DataType::CRDTCounter(remote_counter.clone()),
                expires_at: None,
                timestamp: current_timestamp_ms(),
            });
        }
    }

    // ==================== List Operations ====================

    /// Push to the left (head) of a list
    pub async fn lpush(&self, db_id: DatabaseId, key: &str, values: &[String]) -> usize {
        let mut shard = self.get_shard_mut(&db_id, key).await;
        let db = shard.data.entry(db_id).or_insert_with(HashMap::new);

        if let Some(stored) = db.get_mut(key) {
            if stored.is_expired() {
                stored.data = DataType::List(VecDeque::new());
                stored.expires_at = None;
            }
            if let DataType::List(list) = &mut stored.data {
                for v in values.iter().rev() {
                    list.push_front(v.clone());
                }
                return list.len();
            }
            0
        } else {
            let mut list = VecDeque::new();
            for v in values.iter().rev() {
                list.push_front(v.clone());
            }
            let len = list.len();
            db.insert(key.to_string(), StoredValue {
                data: DataType::List(list),
                expires_at: None,
                timestamp: current_timestamp_ms(),
            });
            len
        }
    }

    /// Push to the right (tail) of a list
    pub async fn rpush(&self, db_id: DatabaseId, key: &str, values: &[String]) -> usize {
        let mut shard = self.get_shard_mut(&db_id, key).await;
        let db = shard.data.entry(db_id).or_insert_with(HashMap::new);

        if let Some(stored) = db.get_mut(key) {
            if stored.is_expired() {
                stored.data = DataType::List(VecDeque::new());
                stored.expires_at = None;
            }
            if let DataType::List(list) = &mut stored.data {
                for v in values {
                    list.push_back(v.clone());
                }
                return list.len();
            }
            0
        } else {
            let mut list = VecDeque::new();
            for v in values {
                list.push_back(v.clone());
            }
            let len = list.len();
            db.insert(key.to_string(), StoredValue {
                data: DataType::List(list),
                expires_at: None,
                timestamp: current_timestamp_ms(),
            });
            len
        }
    }

    /// Pop from the left (head) of a list
    pub async fn lpop(&self, db_id: &DatabaseId, key: &str) -> Option<String> {
        let mut shard = self.get_shard_mut(db_id, key).await;
        let db = shard.data.get_mut(db_id)?;
        let stored = db.get_mut(key)?;

        if stored.is_expired() {
            db.remove(key);
            return None;
        }

        if let DataType::List(list) = &mut stored.data {
            list.pop_front()
        } else {
            None
        }
    }

    /// Pop from the right (tail) of a list
    pub async fn rpop(&self, db_id: &DatabaseId, key: &str) -> Option<String> {
        let mut shard = self.get_shard_mut(db_id, key).await;
        let db = shard.data.get_mut(db_id)?;
        let stored = db.get_mut(key)?;

        if stored.is_expired() {
            db.remove(key);
            return None;
        }

        if let DataType::List(list) = &mut stored.data {
            list.pop_back()
        } else {
            None
        }
    }

    /// Get a range of elements from a list
    pub async fn lrange(&self, db_id: &DatabaseId, key: &str, start: i64, stop: i64) -> Vec<String> {
        let shard = self.get_shard(db_id, key).await;
        let db = match shard.data.get(db_id) {
            Some(db) => db,
            None => return Vec::new(),
        };
        let stored = match db.get(key) {
            Some(s) => s,
            None => return Vec::new(),
        };

        if stored.is_expired() {
            return Vec::new();
        }

        if let DataType::List(list) = &stored.data {
            let len = list.len() as i64;
            if len == 0 {
                return Vec::new();
            }

            let start_idx = if start < 0 {
                (len + start).max(0) as usize
            } else {
                start.min(len) as usize
            };

            let stop_idx = if stop < 0 {
                (len + stop + 1).max(0) as usize
            } else {
                (stop + 1).min(len) as usize
            };

            if start_idx >= stop_idx {
                return Vec::new();
            }

            list.iter()
                .skip(start_idx)
                .take(stop_idx - start_idx)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get the length of a list
    pub async fn llen(&self, db_id: &DatabaseId, key: &str) -> usize {
        let shard = self.get_shard(db_id, key).await;
        let db = match shard.data.get(db_id) {
            Some(db) => db,
            None => return 0,
        };
        let stored = match db.get(key) {
            Some(s) => s,
            None => return 0,
        };

        if stored.is_expired() {
            return 0;
        }

        if let DataType::List(list) = &stored.data {
            list.len()
        } else {
            0
        }
    }

    // ==================== Set Operations ====================

    /// Add elements to a set
    pub async fn sadd(&self, db_id: DatabaseId, key: &str, members: &[String]) -> usize {
        let mut shard = self.get_shard_mut(&db_id, key).await;
        let db = shard.data.entry(db_id).or_insert_with(HashMap::new);

        if let Some(stored) = db.get_mut(key) {
            if stored.is_expired() {
                stored.data = DataType::Set(HashSet::new());
                stored.expires_at = None;
            }
            if let DataType::Set(set) = &mut stored.data {
                let mut count = 0;
                for m in members {
                    if set.insert(m.clone()) {
                        count += 1;
                    }
                }
                return count;
            }
            0
        } else {
            let mut set = HashSet::new();
            for m in members {
                set.insert(m.clone());
            }
            let count = set.len();
            db.insert(key.to_string(), StoredValue {
                data: DataType::Set(set),
                expires_at: None,
                timestamp: current_timestamp_ms(),
            });
            count
        }
    }

    /// Remove elements from a set
    pub async fn srem(&self, db_id: &DatabaseId, key: &str, members: &[String]) -> usize {
        let mut shard = self.get_shard_mut(db_id, key).await;
        let db = match shard.data.get_mut(db_id) {
            Some(db) => db,
            None => return 0,
        };
        let stored = match db.get_mut(key) {
            Some(s) => s,
            None => return 0,
        };

        if stored.is_expired() {
            db.remove(key);
            return 0;
        }

        if let DataType::Set(set) = &mut stored.data {
            let mut count = 0;
            for m in members {
                if set.remove(m) {
                    count += 1;
                }
            }
            count
        } else {
            0
        }
    }

    /// Get all members of a set
    pub async fn smembers(&self, db_id: &DatabaseId, key: &str) -> Vec<String> {
        let shard = self.get_shard(db_id, key).await;
        let db = match shard.data.get(db_id) {
            Some(db) => db,
            None => return Vec::new(),
        };
        let stored = match db.get(key) {
            Some(s) => s,
            None => return Vec::new(),
        };

        if stored.is_expired() {
            return Vec::new();
        }

        if let DataType::Set(set) = &stored.data {
            set.iter().cloned().collect()
        } else {
            Vec::new()
        }
    }

    /// Get the cardinality of a set
    pub async fn scard(&self, db_id: &DatabaseId, key: &str) -> usize {
        let shard = self.get_shard(db_id, key).await;
        let db = match shard.data.get(db_id) {
            Some(db) => db,
            None => return 0,
        };
        let stored = match db.get(key) {
            Some(s) => s,
            None => return 0,
        };

        if stored.is_expired() {
            return 0;
        }

        if let DataType::Set(set) = &stored.data {
            set.len()
        } else {
            0
        }
    }

    /// Check if a member exists in a set
    pub async fn sismember(&self, db_id: &DatabaseId, key: &str, member: &str) -> bool {
        let shard = self.get_shard(db_id, key).await;
        let db = match shard.data.get(db_id) {
            Some(db) => db,
            None => return false,
        };
        let stored = match db.get(key) {
            Some(s) => s,
            None => return false,
        };

        if stored.is_expired() {
            return false;
        }

        if let DataType::Set(set) = &stored.data {
            set.contains(member)
        } else {
            false
        }
    }

    // ==================== Batch Operations ====================

    /// Batch GET - optimized for multiple keys
    pub async fn mget(&self, db_id: &DatabaseId, keys: &[String]) -> Vec<Option<String>> {
        // Group keys by shard for efficient access
        let mut results = vec![None; keys.len()];

        // For each shard, collect keys that belong to it
        let mut shard_keys: Vec<Vec<(usize, &String)>> = vec![Vec::new(); self.shard_count];

        for (idx, key) in keys.iter().enumerate() {
            let shard_idx = self.shard_index(db_id, key);
            shard_keys[shard_idx].push((idx, key));
        }

        // Process each shard
        for (shard_idx, key_list) in shard_keys.iter().enumerate() {
            if key_list.is_empty() {
                continue;
            }

            let shard = self.shards[shard_idx].read().await;
            if let Some(db) = shard.data.get(db_id) {
                for (result_idx, key) in key_list {
                    if let Some(stored) = db.get(*key) {
                        if !stored.is_expired() {
                            results[*result_idx] = stored.value();
                        }
                    }
                }
            }
        }

        results
    }

    /// Batch SET - optimized for multiple key-value pairs
    pub async fn mset(&self, db_id: DatabaseId, pairs: &[(String, String)]) {
        // Group pairs by shard
        let mut shard_pairs: Vec<Vec<&(String, String)>> = vec![Vec::new(); self.shard_count];

        for pair in pairs {
            let shard_idx = self.shard_index(&db_id, &pair.0);
            shard_pairs[shard_idx].push(pair);
        }

        // Process each shard
        for (shard_idx, pair_list) in shard_pairs.iter().enumerate() {
            if pair_list.is_empty() {
                continue;
            }

            let mut shard = self.shards[shard_idx].write().await;
            let db = shard.data.entry(db_id.clone()).or_insert_with(HashMap::new);

            for (key, value) in pair_list {
                db.insert(key.clone(), StoredValue::new(value.clone()));
            }
        }
    }

    /// Batch SET with LWW - optimized for replication
    /// Returns the number of keys that were actually updated (newer timestamp)
    pub async fn mset_if_newer(
        &self,
        db_id: DatabaseId,
        pairs: &[(String, String)],
        incoming_timestamp: u64,
    ) -> usize {
        // Group pairs by shard
        let mut shard_pairs: Vec<Vec<&(String, String)>> = vec![Vec::new(); self.shard_count];

        for pair in pairs {
            let shard_idx = self.shard_index(&db_id, &pair.0);
            shard_pairs[shard_idx].push(pair);
        }

        let mut updated = 0;

        // Process each shard - single lock acquisition per shard
        for (shard_idx, pair_list) in shard_pairs.iter().enumerate() {
            if pair_list.is_empty() {
                continue;
            }

            let mut shard = self.shards[shard_idx].write().await;
            let db = shard.data.entry(db_id.clone()).or_insert_with(HashMap::new);

            for (key, value) in pair_list {
                // Check existing timestamp
                let should_update = if let Some(existing) = db.get(key.as_str()) {
                    existing.is_expired() || incoming_timestamp > existing.timestamp
                } else {
                    true
                };

                if should_update {
                    let stored = StoredValue::new_with_timestamp(
                        value.clone(),
                        incoming_timestamp,
                    );
                    db.insert(key.clone(), stored);
                    updated += 1;
                }
            }
        }

        updated
    }

    /// Batch DELETE - optimized for multiple keys
    pub async fn mdel(&self, db_id: &DatabaseId, keys: &[String]) -> usize {
        // Group keys by shard
        let mut shard_keys: Vec<Vec<&String>> = vec![Vec::new(); self.shard_count];

        for key in keys {
            let shard_idx = self.shard_index(db_id, key);
            shard_keys[shard_idx].push(key);
        }

        let mut deleted = 0;

        // Process each shard
        for (shard_idx, key_list) in shard_keys.iter().enumerate() {
            if key_list.is_empty() {
                continue;
            }

            let mut shard = self.shards[shard_idx].write().await;
            if let Some(db) = shard.data.get_mut(db_id) {
                for key in key_list {
                    if db.remove(*key).is_some() {
                        deleted += 1;
                    }
                }
            }
        }

        deleted
    }

    // ==================== Utility Operations ====================

    /// Get keys matching a pattern (requires scanning all shards)
    pub async fn keys(&self, db_id: &DatabaseId, pattern: &str) -> Vec<String> {
        let regex_pattern = pattern
            .replace("*", ".*")
            .replace("?", ".");

        let re = match regex::Regex::new(&format!("^{}$", regex_pattern)) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        let mut results = Vec::new();

        // Scan all shards
        for shard in &self.shards {
            let shard = shard.read().await;
            if let Some(db) = shard.data.get(db_id) {
                for (key, value) in db.iter() {
                    if !value.is_expired() && re.is_match(key) {
                        results.push(key.clone());
                    }
                }
            }
        }

        results
    }

    /// Get total key count across all shards
    pub async fn keys_count(&self) -> usize {
        let mut count = 0;

        for shard in &self.shards {
            let shard = shard.read().await;
            for db in shard.data.values() {
                count += db.values().filter(|v| !v.is_expired()).count();
            }
        }

        count
    }

    /// Get database count
    pub async fn databases_count(&self) -> usize {
        let mut dbs = HashSet::new();

        for shard in &self.shards {
            let shard = shard.read().await;
            for db_id in shard.data.keys() {
                dbs.insert(db_id.to_wal_string());
            }
        }

        dbs.len()
    }

    /// Clean up expired keys across all shards
    pub async fn cleanup_expired(&self) -> usize {
        let mut removed = 0;

        for shard in &self.shards {
            let mut shard = shard.write().await;
            for db in shard.data.values_mut() {
                let expired_keys: Vec<String> = db.iter()
                    .filter(|(_, v)| v.is_expired())
                    .map(|(k, _)| k.clone())
                    .collect();

                for key in expired_keys {
                    db.remove(&key);
                    removed += 1;
                }
            }
        }

        if removed > 0 {
            info!("Cleaned up {} expired keys", removed);
        }

        removed
    }

    /// Get all entries from a database (for WAL snapshot)
    pub async fn get_all_entries(&self, db_id: &DatabaseId) -> HashMap<String, String> {
        let mut entries = HashMap::new();

        for shard in &self.shards {
            let shard = shard.read().await;
            if let Some(db) = shard.data.get(db_id) {
                for (key, value) in db.iter() {
                    if !value.is_expired() {
                        if let Some(val) = value.value() {
                            entries.insert(key.clone(), val);
                        }
                    }
                }
            }
        }

        entries
    }

    /// Get a snapshot of all data for WAL compaction
    pub async fn snapshot(&self) -> Vec<(String, String, String, Option<u64>)> {
        let mut snapshot = Vec::new();

        for shard in &self.shards {
            let shard = shard.read().await;
            for (db_id, db) in shard.data.iter() {
                let db_id_str = db_id.to_wal_string();
                for (key, value) in db.iter() {
                    if !value.is_expired() {
                        if let Some(val) = value.value() {
                            let ttl_ms = value.ttl_seconds().map(|s| s * 1000);
                            snapshot.push((db_id_str.clone(), key.clone(), val, ttl_ms));
                        }
                    }
                }
            }
        }

        snapshot
    }

    /// Import data from WAL entries (used during recovery)
    pub async fn import_entry(&self, db_id: DatabaseId, key: String, value: StoredValue) {
        let mut shard = self.get_shard_mut(&db_id, &key).await;
        let db = shard.data.entry(db_id).or_insert_with(HashMap::new);
        db.insert(key, value);
    }

    /// Remove entry during WAL recovery
    pub async fn remove_entry(&self, db_id: &DatabaseId, key: &str) {
        let mut shard = self.get_shard_mut(db_id, key).await;
        if let Some(db) = shard.data.get_mut(db_id) {
            db.remove(key);
        }
    }

    /// Start background TTL cleanup task
    pub fn start_ttl_cleanup(storage: Arc<ShardedStorage>, interval: Duration) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                storage.cleanup_expired().await;
            }
        });
    }

    /// Get a random key from the database (for eviction)
    pub async fn get_random_key(&self, db_id: &DatabaseId) -> Option<String> {
        // Try each shard until we find a key
        for shard in &self.shards {
            let shard = shard.read().await;
            if let Some(db) = shard.data.get(db_id) {
                if let Some(key) = db.keys().next() {
                    return Some(key.clone());
                }
            }
        }
        None
    }

    /// Get the key with the nearest expiry time (for volatile-ttl eviction)
    pub async fn get_nearest_expiry_key(&self, db_id: &DatabaseId) -> Option<String> {
        let mut nearest: Option<(String, std::time::Instant)> = None;

        for shard in &self.shards {
            let shard = shard.read().await;
            if let Some(db) = shard.data.get(db_id) {
                for (key, value) in db.iter() {
                    if let Some(expires_at) = value.expires_at {
                        if !value.is_expired() {
                            match &nearest {
                                None => {
                                    nearest = Some((key.clone(), expires_at));
                                }
                                Some((_, current_nearest)) if expires_at < *current_nearest => {
                                    nearest = Some((key.clone(), expires_at));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        nearest.map(|(k, _)| k)
    }
}

impl Default for ShardedStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sharded_set_get() {
        let storage = ShardedStorage::new();

        storage.set(DatabaseId::Default, "key1", "value1".to_string()).await;
        storage.set(DatabaseId::Default, "key2", "value2".to_string()).await;

        assert_eq!(storage.get(&DatabaseId::Default, "key1").await, Some("value1".to_string()));
        assert_eq!(storage.get(&DatabaseId::Default, "key2").await, Some("value2".to_string()));
        assert_eq!(storage.get(&DatabaseId::Default, "key3").await, None);
    }

    #[tokio::test]
    async fn test_sharded_mget_mset() {
        let storage = ShardedStorage::new();

        let pairs = vec![
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
            ("k3".to_string(), "v3".to_string()),
        ];

        storage.mset(DatabaseId::Default, &pairs).await;

        let keys = vec!["k1".to_string(), "k2".to_string(), "k3".to_string(), "k4".to_string()];
        let results = storage.mget(&DatabaseId::Default, &keys).await;

        assert_eq!(results[0], Some("v1".to_string()));
        assert_eq!(results[1], Some("v2".to_string()));
        assert_eq!(results[2], Some("v3".to_string()));
        assert_eq!(results[3], None);
    }

    #[tokio::test]
    async fn test_sharded_counter() {
        let storage = ShardedStorage::new();

        assert_eq!(storage.incr(DatabaseId::Default, "counter").await, 1);
        assert_eq!(storage.incr(DatabaseId::Default, "counter").await, 2);
        assert_eq!(storage.incr_by(DatabaseId::Default, "counter", 10).await, 12);
        assert_eq!(storage.decr(DatabaseId::Default, "counter").await, 11);
    }

    #[tokio::test]
    async fn test_sharded_list() {
        let storage = ShardedStorage::new();

        storage.rpush(DatabaseId::Default, "list", &["a".to_string(), "b".to_string()]).await;
        storage.lpush(DatabaseId::Default, "list", &["z".to_string()]).await;

        let items = storage.lrange(&DatabaseId::Default, "list", 0, -1).await;
        assert_eq!(items, vec!["z", "a", "b"]);

        assert_eq!(storage.llen(&DatabaseId::Default, "list").await, 3);
    }

    #[tokio::test]
    async fn test_sharded_set() {
        let storage = ShardedStorage::new();

        storage.sadd(DatabaseId::Default, "set", &["a".to_string(), "b".to_string(), "a".to_string()]).await;

        assert_eq!(storage.scard(&DatabaseId::Default, "set").await, 2);
        assert!(storage.sismember(&DatabaseId::Default, "set", "a").await);
        assert!(!storage.sismember(&DatabaseId::Default, "set", "c").await);
    }
}
