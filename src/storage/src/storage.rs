use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use log::info;

use crate::types::{DataType, DatabaseId, StoredValue, current_timestamp_ms};
use crate::config::StorageConfig;
use crate::wal::{Wal, WalEntry};

/// Core storage engine for Bolt.
///
/// This version supports multiple data types beyond simple strings:
/// - Strings: basic key-value pairs
/// - Counters: atomic increment/decrement operations
/// - Lists: ordered collections with push/pop from both ends
/// - Sets: unordered collections of unique members
///
/// It also includes WAL persistence, TTL expiration, batch operations,
/// and utility methods for key inspection and pattern matching.
#[derive(Clone)]
pub struct Storage {
    /// Map of databases, each containing key-value pairs
    databases: Arc<RwLock<HashMap<DatabaseId, HashMap<String, StoredValue>>>>,
    /// Write-Ahead Log for crash recovery
    wal: Arc<std::sync::Mutex<Wal>>,
    /// Counter for triggering periodic WAL compaction
    operation_count: Arc<std::sync::atomic::AtomicUsize>,
    /// Number of operations between automatic compactions (0 = disabled)
    compaction_threshold: usize,
}

impl Storage {
    /// Create a new in-memory storage (no persistence)
    pub fn new() -> Self {
        Storage {
            databases: Arc::new(RwLock::new(HashMap::new())),
            wal: Arc::new(std::sync::Mutex::new(Wal::disabled())),
            operation_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            compaction_threshold: 0,
        }
    }

    /// Create storage with configuration
    pub fn with_config(config: StorageConfig) -> std::io::Result<Self> {
        let wal = if let Some(ref path) = config.wal_path {
            Wal::new(path)?
        } else {
            Wal::disabled()
        };

        let mut storage = Storage {
            databases: Arc::new(RwLock::new(HashMap::new())),
            wal: Arc::new(std::sync::Mutex::new(wal)),
            operation_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            compaction_threshold: config.compaction_threshold,
        };

        if config.wal_path.is_some() {
            storage.recover_from_wal_sync()?;
        }

        Ok(storage)
    }

    /// Recover state by replaying WAL entries.
    ///
    /// Reads all entries from the WAL file and rebuilds the in-memory
    /// database state. Set entries are applied in order, and Delete
    /// entries remove previously set keys.
    fn recover_from_wal_sync(&mut self) -> std::io::Result<()> {
        let wal = self.wal.lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "WAL lock poisoned"))?;
        if !wal.is_enabled() {
            return Ok(());
        }

        let entries = wal.read_entries()?;
        let entry_count = entries.len();
        let mut new_databases = HashMap::new();

        for entry in entries {
            match entry {
                WalEntry::Set { database_id, key, value, ttl_ms } => {
                    let db_id = DatabaseId::from_wal_string(&database_id);
                    let db = new_databases.entry(db_id).or_insert_with(HashMap::new);
                    let stored = if let Some(ttl) = ttl_ms {
                        StoredValue::with_ttl(value, Duration::from_millis(ttl))
                    } else {
                        StoredValue::new(value)
                    };
                    db.insert(key, stored);
                }
                WalEntry::Delete { database_id, key } => {
                    let db_id = DatabaseId::from_wal_string(&database_id);
                    if let Some(db) = new_databases.get_mut(&db_id) {
                        db.remove(&key);
                    }
                }
            }
        }

        drop(wal);
        self.databases = Arc::new(RwLock::new(new_databases));

        if entry_count > 0 {
            info!("Recovered {} WAL entries", entry_count);
        }

        Ok(())
    }

    /// Get all non-expired entries from a database as string key-value pairs.
    pub async fn get_all_entries(&self, db_id: DatabaseId) -> Option<HashMap<String, String>> {
        let databases = self.databases.read().await;
        databases.get(&db_id).map(|db| {
            db.iter()
                .filter(|(_, v)| !v.is_expired())
                .filter_map(|(k, v)| v.value().map(|val| (k.clone(), val)))
                .collect()
        })
    }

    /// Set a key-value pair (no TTL).
    pub async fn set(&self, db_id: DatabaseId, key: &str, value: &str) {
        self.set_with_ttl(db_id, key, value, None).await;
    }

    /// Set a key-value pair with optional TTL.
    ///
    /// The write is logged to the WAL before being applied in memory.
    pub async fn set_with_ttl(&self, db_id: DatabaseId, key: &str, value: &str, ttl: Option<Duration>) {
        // Log to WAL first (write-ahead)
        if let Ok(mut wal) = self.wal.lock() {
            let ttl_ms = ttl.map(|d| d.as_millis() as u64);
            let _ = wal.log_set(&db_id.to_wal_string(), key, value, ttl_ms);
        }

        let stored = if let Some(ttl_duration) = ttl {
            StoredValue::with_ttl(value.to_string(), ttl_duration)
        } else {
            StoredValue::new(value.to_string())
        };

        let mut databases = self.databases.write().await;
        let db = databases.entry(db_id).or_insert_with(HashMap::new);
        db.insert(key.to_string(), stored);

        self.maybe_compact().await;
    }

    /// Get a value by key.
    ///
    /// Returns `None` if the key doesn't exist or has expired.
    pub async fn get(&self, db_id: DatabaseId, key: &str) -> Option<String> {
        let databases = self.databases.read().await;
        let stored = databases.get(&db_id)?.get(key)?;
        if stored.is_expired() {
            None
        } else {
            stored.value()
        }
    }

    /// Get the remaining TTL for a key.
    ///
    /// Returns:
    /// - `None` if the key doesn't exist or is expired
    /// - `Some(None)` if the key exists but has no TTL
    /// - `Some(Some(seconds))` if the key has a TTL
    pub async fn get_ttl(&self, db_id: DatabaseId, key: &str) -> Option<Option<u64>> {
        let databases = self.databases.read().await;
        let stored = databases.get(&db_id)?.get(key)?;
        if stored.is_expired() {
            None
        } else {
            Some(stored.ttl_seconds())
        }
    }

    /// Remove a key from a database, returning the old value.
    pub async fn remove(&self, db_id: DatabaseId, key: &str) -> Option<String> {
        if let Ok(mut wal) = self.wal.lock() {
            let _ = wal.log_delete(&db_id.to_wal_string(), key);
        }
        let mut databases = self.databases.write().await;
        if let Some(db) = databases.get_mut(&db_id) {
            if let Some(old_value) = db.remove(key) {
                old_value.value()
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get multiple values at once (batch read).
    pub async fn mget(&self, db_id: DatabaseId, keys: &[String]) -> Vec<Option<String>> {
        let databases = self.databases.read().await;
        databases.get(&db_id).map(|db| {
            keys.iter()
                .map(|key| {
                    db.get(key).and_then(|sv| {
                        if sv.is_expired() { None } else { sv.value() }
                    })
                })
                .collect()
        }).unwrap_or_else(|| vec![None; keys.len()])
    }

    /// Set multiple key-value pairs at once (batch write).
    pub async fn mset(&self, db_id: DatabaseId, pairs: &[(String, String)]) {
        if let Ok(mut wal) = self.wal.lock() {
            for (key, value) in pairs {
                let _ = wal.log_set(&db_id.to_wal_string(), key, value, None);
            }
        }
        let mut databases = self.databases.write().await;
        let db = databases.entry(db_id.clone()).or_insert_with(HashMap::new);
        for (key, value) in pairs {
            db.insert(key.clone(), StoredValue::new(value.clone()));
        }
        self.maybe_compact().await;
    }

    /// Delete multiple keys at once (batch delete).
    pub async fn mdel(&self, db_id: DatabaseId, keys: &[String]) -> usize {
        if let Ok(mut wal) = self.wal.lock() {
            for key in keys {
                let _ = wal.log_delete(&db_id.to_wal_string(), key);
            }
        }
        let mut databases = self.databases.write().await;
        let mut deleted = 0;
        if let Some(db) = databases.get_mut(&db_id) {
            for key in keys {
                if db.remove(key).is_some() {
                    deleted += 1;
                }
            }
        }
        self.maybe_compact().await;
        deleted
    }

    /// Clean up expired keys across all databases.
    pub async fn cleanup_expired(&self) -> usize {
        let mut databases = self.databases.write().await;
        let mut removed = 0;
        for db in databases.values_mut() {
            let expired_keys: Vec<String> = db.iter()
                .filter(|(_, v)| v.is_expired())
                .map(|(k, _)| k.clone())
                .collect();
            for key in expired_keys {
                db.remove(&key);
                removed += 1;
            }
        }
        if removed > 0 {
            info!("Cleaned up {} expired keys", removed);
        }
        removed
    }

    /// Count all non-expired keys across all databases.
    pub async fn keys_count(&self) -> usize {
        let databases = self.databases.read().await;
        databases.values()
            .flat_map(|db| db.values())
            .filter(|v| !v.is_expired())
            .count()
    }

    /// Count the number of databases.
    pub async fn databases_count(&self) -> usize {
        self.databases.read().await.len()
    }

    /// Conditionally trigger WAL compaction.
    async fn maybe_compact(&self) {
        if self.compaction_threshold == 0 { return; }
        let count = self.operation_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if count > 0 && count % self.compaction_threshold == 0 {
            self.compact().await;
        }
    }

    /// Compact the WAL by rewriting it with only current state.
    pub async fn compact(&self) {
        let snapshot: Vec<(String, String, String, Option<u64>)> = {
            let databases = self.databases.read().await;
            databases.iter().flat_map(|(db_id, db)| {
                let db_id_str = db_id.to_wal_string();
                db.iter()
                    .filter(|(_, v)| !v.is_expired())
                    .filter_map(move |(k, v)| {
                        v.value().map(|val| {
                            (db_id_str.clone(), k.clone(), val, v.ttl_seconds().map(|s| s * 1000))
                        })
                    })
            }).collect()
        };
        if let Ok(mut wal) = self.wal.lock() {
            let _ = wal.compact(&snapshot);
        }
    }

    /// Check if storage is using persistent WAL.
    pub async fn is_persistent(&self) -> bool {
        self.wal.lock().map(|w| w.is_enabled()).unwrap_or(false)
    }

    // ---------------------------------------------------------------
    // Counter Operations
    // ---------------------------------------------------------------

    /// Increment a counter by 1.
    ///
    /// If the key doesn't exist, it is created with value 1.
    /// If the key holds a string that can be parsed as i64, it is
    /// converted to a counter and incremented.
    pub async fn incr(&self, db_id: DatabaseId, key: &str) -> i64 {
        self.incr_by(db_id, key, 1).await
    }

    /// Decrement a counter by 1.
    pub async fn decr(&self, db_id: DatabaseId, key: &str) -> i64 {
        self.incr_by(db_id, key, -1).await
    }

    /// Increment (or decrement) a counter by an arbitrary delta.
    ///
    /// Returns the new value after the operation.
    pub async fn incr_by(&self, db_id: DatabaseId, key: &str, delta: i64) -> i64 {
        let mut databases = self.databases.write().await;
        let db = databases.entry(db_id.clone()).or_insert_with(HashMap::new);

        let new_value = if let Some(stored) = db.get_mut(key) {
            if stored.is_expired() {
                // Expired key: reset to counter with delta
                stored.data = DataType::Counter(delta);
                stored.expires_at = None;
                delta
            } else {
                match &mut stored.data {
                    DataType::Counter(n) => {
                        *n += delta;
                        *n
                    }
                    DataType::String(s) => {
                        // Try to parse string as number
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
            }
        } else {
            // New key: create counter with delta
            db.insert(key.to_string(), StoredValue::new_counter(delta));
            delta
        };

        drop(databases);

        // Log the resulting value to WAL
        if let Ok(mut wal) = self.wal.lock() {
            let _ = wal.log_set(&db_id.to_wal_string(), key, &new_value.to_string(), None);
        }

        new_value
    }

    // ---------------------------------------------------------------
    // List Operations
    // ---------------------------------------------------------------

    /// Push one or more values to the head (left) of a list.
    ///
    /// If the key doesn't exist, a new list is created.
    /// If the key holds a non-list type, returns 0.
    /// Returns the length of the list after the push.
    pub async fn lpush(&self, db_id: DatabaseId, key: &str, values: &[String]) -> usize {
        let mut databases = self.databases.write().await;
        let db = databases.entry(db_id).or_insert_with(HashMap::new);

        if let Some(stored) = db.get_mut(key) {
            if stored.is_expired() {
                stored.data = DataType::List(VecDeque::new());
                stored.expires_at = None;
            }
            if let DataType::List(list) = &mut stored.data {
                for v in values.iter().rev() {
                    list.push_front(v.clone());
                }
                list.len()
            } else {
                0
            }
        } else {
            let mut list = VecDeque::new();
            for v in values.iter().rev() {
                list.push_front(v.clone());
            }
            let len = list.len();
            db.insert(
                key.to_string(),
                StoredValue {
                    data: DataType::List(list),
                    expires_at: None,
                    timestamp: current_timestamp_ms(),
                },
            );
            len
        }
    }

    /// Push one or more values to the tail (right) of a list.
    ///
    /// If the key doesn't exist, a new list is created.
    /// Returns the length of the list after the push.
    pub async fn rpush(&self, db_id: DatabaseId, key: &str, values: &[String]) -> usize {
        let mut databases = self.databases.write().await;
        let db = databases.entry(db_id).or_insert_with(HashMap::new);

        if let Some(stored) = db.get_mut(key) {
            if stored.is_expired() {
                stored.data = DataType::List(VecDeque::new());
                stored.expires_at = None;
            }
            if let DataType::List(list) = &mut stored.data {
                for v in values {
                    list.push_back(v.clone());
                }
                list.len()
            } else {
                0
            }
        } else {
            let mut list = VecDeque::new();
            for v in values {
                list.push_back(v.clone());
            }
            let len = list.len();
            db.insert(
                key.to_string(),
                StoredValue {
                    data: DataType::List(list),
                    expires_at: None,
                    timestamp: current_timestamp_ms(),
                },
            );
            len
        }
    }

    /// Pop a value from the head (left) of a list.
    pub async fn lpop(&self, db_id: DatabaseId, key: &str) -> Option<String> {
        let mut databases = self.databases.write().await;
        let db = databases.get_mut(&db_id)?;
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

    /// Pop a value from the tail (right) of a list.
    pub async fn rpop(&self, db_id: DatabaseId, key: &str) -> Option<String> {
        let mut databases = self.databases.write().await;
        let db = databases.get_mut(&db_id)?;
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

    /// Get a range of elements from a list.
    ///
    /// Supports negative indices: -1 is the last element, -2 the
    /// second to last, etc. Both start and stop are inclusive.
    pub async fn lrange(&self, db_id: DatabaseId, key: &str, start: i64, stop: i64) -> Vec<String> {
        let databases = self.databases.read().await;
        if let Some(db) = databases.get(&db_id) {
            if let Some(stored) = db.get(key) {
                if stored.is_expired() {
                    return Vec::new();
                }
                if let DataType::List(list) = &stored.data {
                    let len = list.len() as i64;
                    if len == 0 {
                        return Vec::new();
                    }

                    // Normalize negative indices
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

                    return list.iter()
                        .skip(start_idx)
                        .take(stop_idx - start_idx)
                        .cloned()
                        .collect();
                }
            }
        }
        Vec::new()
    }

    /// Get the length of a list.
    ///
    /// Returns 0 if the key doesn't exist, is expired, or is not a list.
    pub async fn llen(&self, db_id: DatabaseId, key: &str) -> usize {
        let databases = self.databases.read().await;
        databases.get(&db_id)
            .and_then(|db| db.get(key))
            .map(|stored| {
                if stored.is_expired() {
                    0
                } else if let DataType::List(list) = &stored.data {
                    list.len()
                } else {
                    0
                }
            })
            .unwrap_or(0)
    }

    // ---------------------------------------------------------------
    // Set Operations
    // ---------------------------------------------------------------

    /// Add one or more members to a set.
    ///
    /// If the key doesn't exist, a new set is created.
    /// Returns the number of members that were actually added
    /// (not counting members already present).
    pub async fn sadd(&self, db_id: DatabaseId, key: &str, members: &[String]) -> usize {
        let mut databases = self.databases.write().await;
        let db = databases.entry(db_id).or_insert_with(HashMap::new);

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
                count
            } else {
                0
            }
        } else {
            let mut set = HashSet::new();
            for m in members {
                set.insert(m.clone());
            }
            let count = set.len();
            db.insert(
                key.to_string(),
                StoredValue {
                    data: DataType::Set(set),
                    expires_at: None,
                    timestamp: current_timestamp_ms(),
                },
            );
            count
        }
    }

    /// Remove one or more members from a set.
    ///
    /// Returns the number of members that were actually removed.
    pub async fn srem(&self, db_id: DatabaseId, key: &str, members: &[String]) -> usize {
        let mut databases = self.databases.write().await;
        if let Some(db) = databases.get_mut(&db_id) {
            if let Some(stored) = db.get_mut(key) {
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
                    return count;
                }
            }
        }
        0
    }

    /// Get all members of a set.
    pub async fn smembers(&self, db_id: DatabaseId, key: &str) -> Vec<String> {
        let databases = self.databases.read().await;
        databases.get(&db_id)
            .and_then(|db| db.get(key))
            .map(|stored| {
                if stored.is_expired() {
                    Vec::new()
                } else if let DataType::Set(set) = &stored.data {
                    set.iter().cloned().collect()
                } else {
                    Vec::new()
                }
            })
            .unwrap_or_default()
    }

    /// Get the cardinality (number of members) of a set.
    pub async fn scard(&self, db_id: DatabaseId, key: &str) -> usize {
        let databases = self.databases.read().await;
        databases.get(&db_id)
            .and_then(|db| db.get(key))
            .map(|stored| {
                if stored.is_expired() {
                    0
                } else if let DataType::Set(set) = &stored.data {
                    set.len()
                } else {
                    0
                }
            })
            .unwrap_or(0)
    }

    /// Check if a member exists in a set.
    pub async fn sismember(&self, db_id: DatabaseId, key: &str, member: &str) -> bool {
        let databases = self.databases.read().await;
        databases.get(&db_id)
            .and_then(|db| db.get(key))
            .map(|stored| {
                if stored.is_expired() {
                    false
                } else if let DataType::Set(set) = &stored.data {
                    set.contains(member)
                } else {
                    false
                }
            })
            .unwrap_or(false)
    }

    // ---------------------------------------------------------------
    // Utility Operations
    // ---------------------------------------------------------------

    /// Check if a key exists and is not expired.
    pub async fn exists(&self, db_id: DatabaseId, key: &str) -> bool {
        let databases = self.databases.read().await;
        databases.get(&db_id)
            .and_then(|db| db.get(key))
            .map(|stored| !stored.is_expired())
            .unwrap_or(false)
    }

    /// Get the data type of a key.
    ///
    /// Returns the type name (e.g., "string", "counter", "list", "set")
    /// or `None` if the key doesn't exist or is expired.
    pub async fn key_type(&self, db_id: DatabaseId, key: &str) -> Option<&'static str> {
        let databases = self.databases.read().await;
        let stored = databases.get(&db_id)?.get(key)?;
        if stored.is_expired() {
            None
        } else {
            Some(stored.type_name())
        }
    }

    /// Find all keys matching a glob-style pattern.
    ///
    /// Supports `*` (matches any sequence) and `?` (matches any single
    /// character). The pattern is converted to a regex for matching.
    pub async fn keys(&self, db_id: DatabaseId, pattern: &str) -> Vec<String> {
        let databases = self.databases.read().await;
        if let Some(db) = databases.get(&db_id) {
            // Convert glob pattern to regex
            let regex_pattern = pattern.replace("*", ".*").replace("?", ".");
            if let Ok(re) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
                return db.iter()
                    .filter(|(_, v)| !v.is_expired())
                    .filter(|(k, _)| re.is_match(k))
                    .map(|(k, _)| k.clone())
                    .collect();
            }
        }
        Vec::new()
    }

    /// Start a background task to periodically clean up expired keys.
    pub fn start_ttl_cleanup(storage: Arc<Storage>, interval: Duration) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                storage.cleanup_expired().await;
            }
        });
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}
