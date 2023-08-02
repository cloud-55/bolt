use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use log::info;

use crate::types::{DatabaseId, StoredValue, current_timestamp_ms};
use crate::config::StorageConfig;
use crate::wal::{Wal, WalEntry};

/// Core storage engine for Bolt.
///
/// This version adds Write-Ahead Log (WAL) support for crash recovery,
/// TTL (time-to-live) support for automatic key expiration, batch
/// operations (mget/mset/mdel), and WAL compaction.
///
/// The WAL ensures durability: every write is first logged to disk
/// before being applied in memory. On restart, the WAL is replayed
/// to recover the last consistent state.
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
    /// Create a new in-memory storage (no persistence).
    ///
    /// The WAL is disabled so no data will survive process restarts.
    /// Use `with_config` to enable persistence.
    pub fn new() -> Self {
        info!("Initializing in-memory storage (no persistence)");
        Storage {
            databases: Arc::new(RwLock::new(HashMap::new())),
            wal: Arc::new(std::sync::Mutex::new(Wal::disabled())),
            operation_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            compaction_threshold: 0,
        }
    }

    /// Create storage with configuration.
    ///
    /// If `config.wal_path` is set, enables WAL-based persistence at that
    /// path. On startup, replays any existing WAL entries to recover state.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be created or read.
    pub fn with_config(config: StorageConfig) -> std::io::Result<Self> {
        let wal = if let Some(ref path) = config.wal_path {
            info!("Enabling WAL persistence at: {}", path);
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

        // Recover state from WAL if persistence is enabled
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

    /// Get a value by key from a database.
    ///
    /// Returns `None` if the key doesn't exist, the database doesn't
    /// exist, or the value has expired.
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
    /// - `Some(None)` if the key exists but has no TTL (persistent)
    /// - `Some(Some(seconds))` if the key has a TTL with remaining seconds
    pub async fn get_ttl(&self, db_id: DatabaseId, key: &str) -> Option<Option<u64>> {
        let databases = self.databases.read().await;
        let stored = databases.get(&db_id)?.get(key)?;
        if stored.is_expired() {
            None
        } else {
            Some(stored.ttl_seconds())
        }
    }

    /// Set a key-value pair in a database (no TTL).
    ///
    /// This is a convenience wrapper around `set_with_ttl` with `None` TTL.
    pub async fn set(&self, db_id: DatabaseId, key: &str, value: &str) {
        self.set_with_ttl(db_id, key, value, None).await;
    }

    /// Set a key-value pair with an optional TTL.
    ///
    /// The write is logged to the WAL before being applied in memory.
    /// If a TTL is provided, the key will automatically expire after
    /// the specified duration.
    ///
    /// # Arguments
    ///
    /// * `db_id` - The database to write to
    /// * `key` - The key to set
    /// * `value` - The value to store
    /// * `ttl` - Optional time-to-live duration
    pub async fn set_with_ttl(&self, db_id: DatabaseId, key: &str, value: &str, ttl: Option<Duration>) {
        // Log to WAL first (write-ahead)
        if let Ok(mut wal) = self.wal.lock() {
            let ttl_ms = ttl.map(|d| d.as_millis() as u64);
            let _ = wal.log_set(&db_id.to_wal_string(), key, value, ttl_ms);
        }

        // Apply in memory
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

    /// Remove a key from a database.
    ///
    /// The deletion is logged to the WAL before being applied.
    /// Returns the old value if the key existed.
    pub async fn remove(&self, db_id: DatabaseId, key: &str) -> Option<String> {
        // Log to WAL first
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
    ///
    /// Returns a Vec with the same length as `keys`. Each element is
    /// `Some(value)` if the key exists and is not expired, or `None`
    /// otherwise.
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
    ///
    /// All pairs are logged to the WAL in a single lock acquisition,
    /// then applied to the in-memory database. None of the pairs have
    /// a TTL set.
    pub async fn mset(&self, db_id: DatabaseId, pairs: &[(String, String)]) {
        // Log all pairs to WAL
        if let Ok(mut wal) = self.wal.lock() {
            for (key, value) in pairs {
                let _ = wal.log_set(&db_id.to_wal_string(), key, value, None);
            }
        }

        // Apply all pairs in memory
        let mut databases = self.databases.write().await;
        let db = databases.entry(db_id.clone()).or_insert_with(HashMap::new);
        for (key, value) in pairs {
            db.insert(key.clone(), StoredValue::new(value.clone()));
        }

        self.maybe_compact().await;
    }

    /// Delete multiple keys at once (batch delete).
    ///
    /// Returns the number of keys that were actually removed
    /// (keys that didn't exist are not counted).
    pub async fn mdel(&self, db_id: DatabaseId, keys: &[String]) -> usize {
        // Log all deletes to WAL
        if let Ok(mut wal) = self.wal.lock() {
            for key in keys {
                let _ = wal.log_delete(&db_id.to_wal_string(), key);
            }
        }

        // Apply deletions in memory
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

    /// Get all non-expired entries from a database.
    ///
    /// Returns a HashMap of key -> value pairs. Expired entries are
    /// filtered out. Returns `None` if the database doesn't exist.
    pub async fn get_all_entries(&self, db_id: DatabaseId) -> Option<HashMap<String, String>> {
        let databases = self.databases.read().await;
        databases.get(&db_id).map(|db| {
            db.iter()
                .filter(|(_, v)| !v.is_expired())
                .filter_map(|(k, v)| v.value().map(|val| (k.clone(), val)))
                .collect()
        })
    }

    /// Clean up expired keys across all databases.
    ///
    /// Scans all databases and removes any keys whose TTL has elapsed.
    /// Returns the number of keys removed. This is called periodically
    /// by `start_ttl_cleanup`.
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

    /// Check if a key exists and is not expired.
    pub async fn exists(&self, db_id: DatabaseId, key: &str) -> bool {
        let databases = self.databases.read().await;
        databases
            .get(&db_id)
            .and_then(|db| db.get(key))
            .map(|stored| !stored.is_expired())
            .unwrap_or(false)
    }

    /// Conditionally trigger WAL compaction based on operation count.
    ///
    /// If `compaction_threshold` is non-zero, compaction is triggered
    /// every N operations where N is the threshold.
    async fn maybe_compact(&self) {
        if self.compaction_threshold == 0 {
            return;
        }
        let count = self.operation_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if count > 0 && count % self.compaction_threshold == 0 {
            self.compact().await;
        }
    }

    /// Compact the WAL by rewriting it with only current state.
    ///
    /// Takes a snapshot of all non-expired key-value pairs and rewrites
    /// the WAL file with just the current state. This reduces the WAL
    /// file size by removing obsolete set/delete entries.
    pub async fn compact(&self) {
        let snapshot: Vec<(String, String, String, Option<u64>)> = {
            let databases = self.databases.read().await;
            databases.iter().flat_map(|(db_id, db)| {
                let db_id_str = db_id.to_wal_string();
                db.iter()
                    .filter(|(_, v)| !v.is_expired())
                    .filter_map(move |(k, v)| {
                        v.value().map(|val| {
                            (
                                db_id_str.clone(),
                                k.clone(),
                                val,
                                v.ttl_seconds().map(|s| s * 1000),
                            )
                        })
                    })
            }).collect()
        };

        if let Ok(mut wal) = self.wal.lock() {
            let _ = wal.compact(&snapshot);
        }
    }

    /// Check if storage is using persistent WAL.
    ///
    /// Returns true if the WAL is enabled and writes are being
    /// persisted to disk.
    pub async fn is_persistent(&self) -> bool {
        self.wal.lock().map(|w| w.is_enabled()).unwrap_or(false)
    }

    /// Start a background task to periodically clean up expired keys.
    ///
    /// The cleanup task runs on the tokio runtime and calls
    /// `cleanup_expired` at the specified interval.
    ///
    /// # Arguments
    ///
    /// * `storage` - Arc-wrapped storage instance
    /// * `interval` - How often to run the cleanup
    pub fn start_ttl_cleanup(storage: Arc<Storage>, interval: Duration) {
        info!("Starting TTL cleanup task (interval: {:?})", interval);
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
