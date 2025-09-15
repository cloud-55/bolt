use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Mutex};
use log::{info, warn};

use crate::types::{DataType, DatabaseId, StoredValue, current_timestamp_ms};
use crate::config::StorageConfig;
use crate::backend::StorageBackend;
use crate::wal::{Wal, WalEntry};
use crate::memory::{MemoryTracker, MemoryStats, LRUTracker, EvictionPolicy, format_bytes};

#[derive(Clone)]
pub struct Storage {
    backend: StorageBackend,
    operation_count: Arc<std::sync::atomic::AtomicUsize>,
    compaction_threshold: usize,
    /// Memory tracker for enforcing limits
    memory_tracker: Arc<MemoryTracker>,
    /// LRU tracker for eviction
    lru_tracker: Arc<Mutex<LRUTracker>>,
    /// Eviction policy
    eviction_policy: EvictionPolicy,
    /// Total evictions counter
    eviction_count: Arc<std::sync::atomic::AtomicU64>,
}

impl Storage {
    /// Create a new in-memory storage (no persistence)
    pub fn new() -> Self {
        Storage {
            backend: StorageBackend::Standard {
                databases: Arc::new(RwLock::new(HashMap::new())),
                wal: Arc::new(std::sync::Mutex::new(Wal::disabled())),
            },
            operation_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            compaction_threshold: 0,
            memory_tracker: Arc::new(MemoryTracker::unlimited()),
            lru_tracker: Arc::new(Mutex::new(LRUTracker::new())),
            eviction_policy: EvictionPolicy::NoEviction,
            eviction_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Create a new high-performance in-memory storage (no persistence)
    pub fn new_high_performance() -> Self {
        Storage {
            backend: StorageBackend::HighPerformance {
                sharded: Arc::new(crate::sharded::ShardedStorage::new()),
                wal: Arc::new(crate::batched_wal::BatchedWal::disabled()),
            },
            operation_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            compaction_threshold: 0,
            memory_tracker: Arc::new(MemoryTracker::unlimited()),
            lru_tracker: Arc::new(Mutex::new(LRUTracker::new())),
            eviction_policy: EvictionPolicy::NoEviction,
            eviction_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Create storage with configuration
    pub fn with_config(config: StorageConfig) -> std::io::Result<Self> {
        if config.high_performance {
            return Self::with_config_high_performance(config);
        }

        let wal = if let Some(ref path) = config.wal_path {
            Wal::new(path)?
        } else {
            Wal::disabled()
        };

        // Initialize memory tracking
        let memory_tracker = if config.max_memory > 0 {
            Arc::new(MemoryTracker::new(config.max_memory)
                .with_warning_threshold(config.memory_warning_threshold))
        } else {
            Arc::new(MemoryTracker::unlimited())
        };

        info!(
            "Memory config: max={}, policy={}, warning_threshold={}%",
            if config.max_memory > 0 { format_bytes(config.max_memory) } else { "unlimited".to_string() },
            config.eviction_policy,
            config.memory_warning_threshold
        );

        let mut storage = Storage {
            backend: StorageBackend::Standard {
                databases: Arc::new(RwLock::new(HashMap::new())),
                wal: Arc::new(std::sync::Mutex::new(wal)),
            },
            operation_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            compaction_threshold: config.compaction_threshold,
            memory_tracker,
            lru_tracker: Arc::new(Mutex::new(LRUTracker::new())),
            eviction_policy: config.eviction_policy,
            eviction_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };

        if config.wal_path.is_some() {
            storage.recover_from_wal_sync()?;
        }

        Ok(storage)
    }

    fn with_config_high_performance(config: StorageConfig) -> std::io::Result<Self> {
        use crate::batched_wal::{BatchedWal, BatchedWalConfig};
        use crate::sharded::ShardedStorage;

        let sharded = Arc::new(ShardedStorage::with_shard_count(config.shard_count));

        let wal = if let Some(ref path) = config.wal_path {
            let temp_wal = Wal::new(path)?;
            let entries = temp_wal.read_entries()?;
            let entry_count = entries.len();

            let sharded_clone = sharded.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    for entry in entries {
                        match entry {
                            WalEntry::Set { database_id, key, value, ttl_ms } => {
                                let db_id = DatabaseId::from_wal_string(&database_id);
                                let stored = if let Some(ttl) = ttl_ms {
                                    StoredValue::with_ttl(value, Duration::from_millis(ttl))
                                } else {
                                    StoredValue::new(value)
                                };
                                sharded_clone.import_entry(db_id, key, stored).await;
                            }
                            WalEntry::Delete { database_id, key } => {
                                let db_id = DatabaseId::from_wal_string(&database_id);
                                sharded_clone.remove_entry(&db_id, &key).await;
                            }
                        }
                    }
                });
            }).join().unwrap();

            if entry_count > 0 {
                info!("Recovered {} WAL entries (high-performance mode)", entry_count);
            }

            let wal_config = BatchedWalConfig::new(path)
                .with_batch_size(config.wal_batch_size)
                .with_flush_interval(Duration::from_millis(config.wal_flush_interval_ms));

            let wal = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async { BatchedWal::new(wal_config).await })
            }).join().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Failed to create batched WAL"))??;

            Arc::new(wal)
        } else {
            Arc::new(BatchedWal::disabled())
        };

        // Initialize memory tracking
        let memory_tracker = if config.max_memory > 0 {
            Arc::new(MemoryTracker::new(config.max_memory)
                .with_warning_threshold(config.memory_warning_threshold))
        } else {
            Arc::new(MemoryTracker::unlimited())
        };

        info!("Storage initialized in high-performance mode (shards: {}, batch_size: {})",
              config.shard_count, config.wal_batch_size);
        info!(
            "Memory config: max={}, policy={}, warning_threshold={}%",
            if config.max_memory > 0 { format_bytes(config.max_memory) } else { "unlimited".to_string() },
            config.eviction_policy,
            config.memory_warning_threshold
        );

        Ok(Storage {
            backend: StorageBackend::HighPerformance { sharded, wal },
            operation_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            compaction_threshold: config.compaction_threshold,
            memory_tracker,
            lru_tracker: Arc::new(Mutex::new(LRUTracker::new())),
            eviction_policy: config.eviction_policy,
            eviction_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    fn recover_from_wal_sync(&mut self) -> std::io::Result<()> {
        if let StorageBackend::Standard { databases: _, wal } = &self.backend {
            let wal = wal.lock().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "WAL lock poisoned"))?;
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
            self.backend = StorageBackend::Standard {
                databases: Arc::new(RwLock::new(new_databases)),
                wal: Arc::clone(match &self.backend {
                    StorageBackend::Standard { wal, .. } => wal,
                    _ => unreachable!(),
                }),
            };

            if entry_count > 0 {
                info!("Recovered {} WAL entries", entry_count);
            }
        }
        Ok(())
    }

    pub fn is_high_performance(&self) -> bool {
        matches!(self.backend, StorageBackend::HighPerformance { .. })
    }

    /// Get memory statistics
    pub fn memory_stats(&self) -> MemoryStats {
        self.memory_tracker.stats()
    }

    /// Get total eviction count
    pub fn eviction_count(&self) -> u64 {
        self.eviction_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Evict keys if memory limit would be exceeded
    /// Returns true if space was made or no eviction needed
    /// Returns false if NoEviction policy and limit would be exceeded
    async fn evict_if_needed(&self, needed_bytes: usize, db_id: &DatabaseId) -> bool {
        if !self.memory_tracker.is_limited() {
            return true; // Unlimited memory
        }

        if !self.memory_tracker.would_exceed(needed_bytes) {
            return true; // Have enough space
        }

        if !self.eviction_policy.can_evict() {
            return false; // NoEviction policy
        }

        // Evict until we have space
        let mut evicted = 0;
        while self.memory_tracker.would_exceed(needed_bytes) {
            let victim_key = match self.eviction_policy {
                EvictionPolicy::AllKeysLRU | EvictionPolicy::VolatileLRU => {
                    let lru = self.lru_tracker.lock().await;
                    lru.get_lru().map(|k| k.to_string())
                }
                EvictionPolicy::AllKeysRandom => {
                    self.get_random_key(db_id).await
                }
                EvictionPolicy::VolatileTTL => {
                    self.get_nearest_expiry_key(db_id).await
                }
                EvictionPolicy::NoEviction => None,
            };

            match victim_key {
                Some(key) => {
                    // Remove the key
                    self.remove_for_eviction(db_id.clone(), &key).await;
                    evicted += 1;
                    self.eviction_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                None => {
                    // No more keys to evict
                    break;
                }
            }

            // Safety: prevent infinite loop
            if evicted > 1000 {
                warn!("Eviction loop limit reached (1000 keys evicted)");
                break;
            }
        }

        if evicted > 0 {
            info!("Evicted {} keys to free memory", evicted);
        }

        !self.memory_tracker.would_exceed(needed_bytes)
    }

    /// Remove a key during eviction (doesn't log to WAL eviction, but does log delete)
    async fn remove_for_eviction(&self, db_id: DatabaseId, key: &str) {
        match &self.backend {
            StorageBackend::Standard { databases, wal } => {
                let mut databases = databases.write().await;
                if let Some(db) = databases.get_mut(&db_id) {
                    if let Some(old_value) = db.remove(key) {
                        let old_size = MemoryTracker::estimate_stored_value_size(key, &old_value);
                        self.memory_tracker.subtract(old_size);

                        // Log deletion to WAL
                        if let Ok(mut wal) = wal.lock() {
                            let _ = wal.log_delete(&db_id.to_wal_string(), key);
                        }
                    }
                }
            }
            StorageBackend::HighPerformance { sharded, wal } => {
                if sharded.remove(&db_id, key).await.is_some() {
                    // Approximate size removal
                    self.memory_tracker.subtract(100); // Rough estimate
                    let _ = wal.log_delete(&db_id.to_wal_string(), key).await;
                }
            }
        }

        // Remove from LRU tracker
        let mut lru = self.lru_tracker.lock().await;
        lru.remove(key);
    }

    /// Get a random key from the database
    async fn get_random_key(&self, db_id: &DatabaseId) -> Option<String> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                databases.get(db_id).and_then(|db| {
                    db.keys().next().cloned() // Simple: just get first key
                })
            }
            StorageBackend::HighPerformance { sharded, .. } => {
                sharded.get_random_key(db_id).await
            }
        }
    }

    /// Get the key with nearest expiry time
    async fn get_nearest_expiry_key(&self, db_id: &DatabaseId) -> Option<String> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                databases.get(db_id).and_then(|db| {
                    db.iter()
                        .filter(|(_, v)| v.expires_at.is_some() && !v.is_expired())
                        .min_by_key(|(_, v)| v.expires_at)
                        .map(|(k, _)| k.clone())
                })
            }
            StorageBackend::HighPerformance { sharded, .. } => {
                sharded.get_nearest_expiry_key(db_id).await
            }
        }
    }

    pub async fn get_all_entries(&self, db_id: DatabaseId) -> Option<HashMap<String, String>> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                databases.get(&db_id).map(|db| {
                    db.iter()
                        .filter(|(_, v)| !v.is_expired())
                        .filter_map(|(k, v)| v.value().map(|val| (k.clone(), val)))
                        .collect()
                })
            }
            StorageBackend::HighPerformance { sharded, .. } => {
                let entries = sharded.get_all_entries(&db_id).await;
                if entries.is_empty() { None } else { Some(entries) }
            }
        }
    }

    pub async fn set(&self, db_id: DatabaseId, key: &str, value: &str) {
        self.set_with_ttl(db_id, key, value, None).await;
    }

    pub async fn set_with_ttl(&self, db_id: DatabaseId, key: &str, value: &str, ttl: Option<Duration>) {
        let entry_size = MemoryTracker::estimate_entry_size(key, value);

        // Evict if necessary before inserting
        if !self.evict_if_needed(entry_size, &db_id).await {
            // NoEviction policy and memory full - log warning but still insert
            // (to maintain backwards compatibility)
            if self.memory_tracker.is_limited() {
                warn!(
                    "Memory limit reached ({}/{}), cannot evict (policy: {})",
                    format_bytes(self.memory_tracker.current()),
                    format_bytes(self.memory_tracker.max()),
                    self.eviction_policy
                );
            }
        }

        match &self.backend {
            StorageBackend::Standard { databases, wal } => {
                if let Ok(mut wal) = wal.lock() {
                    let ttl_ms = ttl.map(|d| d.as_millis() as u64);
                    let _ = wal.log_set(&db_id.to_wal_string(), key, value, ttl_ms);
                }
                let stored = if let Some(ttl_duration) = ttl {
                    StoredValue::with_ttl(value.to_string(), ttl_duration)
                } else {
                    StoredValue::new(value.to_string())
                };
                let mut databases = databases.write().await;
                let db = databases.entry(db_id).or_insert_with(HashMap::new);

                // Check if key already exists (for memory tracking)
                let old_size = db.get(key).map(|v| MemoryTracker::estimate_stored_value_size(key, v)).unwrap_or(0);

                db.insert(key.to_string(), stored);

                // Update memory tracker
                if old_size > 0 {
                    self.memory_tracker.subtract(old_size);
                }
                self.memory_tracker.add(entry_size);
            }
            StorageBackend::HighPerformance { sharded, wal } => {
                sharded.set_with_ttl(db_id.clone(), key, value.to_string(), ttl).await;
                let ttl_ms = ttl.map(|d| d.as_millis() as u64);
                let _ = wal.log_set(&db_id.to_wal_string(), key, value, ttl_ms).await;

                // Update memory tracker (approximate for high-perf mode)
                self.memory_tracker.add(entry_size);
            }
        }

        // Update LRU tracker
        if self.eviction_policy.requires_lru() {
            let mut lru = self.lru_tracker.lock().await;
            lru.touch(key);
        }

        self.maybe_compact().await;
    }

    pub async fn get_timestamp(&self, db_id: &DatabaseId, key: &str) -> Option<u64> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                let stored = databases.get(db_id)?.get(key)?;
                if stored.is_expired() { None } else { Some(stored.timestamp) }
            }
            StorageBackend::HighPerformance { sharded, .. } => {
                sharded.get_timestamp(db_id, key).await
            }
        }
    }

    pub async fn set_if_newer(&self, db_id: DatabaseId, key: &str, value: &str, ttl: Option<Duration>, incoming_timestamp: u64) -> bool {
        if let Some(existing_ts) = self.get_timestamp(&db_id, key).await {
            if incoming_timestamp <= existing_ts { return false; }
        }
        match &self.backend {
            StorageBackend::Standard { databases, wal } => {
                if let Ok(mut wal) = wal.lock() {
                    let ttl_ms = ttl.map(|d| d.as_millis() as u64);
                    let _ = wal.log_set(&db_id.to_wal_string(), key, value, ttl_ms);
                }
                let stored = if let Some(ttl_duration) = ttl {
                    StoredValue::with_ttl_and_timestamp(value.to_string(), ttl_duration, incoming_timestamp)
                } else {
                    StoredValue::new_with_timestamp(value.to_string(), incoming_timestamp)
                };
                let mut databases = databases.write().await;
                let db = databases.entry(db_id).or_insert_with(HashMap::new);
                db.insert(key.to_string(), stored);
            }
            StorageBackend::HighPerformance { sharded, wal } => {
                sharded.set_if_newer(db_id.clone(), key, value.to_string(), ttl, incoming_timestamp).await;
                let ttl_ms = ttl.map(|d| d.as_millis() as u64);
                let _ = wal.log_set(&db_id.to_wal_string(), key, value, ttl_ms).await;
            }
        }
        self.maybe_compact().await;
        true
    }

    pub async fn delete_if_newer(&self, db_id: DatabaseId, key: &str, incoming_timestamp: u64) -> bool {
        if let Some(existing_ts) = self.get_timestamp(&db_id, key).await {
            if incoming_timestamp <= existing_ts { return false; }
        }
        self.remove(db_id, key).await;
        true
    }

    pub async fn mset_if_newer(&self, db_id: DatabaseId, pairs: &[(String, String)], incoming_timestamp: u64) -> usize {
        if pairs.is_empty() { return 0; }
        match &self.backend {
            StorageBackend::Standard { databases, wal } => {
                let mut to_update = Vec::new();
                {
                    let databases_read = databases.read().await;
                    for (key, value) in pairs {
                        let should_update = databases_read.get(&db_id)
                            .and_then(|db| db.get(key))
                            .map(|existing| existing.is_expired() || incoming_timestamp > existing.timestamp)
                            .unwrap_or(true);
                        if should_update { to_update.push((key.clone(), value.clone())); }
                    }
                }
                if to_update.is_empty() { return 0; }
                if let Ok(mut wal) = wal.lock() {
                    for (key, value) in &to_update {
                        let _ = wal.log_set(&db_id.to_wal_string(), key, value, None);
                    }
                }
                let mut updated = 0;
                {
                    let mut databases_write = databases.write().await;
                    let db = databases_write.entry(db_id).or_insert_with(HashMap::new);
                    for (key, value) in to_update {
                        db.insert(key, StoredValue::new_with_timestamp(value, incoming_timestamp));
                        updated += 1;
                    }
                }
                updated
            }
            StorageBackend::HighPerformance { sharded, wal } => {
                let updated = sharded.mset_if_newer(db_id.clone(), pairs, incoming_timestamp).await;
                if updated > 0 {
                    for (key, value) in pairs {
                        let _ = wal.log_set(&db_id.to_wal_string(), key, value, None).await;
                    }
                }
                updated
            }
        }
    }

    pub async fn get(&self, db_id: DatabaseId, key: &str) -> Option<String> {
        let result = match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                let stored = databases.get(&db_id)?.get(key)?;
                if stored.is_expired() { None } else { stored.value() }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.get(&db_id, key).await,
        };

        // Update LRU tracker on successful get
        if result.is_some() && self.eviction_policy.requires_lru() {
            let mut lru = self.lru_tracker.lock().await;
            lru.touch(key);
        }

        result
    }

    pub async fn get_ttl(&self, db_id: DatabaseId, key: &str) -> Option<Option<u64>> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                let stored = databases.get(&db_id)?.get(key)?;
                if stored.is_expired() { None } else { Some(stored.ttl_seconds()) }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.get_ttl(&db_id, key).await,
        }
    }

    pub async fn remove(&self, db_id: DatabaseId, key: &str) -> Option<String> {
        let result = match &self.backend {
            StorageBackend::Standard { databases, wal } => {
                if let Ok(mut wal) = wal.lock() { let _ = wal.log_delete(&db_id.to_wal_string(), key); }
                let mut databases = databases.write().await;
                if let Some(db) = databases.get_mut(&db_id) {
                    if let Some(old_value) = db.remove(key) {
                        // Update memory tracker
                        let old_size = MemoryTracker::estimate_stored_value_size(key, &old_value);
                        self.memory_tracker.subtract(old_size);
                        old_value.value()
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            StorageBackend::HighPerformance { sharded, wal } => {
                let result = sharded.remove(&db_id, key).await;
                let _ = wal.log_delete(&db_id.to_wal_string(), key).await;
                if result.is_some() {
                    // Approximate size removal for high-perf mode
                    self.memory_tracker.subtract(100);
                }
                result
            }
        };

        // Update LRU tracker
        if result.is_some() {
            let mut lru = self.lru_tracker.lock().await;
            lru.remove(key);
        }

        self.maybe_compact().await;
        result
    }

    pub async fn mget(&self, db_id: DatabaseId, keys: &[String]) -> Vec<Option<String>> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                databases.get(&db_id).map(|db| {
                    keys.iter().map(|key| db.get(key).and_then(|sv| if sv.is_expired() { None } else { sv.value() })).collect()
                }).unwrap_or_else(|| vec![None; keys.len()])
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.mget(&db_id, keys).await,
        }
    }

    pub async fn mset(&self, db_id: DatabaseId, pairs: &[(String, String)]) {
        match &self.backend {
            StorageBackend::Standard { databases, wal } => {
                if let Ok(mut wal) = wal.lock() {
                    for (key, value) in pairs { let _ = wal.log_set(&db_id.to_wal_string(), key, value, None); }
                }
                let mut databases = databases.write().await;
                let db = databases.entry(db_id.clone()).or_insert_with(HashMap::new);
                for (key, value) in pairs { db.insert(key.clone(), StoredValue::new(value.clone())); }
            }
            StorageBackend::HighPerformance { sharded, wal } => {
                sharded.mset(db_id.clone(), pairs).await;
                for (key, value) in pairs { let _ = wal.log_set(&db_id.to_wal_string(), key, value, None).await; }
            }
        }
        self.maybe_compact().await;
    }

    pub async fn mdel(&self, db_id: DatabaseId, keys: &[String]) -> usize {
        let deleted = match &self.backend {
            StorageBackend::Standard { databases, wal } => {
                if let Ok(mut wal) = wal.lock() {
                    for key in keys { let _ = wal.log_delete(&db_id.to_wal_string(), key); }
                }
                let mut databases = databases.write().await;
                let mut deleted = 0;
                if let Some(db) = databases.get_mut(&db_id) {
                    for key in keys { if db.remove(key).is_some() { deleted += 1; } }
                }
                deleted
            }
            StorageBackend::HighPerformance { sharded, wal } => {
                let deleted = sharded.mdel(&db_id, keys).await;
                for key in keys { let _ = wal.log_delete(&db_id.to_wal_string(), key).await; }
                deleted
            }
        };
        self.maybe_compact().await;
        deleted
    }

    pub async fn cleanup_expired(&self) -> usize {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let mut databases = databases.write().await;
                let mut removed = 0;
                for db in databases.values_mut() {
                    let expired_keys: Vec<String> = db.iter().filter(|(_, v)| v.is_expired()).map(|(k, _)| k.clone()).collect();
                    for key in expired_keys { db.remove(&key); removed += 1; }
                }
                if removed > 0 { info!("Cleaned up {} expired keys", removed); }
                removed
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.cleanup_expired().await,
        }
    }

    pub async fn keys_count(&self) -> usize {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                databases.values().flat_map(|db| db.values()).filter(|v| !v.is_expired()).count()
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.keys_count().await,
        }
    }

    pub async fn databases_count(&self) -> usize {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => databases.read().await.len(),
            StorageBackend::HighPerformance { sharded, .. } => sharded.databases_count().await,
        }
    }

    async fn maybe_compact(&self) {
        if self.compaction_threshold == 0 { return; }
        let count = self.operation_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if count > 0 && count % self.compaction_threshold == 0 { self.compact().await; }
    }

    pub async fn compact(&self) {
        match &self.backend {
            StorageBackend::Standard { databases, wal } => {
                let snapshot: Vec<(String, String, String, Option<u64>)> = {
                    let databases = databases.read().await;
                    databases.iter().flat_map(|(db_id, db)| {
                        let db_id_str = db_id.to_wal_string();
                        db.iter().filter(|(_, v)| !v.is_expired()).filter_map(move |(k, v)| {
                            v.value().map(|val| (db_id_str.clone(), k.clone(), val, v.ttl_seconds().map(|s| s * 1000)))
                        })
                    }).collect()
                };
                if let Ok(mut wal) = wal.lock() { let _ = wal.compact(&snapshot); }
            }
            StorageBackend::HighPerformance { sharded, wal } => {
                let snapshot = sharded.snapshot().await;
                let _ = wal.compact(snapshot).await;
            }
        }
    }

    pub async fn is_persistent(&self) -> bool {
        match &self.backend {
            StorageBackend::Standard { wal, .. } => wal.lock().map(|w| w.is_enabled()).unwrap_or(false),
            StorageBackend::HighPerformance { wal, .. } => wal.is_enabled(),
        }
    }

    // Counter Operations
    pub async fn incr(&self, db_id: DatabaseId, key: &str) -> i64 { self.incr_by(db_id, key, 1).await }
    pub async fn decr(&self, db_id: DatabaseId, key: &str) -> i64 { self.incr_by(db_id, key, -1).await }

    pub async fn incr_by(&self, db_id: DatabaseId, key: &str, delta: i64) -> i64 {
        match &self.backend {
            StorageBackend::Standard { databases, wal } => {
                let mut databases = databases.write().await;
                let db = databases.entry(db_id.clone()).or_insert_with(HashMap::new);
                let new_value = if let Some(stored) = db.get_mut(key) {
                    if stored.is_expired() {
                        stored.data = DataType::Counter(delta);
                        stored.expires_at = None;
                        delta
                    } else {
                        match &mut stored.data {
                            DataType::Counter(n) => { *n += delta; *n }
                            DataType::String(s) => {
                                if let Ok(n) = s.parse::<i64>() {
                                    let new_val = n + delta;
                                    stored.data = DataType::Counter(new_val);
                                    new_val
                                } else { 0 }
                            }
                            _ => 0,
                        }
                    }
                } else {
                    db.insert(key.to_string(), StoredValue::new_counter(delta));
                    delta
                };
                drop(databases);
                if let Ok(mut wal) = wal.lock() { let _ = wal.log_set(&db_id.to_wal_string(), key, &new_value.to_string(), None); }
                new_value
            }
            StorageBackend::HighPerformance { sharded, wal } => {
                let new_value = sharded.incr_by(db_id.clone(), key, delta).await;
                let _ = wal.log_set(&db_id.to_wal_string(), key, &new_value.to_string(), None).await;
                new_value
            }
        }
    }

    // CRDT Counter Operations
    pub async fn crdt_incr(&self, db_id: DatabaseId, key: &str, node_id: &str) -> i64 { self.crdt_incr_by(db_id, key, node_id, 1).await }
    pub async fn crdt_decr(&self, db_id: DatabaseId, key: &str, node_id: &str) -> i64 { self.crdt_decr_by(db_id, key, node_id, 1).await }

    pub async fn crdt_incr_by(&self, db_id: DatabaseId, key: &str, node_id: &str, amount: u64) -> i64 {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let mut databases = databases.write().await;
                let db = databases.entry(db_id).or_insert_with(HashMap::new);
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
                    } else { 0 }
                } else {
                    let mut counter = crate::crdt::PNCounter::new();
                    counter.increment_by(node_id, amount);
                    let value = counter.value();
                    db.insert(key.to_string(), StoredValue { data: DataType::CRDTCounter(counter), expires_at: None, timestamp: current_timestamp_ms() });
                    value
                }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.crdt_incr_by(db_id, key, node_id, amount).await,
        }
    }

    pub async fn crdt_decr_by(&self, db_id: DatabaseId, key: &str, node_id: &str, amount: u64) -> i64 {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let mut databases = databases.write().await;
                let db = databases.entry(db_id).or_insert_with(HashMap::new);
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
                    } else { 0 }
                } else {
                    let mut counter = crate::crdt::PNCounter::new();
                    counter.decrement_by(node_id, amount);
                    let value = counter.value();
                    db.insert(key.to_string(), StoredValue { data: DataType::CRDTCounter(counter), expires_at: None, timestamp: current_timestamp_ms() });
                    value
                }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.crdt_decr_by(db_id, key, node_id, amount).await,
        }
    }

    pub async fn crdt_get(&self, db_id: DatabaseId, key: &str) -> Option<i64> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                let stored = databases.get(&db_id)?.get(key)?;
                if stored.is_expired() { None } else if let DataType::CRDTCounter(counter) = &stored.data { Some(counter.value()) } else { None }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.crdt_get(&db_id, key).await,
        }
    }

    pub async fn crdt_merge(&self, db_id: DatabaseId, key: &str, remote_counter: &crate::crdt::PNCounter) {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let mut databases = databases.write().await;
                let db = databases.entry(db_id).or_insert_with(HashMap::new);
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
                    db.insert(key.to_string(), StoredValue { data: DataType::CRDTCounter(remote_counter.clone()), expires_at: None, timestamp: current_timestamp_ms() });
                }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.crdt_merge(db_id, key, remote_counter).await,
        }
    }

    pub async fn crdt_get_state(&self, db_id: &DatabaseId, key: &str) -> Option<crate::crdt::PNCounter> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                let stored = databases.get(db_id)?.get(key)?;
                if stored.is_expired() { None } else if let DataType::CRDTCounter(counter) = &stored.data { Some(counter.clone()) } else { None }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.crdt_get_state(db_id, key).await,
        }
    }

    // List Operations
    pub async fn lpush(&self, db_id: DatabaseId, key: &str, values: &[String]) -> usize {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let mut databases = databases.write().await;
                let db = databases.entry(db_id).or_insert_with(HashMap::new);
                if let Some(stored) = db.get_mut(key) {
                    if stored.is_expired() { stored.data = DataType::List(VecDeque::new()); stored.expires_at = None; }
                    if let DataType::List(list) = &mut stored.data {
                        for v in values.iter().rev() { list.push_front(v.clone()); }
                        list.len()
                    } else { 0 }
                } else {
                    let mut list = VecDeque::new();
                    for v in values.iter().rev() { list.push_front(v.clone()); }
                    let len = list.len();
                    db.insert(key.to_string(), StoredValue { data: DataType::List(list), expires_at: None, timestamp: current_timestamp_ms() });
                    len
                }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.lpush(db_id, key, values).await,
        }
    }

    pub async fn rpush(&self, db_id: DatabaseId, key: &str, values: &[String]) -> usize {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let mut databases = databases.write().await;
                let db = databases.entry(db_id).or_insert_with(HashMap::new);
                if let Some(stored) = db.get_mut(key) {
                    if stored.is_expired() { stored.data = DataType::List(VecDeque::new()); stored.expires_at = None; }
                    if let DataType::List(list) = &mut stored.data {
                        for v in values { list.push_back(v.clone()); }
                        list.len()
                    } else { 0 }
                } else {
                    let mut list = VecDeque::new();
                    for v in values { list.push_back(v.clone()); }
                    let len = list.len();
                    db.insert(key.to_string(), StoredValue { data: DataType::List(list), expires_at: None, timestamp: current_timestamp_ms() });
                    len
                }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.rpush(db_id, key, values).await,
        }
    }

    pub async fn lpop(&self, db_id: DatabaseId, key: &str) -> Option<String> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let mut databases = databases.write().await;
                let db = databases.get_mut(&db_id)?;
                let stored = db.get_mut(key)?;
                if stored.is_expired() { db.remove(key); return None; }
                if let DataType::List(list) = &mut stored.data { list.pop_front() } else { None }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.lpop(&db_id, key).await,
        }
    }

    pub async fn rpop(&self, db_id: DatabaseId, key: &str) -> Option<String> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let mut databases = databases.write().await;
                let db = databases.get_mut(&db_id)?;
                let stored = db.get_mut(key)?;
                if stored.is_expired() { db.remove(key); return None; }
                if let DataType::List(list) = &mut stored.data { list.pop_back() } else { None }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.rpop(&db_id, key).await,
        }
    }

    pub async fn lrange(&self, db_id: DatabaseId, key: &str, start: i64, stop: i64) -> Vec<String> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                if let Some(db) = databases.get(&db_id) {
                    if let Some(stored) = db.get(key) {
                        if stored.is_expired() { return Vec::new(); }
                        if let DataType::List(list) = &stored.data {
                            let len = list.len() as i64;
                            if len == 0 { return Vec::new(); }
                            let start_idx = if start < 0 { (len + start).max(0) as usize } else { start.min(len) as usize };
                            let stop_idx = if stop < 0 { (len + stop + 1).max(0) as usize } else { (stop + 1).min(len) as usize };
                            if start_idx >= stop_idx { return Vec::new(); }
                            return list.iter().skip(start_idx).take(stop_idx - start_idx).cloned().collect();
                        }
                    }
                }
                Vec::new()
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.lrange(&db_id, key, start, stop).await,
        }
    }

    pub async fn llen(&self, db_id: DatabaseId, key: &str) -> usize {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                databases.get(&db_id).and_then(|db| db.get(key)).map(|stored| {
                    if stored.is_expired() { 0 } else if let DataType::List(list) = &stored.data { list.len() } else { 0 }
                }).unwrap_or(0)
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.llen(&db_id, key).await,
        }
    }

    // Set Operations
    pub async fn sadd(&self, db_id: DatabaseId, key: &str, members: &[String]) -> usize {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let mut databases = databases.write().await;
                let db = databases.entry(db_id).or_insert_with(HashMap::new);
                if let Some(stored) = db.get_mut(key) {
                    if stored.is_expired() { stored.data = DataType::Set(HashSet::new()); stored.expires_at = None; }
                    if let DataType::Set(set) = &mut stored.data {
                        let mut count = 0;
                        for m in members { if set.insert(m.clone()) { count += 1; } }
                        count
                    } else { 0 }
                } else {
                    let mut set = HashSet::new();
                    for m in members { set.insert(m.clone()); }
                    let count = set.len();
                    db.insert(key.to_string(), StoredValue { data: DataType::Set(set), expires_at: None, timestamp: current_timestamp_ms() });
                    count
                }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.sadd(db_id, key, members).await,
        }
    }

    pub async fn srem(&self, db_id: DatabaseId, key: &str, members: &[String]) -> usize {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let mut databases = databases.write().await;
                if let Some(db) = databases.get_mut(&db_id) {
                    if let Some(stored) = db.get_mut(key) {
                        if stored.is_expired() { db.remove(key); return 0; }
                        if let DataType::Set(set) = &mut stored.data {
                            let mut count = 0;
                            for m in members { if set.remove(m) { count += 1; } }
                            return count;
                        }
                    }
                }
                0
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.srem(&db_id, key, members).await,
        }
    }

    pub async fn smembers(&self, db_id: DatabaseId, key: &str) -> Vec<String> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                databases.get(&db_id).and_then(|db| db.get(key)).map(|stored| {
                    if stored.is_expired() { Vec::new() } else if let DataType::Set(set) = &stored.data { set.iter().cloned().collect() } else { Vec::new() }
                }).unwrap_or_default()
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.smembers(&db_id, key).await,
        }
    }

    pub async fn scard(&self, db_id: DatabaseId, key: &str) -> usize {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                databases.get(&db_id).and_then(|db| db.get(key)).map(|stored| {
                    if stored.is_expired() { 0 } else if let DataType::Set(set) = &stored.data { set.len() } else { 0 }
                }).unwrap_or(0)
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.scard(&db_id, key).await,
        }
    }

    pub async fn sismember(&self, db_id: DatabaseId, key: &str, member: &str) -> bool {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                databases.get(&db_id).and_then(|db| db.get(key)).map(|stored| {
                    if stored.is_expired() { false } else if let DataType::Set(set) = &stored.data { set.contains(member) } else { false }
                }).unwrap_or(false)
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.sismember(&db_id, key, member).await,
        }
    }

    // Utility Operations
    pub async fn exists(&self, db_id: DatabaseId, key: &str) -> bool {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                databases.get(&db_id).and_then(|db| db.get(key)).map(|stored| !stored.is_expired()).unwrap_or(false)
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.exists(&db_id, key).await,
        }
    }

    pub async fn key_type(&self, db_id: DatabaseId, key: &str) -> Option<&'static str> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                let stored = databases.get(&db_id)?.get(key)?;
                if stored.is_expired() { None } else { Some(stored.type_name()) }
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.key_type(&db_id, key).await,
        }
    }

    pub async fn keys(&self, db_id: DatabaseId, pattern: &str) -> Vec<String> {
        match &self.backend {
            StorageBackend::Standard { databases, .. } => {
                let databases = databases.read().await;
                if let Some(db) = databases.get(&db_id) {
                    let regex_pattern = pattern.replace("*", ".*").replace("?", ".");
                    if let Ok(re) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
                        return db.iter().filter(|(_, v)| !v.is_expired()).filter(|(k, _)| re.is_match(k)).map(|(k, _)| k.clone()).collect();
                    }
                }
                Vec::new()
            }
            StorageBackend::HighPerformance { sharded, .. } => sharded.keys(&db_id, pattern).await,
        }
    }

    pub fn start_ttl_cleanup(storage: Arc<Storage>, interval: Duration) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop { ticker.tick().await; storage.cleanup_expired().await; }
        });
    }
}

impl Default for Storage {
    fn default() -> Self { Self::new() }
}
