use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use log::info;

use crate::types::{DatabaseId, StoredValue, current_timestamp_ms};
use crate::config::StorageConfig;

/// Core storage engine for Bolt.
///
/// This is a simple in-memory key-value store backed by a HashMap.
/// Each key-value pair lives in a named database (identified by DatabaseId).
/// Thread-safe access is provided via an Arc<RwLock<...>> wrapper.
///
/// Keys are strings, values are StoredValue which wraps the actual data
/// along with optional expiration metadata.
#[derive(Clone)]
pub struct Storage {
    /// Map of databases, each containing key-value pairs.
    /// The outer HashMap maps DatabaseId -> inner database.
    /// The inner HashMap maps String keys -> StoredValue entries.
    databases: Arc<RwLock<HashMap<DatabaseId, HashMap<String, StoredValue>>>>,
}

impl Storage {
    /// Create a new in-memory storage (no persistence).
    ///
    /// Returns a Storage instance with an empty database map.
    /// All data will be lost when the process exits.
    ///
    /// # Examples
    ///
    /// ```
    /// let storage = Storage::new();
    /// ```
    pub fn new() -> Self {
        info!("Initializing in-memory storage");
        Storage {
            databases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create storage with configuration.
    ///
    /// Currently this is equivalent to `new()` since we don't yet
    /// support persistence. The config parameter is accepted for
    /// forward compatibility.
    pub fn with_config(_config: StorageConfig) -> std::io::Result<Self> {
        Ok(Self::new())
    }

    /// Get a value by key from a specific database.
    ///
    /// Returns `None` if the database doesn't exist, the key doesn't exist,
    /// or the value has expired.
    ///
    /// # Arguments
    ///
    /// * `db_id` - The database to query
    /// * `key` - The key to look up
    pub async fn get(&self, db_id: DatabaseId, key: &str) -> Option<String> {
        let databases = self.databases.read().await;
        let db = databases.get(&db_id)?;
        let stored = db.get(key)?;
        if stored.is_expired() {
            None
        } else {
            stored.value()
        }
    }

    /// Set a key-value pair in a specific database.
    ///
    /// Creates the database if it doesn't exist. Overwrites any existing
    /// value for the given key.
    ///
    /// # Arguments
    ///
    /// * `db_id` - The database to write to
    /// * `key` - The key to set
    /// * `value` - The value to store
    pub async fn set(&self, db_id: DatabaseId, key: &str, value: &str) {
        let stored = StoredValue::new(value.to_string());
        let mut databases = self.databases.write().await;
        let db = databases.entry(db_id).or_insert_with(HashMap::new);
        db.insert(key.to_string(), stored);
    }

    /// Remove a key from a specific database.
    ///
    /// Returns the old value if the key existed, or `None` if it didn't.
    ///
    /// # Arguments
    ///
    /// * `db_id` - The database to remove from
    /// * `key` - The key to remove
    pub async fn remove(&self, db_id: DatabaseId, key: &str) -> Option<String> {
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

    /// Get all non-expired entries from a specific database.
    ///
    /// Returns a HashMap of key-value pairs (as strings), filtering out
    /// any entries that have expired. Returns `None` if the database
    /// doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `db_id` - The database to read from
    pub async fn get_all_entries(&self, db_id: DatabaseId) -> Option<HashMap<String, String>> {
        let databases = self.databases.read().await;
        databases.get(&db_id).map(|db| {
            db.iter()
                .filter(|(_, v)| !v.is_expired())
                .filter_map(|(k, v)| {
                    v.value().map(|val| (k.clone(), val))
                })
                .collect()
        })
    }

    /// Count all non-expired keys across all databases.
    ///
    /// Iterates through every database and every key, checking expiration.
    /// This is an O(n) operation where n is the total number of keys.
    pub async fn keys_count(&self) -> usize {
        let databases = self.databases.read().await;
        databases
            .values()
            .flat_map(|db| db.values())
            .filter(|v| !v.is_expired())
            .count()
    }

    /// Count the number of databases that have been created.
    ///
    /// Note: this includes databases that may have had all their
    /// keys removed or expired. Empty databases are not automatically
    /// cleaned up.
    pub async fn databases_count(&self) -> usize {
        let databases = self.databases.read().await;
        databases.len()
    }

    /// Check if the storage has any data at all.
    ///
    /// Returns true if there are no databases or all databases are empty.
    pub async fn is_empty(&self) -> bool {
        let databases = self.databases.read().await;
        if databases.is_empty() {
            return true;
        }
        databases.values().all(|db| db.is_empty())
    }

    /// Check if a specific key exists and is not expired.
    ///
    /// # Arguments
    ///
    /// * `db_id` - The database to check
    /// * `key` - The key to look for
    pub async fn exists(&self, db_id: DatabaseId, key: &str) -> bool {
        let databases = self.databases.read().await;
        databases
            .get(&db_id)
            .and_then(|db| db.get(key))
            .map(|stored| !stored.is_expired())
            .unwrap_or(false)
    }

    /// Get the number of keys in a specific database (excluding expired).
    ///
    /// Returns 0 if the database doesn't exist.
    pub async fn db_keys_count(&self, db_id: DatabaseId) -> usize {
        let databases = self.databases.read().await;
        databases
            .get(&db_id)
            .map(|db| {
                db.values()
                    .filter(|v| !v.is_expired())
                    .count()
            })
            .unwrap_or(0)
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}
