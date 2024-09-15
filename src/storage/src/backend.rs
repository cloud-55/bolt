use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::types::{DatabaseId, StoredValue};
use crate::wal::Wal;

/// Storage backend mode
pub enum StorageBackend {
    /// Standard mode: single RwLock, sync WAL
    Standard {
        databases: Arc<RwLock<HashMap<DatabaseId, HashMap<String, StoredValue>>>>,
        wal: Arc<std::sync::Mutex<Wal>>,
    },
    /// High-performance mode: sharded storage, batched WAL
    HighPerformance {
        sharded: Arc<crate::sharded::ShardedStorage>,
        wal: Arc<crate::batched_wal::BatchedWal>,
    },
}

impl Clone for StorageBackend {
    fn clone(&self) -> Self {
        match self {
            StorageBackend::Standard { databases, wal } => {
                StorageBackend::Standard {
                    databases: Arc::clone(databases),
                    wal: Arc::clone(wal),
                }
            }
            StorageBackend::HighPerformance { sharded, wal } => {
                StorageBackend::HighPerformance {
                    sharded: Arc::clone(sharded),
                    wal: Arc::clone(wal),
                }
            }
        }
    }
}
