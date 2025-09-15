// Core modules
pub mod types;
pub mod config;
pub mod backend;
pub mod storage;
pub mod memory;

// Support modules
pub mod wal;
pub mod sharded;
pub mod batched_wal;
pub mod crdt;
pub mod compression;

// Re-export main types for convenience
pub use types::{DataType, DatabaseId, StoredValue, current_timestamp_ms};
pub use config::StorageConfig;
pub use storage::Storage;
pub use sharded::ShardedStorage;
pub use batched_wal::{BatchedWal, BatchedWalConfig, HighPerfStorage};
pub use crdt::{GCounter, PNCounter};
pub use compression::{compress_if_needed, decompress, COMPRESSION_THRESHOLD};
pub use memory::{MemoryTracker, LRUTracker, EvictionPolicy, format_bytes, parse_memory_size, default_max_memory};