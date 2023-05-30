use std::path::Path;
use std::time::Duration;
use crate::memory::{EvictionPolicy, default_max_memory, parse_memory_size};

/// Storage configuration
pub struct StorageConfig {
    pub wal_path: Option<String>,
    pub compaction_threshold: usize,
    pub ttl_cleanup_interval: Duration,
    /// Enable high-performance mode with sharded storage and batched WAL
    pub high_performance: bool,
    /// Number of shards for high-performance mode (default: 64)
    pub shard_count: usize,
    /// WAL batch size for high-performance mode (default: 1000)
    pub wal_batch_size: usize,
    /// WAL flush interval in milliseconds for high-performance mode (default: 10ms)
    pub wal_flush_interval_ms: u64,
    /// Maximum memory in bytes (0 = unlimited, default = 75% of system RAM)
    pub max_memory: usize,
    /// Policy for evicting keys when max_memory is reached
    pub eviction_policy: EvictionPolicy,
    /// Memory warning threshold percentage (default: 90)
    pub memory_warning_threshold: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            wal_path: None,
            compaction_threshold: 10000,
            ttl_cleanup_interval: Duration::from_secs(60),
            high_performance: false,
            shard_count: 64,
            wal_batch_size: 1000,
            wal_flush_interval_ms: 10,
            max_memory: 0, // Will be set to default on Storage::new() if 0
            eviction_policy: EvictionPolicy::default(),
            memory_warning_threshold: 90,
        }
    }
}

impl StorageConfig {
    /// Create config with WAL enabled at the given path
    pub fn with_wal<P: AsRef<Path>>(path: P) -> Self {
        StorageConfig {
            wal_path: Some(path.as_ref().to_string_lossy().to_string()),
            ..Default::default()
        }
    }

    /// Create config for in-memory only storage
    pub fn in_memory() -> Self {
        StorageConfig {
            wal_path: None,
            compaction_threshold: 0,
            ..Default::default()
        }
    }

    /// Enable high-performance mode with sharded storage and batched WAL
    pub fn with_high_performance(mut self) -> Self {
        self.high_performance = true;
        self
    }

    /// Set the number of shards for high-performance mode
    pub fn with_shard_count(mut self, count: usize) -> Self {
        self.shard_count = count;
        self
    }

    /// Set the WAL batch size for high-performance mode
    pub fn with_wal_batch_size(mut self, size: usize) -> Self {
        self.wal_batch_size = size;
        self
    }

    /// Set the WAL flush interval in milliseconds
    pub fn with_wal_flush_interval_ms(mut self, ms: u64) -> Self {
        self.wal_flush_interval_ms = ms;
        self
    }

    /// Set maximum memory in bytes
    pub fn with_max_memory(mut self, max_bytes: usize) -> Self {
        self.max_memory = max_bytes;
        self
    }

    /// Set maximum memory from a string (e.g., "1gb", "512mb", "75%")
    pub fn with_max_memory_str(mut self, s: &str) -> Self {
        match parse_memory_size(s) {
            Ok(bytes) => self.max_memory = bytes,
            Err(e) => log::warn!("Invalid max_memory '{}': {}", s, e),
        }
        self
    }

    /// Set eviction policy
    pub fn with_eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    /// Set memory warning threshold percentage
    pub fn with_memory_warning_threshold(mut self, threshold: usize) -> Self {
        self.memory_warning_threshold = threshold.min(100);
        self
    }

    /// Disable memory limit (unlimited)
    pub fn with_unlimited_memory(mut self) -> Self {
        self.max_memory = 0;
        self.eviction_policy = EvictionPolicy::NoEviction;
        self
    }

    /// Create config from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // WAL path
        if let Ok(path) = std::env::var("BOLT_DATA_DIR") {
            let persist = std::env::var("BOLT_PERSIST")
                .map(|v| v == "1" || v.to_lowercase() == "true")
                .unwrap_or(true);
            if persist {
                config.wal_path = Some(path);
            }
        }

        // High performance mode
        if let Ok(hp) = std::env::var("BOLT_HIGH_PERF") {
            config.high_performance = hp == "1" || hp.to_lowercase() == "true";
        }

        // Shard count
        if let Ok(count) = std::env::var("BOLT_SHARD_COUNT") {
            if let Ok(n) = count.parse() {
                config.shard_count = n;
            }
        }

        // WAL batch size
        if let Ok(size) = std::env::var("BOLT_WAL_BATCH_SIZE") {
            if let Ok(n) = size.parse() {
                config.wal_batch_size = n;
            }
        }

        // WAL flush interval
        if let Ok(ms) = std::env::var("BOLT_WAL_FLUSH_INTERVAL_MS") {
            if let Ok(n) = ms.parse() {
                config.wal_flush_interval_ms = n;
            }
        }

        // Max memory
        if let Ok(mem) = std::env::var("BOLT_MAX_MEMORY") {
            match parse_memory_size(&mem) {
                Ok(bytes) => config.max_memory = bytes,
                Err(e) => log::warn!("Invalid BOLT_MAX_MEMORY '{}': {}", mem, e),
            }
        } else {
            // Use default (75% of system RAM)
            config.max_memory = default_max_memory();
        }

        // Eviction policy
        if let Ok(policy) = std::env::var("BOLT_EVICTION_POLICY") {
            match EvictionPolicy::from_str(&policy) {
                Some(p) => config.eviction_policy = p,
                None => log::warn!("Invalid BOLT_EVICTION_POLICY '{}', using default", policy),
            }
        }

        // Memory warning threshold
        if let Ok(threshold) = std::env::var("BOLT_MEMORY_WARNING_THRESHOLD") {
            if let Ok(n) = threshold.parse() {
                config.memory_warning_threshold = n;
            }
        }

        config
    }
}
