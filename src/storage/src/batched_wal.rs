use std::io::{self, Write};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use log::info;

use crate::wal::{Wal, WalEntry};

/// Configuration for the batched WAL writer
#[derive(Clone)]
pub struct BatchedWalConfig {
    /// Maximum number of entries to buffer before flushing
    pub batch_size: usize,
    /// Maximum time to wait before flushing
    pub flush_interval: Duration,
    /// WAL file path
    pub path: PathBuf,
}

impl Default for BatchedWalConfig {
    fn default() -> Self {
        BatchedWalConfig {
            batch_size: 1000,
            flush_interval: Duration::from_millis(10),
            path: PathBuf::from("./bolt.wal"),
        }
    }
}

impl BatchedWalConfig {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        BatchedWalConfig {
            path: path.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }
}

/// Command sent to the WAL writer task
enum WalCommand {
    Write(WalEntry),
    Compact(Vec<(String, String, String, Option<u64>)>),
    Shutdown,
}

/// Batched Write-Ahead Log for high-throughput durability
///
/// Instead of flushing to disk on every write, this implementation
/// batches writes and flushes periodically or when the batch is full.
pub struct BatchedWal {
    sender: mpsc::Sender<WalCommand>,
    config: BatchedWalConfig,
    enabled: bool,
}

impl BatchedWal {
    /// Create a new batched WAL with the given configuration
    pub async fn new(config: BatchedWalConfig) -> io::Result<Self> {
        // Create parent directories if needed
        if let Some(parent) = config.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let (sender, receiver) = mpsc::channel(config.batch_size * 2);

        // Start the background writer task in a dedicated thread with its own runtime
        // This ensures the task survives even if created from a temporary runtime
        let writer_config = config.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create WAL runtime");
            rt.block_on(wal_writer_task(receiver, writer_config));
        });

        info!("BatchedWal initialized at {:?} (batch_size={}, flush_interval={:?})",
              config.path, config.batch_size, config.flush_interval);

        Ok(BatchedWal {
            sender,
            config,
            enabled: true,
        })
    }

    /// Create a disabled batched WAL (in-memory only mode)
    pub fn disabled() -> Self {
        let (sender, _) = mpsc::channel(1);
        BatchedWal {
            sender,
            config: BatchedWalConfig::default(),
            enabled: false,
        }
    }

    /// Check if WAL is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the WAL file path
    pub fn path(&self) -> &Path {
        &self.config.path
    }

    /// Log a SET operation
    pub async fn log_set(&self, database_id: &str, key: &str, value: &str, ttl_ms: Option<u64>) -> io::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let entry = WalEntry::Set {
            database_id: database_id.to_string(),
            key: key.to_string(),
            value: value.to_string(),
            ttl_ms,
        };

        self.sender.send(WalCommand::Write(entry)).await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL writer task closed"))?;

        Ok(())
    }

    /// Log a DELETE operation
    pub async fn log_delete(&self, database_id: &str, key: &str) -> io::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let entry = WalEntry::Delete {
            database_id: database_id.to_string(),
            key: key.to_string(),
        };

        self.sender.send(WalCommand::Write(entry)).await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL writer task closed"))?;

        Ok(())
    }

    /// Compact the WAL with a snapshot
    pub async fn compact(&self, snapshot: Vec<(String, String, String, Option<u64>)>) -> io::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        self.sender.send(WalCommand::Compact(snapshot)).await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL writer task closed"))?;

        Ok(())
    }

    /// Read all entries from the WAL file (for recovery)
    pub fn read_entries(&self) -> io::Result<Vec<WalEntry>> {
        if !self.enabled {
            return Ok(Vec::new());
        }

        // Use the synchronous WAL reader
        let wal = Wal::new(&self.config.path)?;
        wal.read_entries()
    }

    /// Shutdown the WAL writer
    pub async fn shutdown(&self) {
        if self.enabled {
            let _ = self.sender.send(WalCommand::Shutdown).await;
        }
    }
}

/// Background task that handles batched writes
async fn wal_writer_task(mut receiver: mpsc::Receiver<WalCommand>, config: BatchedWalConfig) {
    let mut file = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.path)
    {
        Ok(f) => f,
        Err(e) => {
            info!("Failed to open WAL file: {}", e);
            return;
        }
    };

    let mut buffer: Vec<WalEntry> = Vec::with_capacity(config.batch_size);
    let mut interval = tokio::time::interval(config.flush_interval);

    loop {
        tokio::select! {
            // Check for new commands
            cmd = receiver.recv() => {
                match cmd {
                    Some(WalCommand::Write(entry)) => {
                        buffer.push(entry);
                        if buffer.len() >= config.batch_size {
                            flush_buffer_sync(&mut file, &mut buffer);
                        }
                    }
                    Some(WalCommand::Compact(snapshot)) => {
                        // Flush any pending writes first
                        flush_buffer_sync(&mut file, &mut buffer);

                        // Perform compaction
                        if let Err(e) = perform_compaction_sync(&config.path, snapshot) {
                            info!("WAL compaction error: {}", e);
                        }

                        // Reopen file after compaction
                        match OpenOptions::new().append(true).open(&config.path) {
                            Ok(f) => file = f,
                            Err(e) => {
                                info!("Failed to reopen WAL file after compaction: {}", e);
                                return;
                            }
                        }
                    }
                    Some(WalCommand::Shutdown) | None => {
                        // Flush remaining entries and exit
                        flush_buffer_sync(&mut file, &mut buffer);
                        info!("WAL writer task shutting down");
                        return;
                    }
                }
            }
            // Periodic flush
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    flush_buffer_sync(&mut file, &mut buffer);
                }
            }
        }
    }
}

/// Flush the buffer to disk (binary format)
fn flush_buffer_sync(file: &mut File, buffer: &mut Vec<WalEntry>) {
    if buffer.is_empty() {
        return;
    }

    let mut data = Vec::new();
    for entry in buffer.drain(..) {
        data.extend(entry.encode());
    }

    if let Err(e) = file.write_all(&data) {
        info!("WAL write error: {}", e);
        return;
    }

    if let Err(e) = file.flush() {
        info!("WAL flush error: {}", e);
    }
}

/// Perform WAL compaction (binary format)
fn perform_compaction_sync(
    path: &Path,
    snapshot: Vec<(String, String, String, Option<u64>)>,
) -> io::Result<()> {
    // Create backup
    let backup_path = path.with_extension("wal.bak");
    if path.exists() {
        std::fs::rename(path, &backup_path)?;
    }

    // Write new compacted WAL in binary format
    let mut new_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;

    for (db_id, key, value, ttl_ms) in &snapshot {
        let entry = WalEntry::Set {
            database_id: db_id.clone(),
            key: key.clone(),
            value: value.clone(),
            ttl_ms: *ttl_ms,
        };
        new_file.write_all(&entry.encode())?;
    }
    new_file.flush()?;

    // Remove backup
    if backup_path.exists() {
        std::fs::remove_file(&backup_path)?;
    }

    info!("WAL compacted with {} entries (binary format)", snapshot.len());
    Ok(())
}

/// High-performance storage with batched WAL
pub struct HighPerfStorage {
    sharded: Arc<crate::sharded::ShardedStorage>,
    wal: Arc<BatchedWal>,
    operation_count: std::sync::atomic::AtomicUsize,
    compaction_threshold: usize,
}

impl HighPerfStorage {
    /// Create a new high-performance storage
    pub async fn new(config: BatchedWalConfig, compaction_threshold: usize) -> io::Result<Self> {
        let wal = BatchedWal::new(config.clone()).await?;
        let sharded = Arc::new(crate::sharded::ShardedStorage::new());

        // Recover from WAL
        let entries = wal.read_entries()?;
        let entry_count = entries.len();

        for entry in entries {
            match entry {
                WalEntry::Set { database_id, key, value, ttl_ms } => {
                    let db_id = crate::types::DatabaseId::from_wal_string(&database_id);
                    let stored = if let Some(ttl) = ttl_ms {
                        crate::types::StoredValue::with_ttl(value, Duration::from_millis(ttl))
                    } else {
                        crate::types::StoredValue::new(value)
                    };
                    sharded.import_entry(db_id, key, stored).await;
                }
                WalEntry::Delete { database_id, key } => {
                    let db_id = crate::types::DatabaseId::from_wal_string(&database_id);
                    sharded.remove_entry(&db_id, &key).await;
                }
            }
        }

        if entry_count > 0 {
            info!("Recovered {} WAL entries", entry_count);
        }

        Ok(HighPerfStorage {
            sharded,
            wal: Arc::new(wal),
            operation_count: std::sync::atomic::AtomicUsize::new(0),
            compaction_threshold,
        })
    }

    /// Create in-memory only storage (no persistence)
    pub fn in_memory() -> Self {
        HighPerfStorage {
            sharded: Arc::new(crate::sharded::ShardedStorage::new()),
            wal: Arc::new(BatchedWal::disabled()),
            operation_count: std::sync::atomic::AtomicUsize::new(0),
            compaction_threshold: 0,
        }
    }

    /// Get the sharded storage reference
    pub fn sharded(&self) -> &Arc<crate::sharded::ShardedStorage> {
        &self.sharded
    }

    /// Check if persistence is enabled
    pub fn is_persistent(&self) -> bool {
        self.wal.is_enabled()
    }

    /// Log a SET operation to WAL
    pub async fn log_set(&self, db_id: &str, key: &str, value: &str, ttl_ms: Option<u64>) {
        if let Err(e) = self.wal.log_set(db_id, key, value, ttl_ms).await {
            info!("WAL write error: {}", e);
        }
        self.maybe_compact().await;
    }

    /// Log a DELETE operation to WAL
    pub async fn log_delete(&self, db_id: &str, key: &str) {
        if let Err(e) = self.wal.log_delete(db_id, key).await {
            info!("WAL write error: {}", e);
        }
        self.maybe_compact().await;
    }

    /// Check and perform compaction if needed
    async fn maybe_compact(&self) {
        if self.compaction_threshold == 0 {
            return;
        }

        let count = self.operation_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if count > 0 && count % self.compaction_threshold == 0 {
            self.compact().await;
        }
    }

    /// Compact the WAL
    pub async fn compact(&self) {
        let snapshot = self.sharded.snapshot().await;
        if let Err(e) = self.wal.compact(snapshot).await {
            info!("WAL compaction error: {}", e);
        }
    }

    /// Get key count
    pub async fn keys_count(&self) -> usize {
        self.sharded.keys_count().await
    }

    /// Get database count
    pub async fn databases_count(&self) -> usize {
        self.sharded.databases_count().await
    }

    /// Cleanup expired keys
    pub async fn cleanup_expired(&self) -> usize {
        self.sharded.cleanup_expired().await
    }

    /// Shutdown the storage (flush pending WAL writes)
    pub async fn shutdown(&self) {
        self.wal.shutdown().await;
    }

    /// Start TTL cleanup task
    pub fn start_ttl_cleanup(storage: Arc<HighPerfStorage>, interval: Duration) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                storage.cleanup_expired().await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_batched_wal() {
        let path = "/tmp/test_bolt_batched_wal.log";
        let _ = std::fs::remove_file(path);

        let config = BatchedWalConfig::new(path)
            .with_batch_size(5)
            .with_flush_interval(Duration::from_millis(100));

        let wal = BatchedWal::new(config).await.unwrap();

        // Write some entries
        for i in 0..10 {
            wal.log_set("default", &format!("key{}", i), &format!("value{}", i), None).await.unwrap();
        }

        // Wait for flush
        tokio::time::sleep(Duration::from_millis(200)).await;
        wal.shutdown().await;

        // Verify entries
        let wal2 = BatchedWalConfig::new(path);
        let entries = Wal::new(path).unwrap().read_entries().unwrap();
        assert_eq!(entries.len(), 10);

        std::fs::remove_file(path).unwrap();
    }
}
