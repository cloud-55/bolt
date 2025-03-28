use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use storage::Storage;
use cluster::ClusterManager;

/// Server metrics
#[derive(Clone)]
pub struct Metrics {
    pub active_connections: Arc<AtomicUsize>,
    pub total_connections: Arc<AtomicUsize>,
    pub total_gets: Arc<AtomicUsize>,
    pub total_puts: Arc<AtomicUsize>,
    pub total_deletes: Arc<AtomicUsize>,
    pub total_mgets: Arc<AtomicUsize>,
    pub total_msets: Arc<AtomicUsize>,
    pub total_mdels: Arc<AtomicUsize>,
    pub total_incrs: Arc<AtomicUsize>,
    pub total_list_ops: Arc<AtomicUsize>,
    pub total_set_ops: Arc<AtomicUsize>,
    pub total_expired_keys: Arc<AtomicUsize>,
    pub total_keys_written: Arc<AtomicUsize>,
    pub total_keys_read: Arc<AtomicUsize>,
    pub total_keys_deleted: Arc<AtomicUsize>,
    pub start_time: std::time::Instant,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            active_connections: Arc::new(AtomicUsize::new(0)),
            total_connections: Arc::new(AtomicUsize::new(0)),
            total_gets: Arc::new(AtomicUsize::new(0)),
            total_puts: Arc::new(AtomicUsize::new(0)),
            total_deletes: Arc::new(AtomicUsize::new(0)),
            total_mgets: Arc::new(AtomicUsize::new(0)),
            total_msets: Arc::new(AtomicUsize::new(0)),
            total_mdels: Arc::new(AtomicUsize::new(0)),
            total_incrs: Arc::new(AtomicUsize::new(0)),
            total_list_ops: Arc::new(AtomicUsize::new(0)),
            total_set_ops: Arc::new(AtomicUsize::new(0)),
            total_expired_keys: Arc::new(AtomicUsize::new(0)),
            total_keys_written: Arc::new(AtomicUsize::new(0)),
            total_keys_read: Arc::new(AtomicUsize::new(0)),
            total_keys_deleted: Arc::new(AtomicUsize::new(0)),
            start_time: std::time::Instant::now(),
        }
    }

    pub fn to_json(&self, _storage: &Storage) -> String {
        let uptime_secs = self.start_time.elapsed().as_secs();
        format!(
            r#"{{"active_connections":{},"total_connections":{},"total_gets":{},"total_puts":{},"total_deletes":{},"total_mgets":{},"total_msets":{},"total_mdels":{},"total_incrs":{},"total_list_ops":{},"total_set_ops":{},"total_expired_keys":{},"uptime_seconds":{}}}"#,
            self.active_connections.load(Ordering::SeqCst),
            self.total_connections.load(Ordering::SeqCst),
            self.total_gets.load(Ordering::SeqCst),
            self.total_puts.load(Ordering::SeqCst),
            self.total_deletes.load(Ordering::SeqCst),
            self.total_mgets.load(Ordering::SeqCst),
            self.total_msets.load(Ordering::SeqCst),
            self.total_mdels.load(Ordering::SeqCst),
            self.total_incrs.load(Ordering::SeqCst),
            self.total_list_ops.load(Ordering::SeqCst),
            self.total_set_ops.load(Ordering::SeqCst),
            self.total_expired_keys.load(Ordering::SeqCst),
            uptime_secs,
        )
    }

    /// Generate Prometheus-format metrics
    pub async fn to_prometheus(&self, storage: &Storage, cluster: Option<&Arc<ClusterManager>>) -> String {
        let uptime_secs = self.start_time.elapsed().as_secs();
        let keys_count = storage.keys_count().await;
        let db_count = storage.databases_count().await;

        let mut output = String::new();

        // Connection metrics
        output.push_str("# HELP bolt_active_connections Current number of active connections\n");
        output.push_str("# TYPE bolt_active_connections gauge\n");
        output.push_str(&format!("bolt_active_connections {}\n", self.active_connections.load(Ordering::SeqCst)));

        output.push_str("# HELP bolt_total_connections Total number of connections since start\n");
        output.push_str("# TYPE bolt_total_connections counter\n");
        output.push_str(&format!("bolt_total_connections {}\n", self.total_connections.load(Ordering::SeqCst)));

        // Operation metrics
        output.push_str("# HELP bolt_operations_total Total number of operations by type\n");
        output.push_str("# TYPE bolt_operations_total counter\n");
        output.push_str(&format!("bolt_operations_total{{type=\"get\"}} {}\n", self.total_gets.load(Ordering::SeqCst)));
        output.push_str(&format!("bolt_operations_total{{type=\"put\"}} {}\n", self.total_puts.load(Ordering::SeqCst)));
        output.push_str(&format!("bolt_operations_total{{type=\"del\"}} {}\n", self.total_deletes.load(Ordering::SeqCst)));
        output.push_str(&format!("bolt_operations_total{{type=\"mget\"}} {}\n", self.total_mgets.load(Ordering::SeqCst)));
        output.push_str(&format!("bolt_operations_total{{type=\"mset\"}} {}\n", self.total_msets.load(Ordering::SeqCst)));
        output.push_str(&format!("bolt_operations_total{{type=\"mdel\"}} {}\n", self.total_mdels.load(Ordering::SeqCst)));
        output.push_str(&format!("bolt_operations_total{{type=\"incr\"}} {}\n", self.total_incrs.load(Ordering::SeqCst)));
        output.push_str(&format!("bolt_operations_total{{type=\"list\"}} {}\n", self.total_list_ops.load(Ordering::SeqCst)));
        output.push_str(&format!("bolt_operations_total{{type=\"set\"}} {}\n", self.total_set_ops.load(Ordering::SeqCst)));

        // TTL metrics
        output.push_str("# HELP bolt_expired_keys_total Total number of expired keys cleaned up\n");
        output.push_str("# TYPE bolt_expired_keys_total counter\n");
        output.push_str(&format!("bolt_expired_keys_total {}\n", self.total_expired_keys.load(Ordering::SeqCst)));

        // Key throughput metrics (counts individual keys, not operations)
        output.push_str("# HELP bolt_keys_written_total Total number of keys written (includes batch operations)\n");
        output.push_str("# TYPE bolt_keys_written_total counter\n");
        output.push_str(&format!("bolt_keys_written_total {}\n", self.total_keys_written.load(Ordering::SeqCst)));

        output.push_str("# HELP bolt_keys_read_total Total number of keys read (includes batch operations)\n");
        output.push_str("# TYPE bolt_keys_read_total counter\n");
        output.push_str(&format!("bolt_keys_read_total {}\n", self.total_keys_read.load(Ordering::SeqCst)));

        output.push_str("# HELP bolt_keys_deleted_total Total number of keys deleted (includes batch operations)\n");
        output.push_str("# TYPE bolt_keys_deleted_total counter\n");
        output.push_str(&format!("bolt_keys_deleted_total {}\n", self.total_keys_deleted.load(Ordering::SeqCst)));

        // Storage metrics
        output.push_str("# HELP bolt_keys_total Current number of keys in storage\n");
        output.push_str("# TYPE bolt_keys_total gauge\n");
        output.push_str(&format!("bolt_keys_total {}\n", keys_count));

        output.push_str("# HELP bolt_databases_total Current number of databases\n");
        output.push_str("# TYPE bolt_databases_total gauge\n");
        output.push_str(&format!("bolt_databases_total {}\n", db_count));

        // Uptime metric
        output.push_str("# HELP bolt_uptime_seconds Server uptime in seconds\n");
        output.push_str("# TYPE bolt_uptime_seconds counter\n");
        output.push_str(&format!("bolt_uptime_seconds {}\n", uptime_secs));

        // Cluster replication metrics (if cluster mode is enabled)
        if let Some(cluster) = cluster {
            let pending_count = cluster.total_pending_count().await;
            let (drain_ops, drain_skipped, drain_entries) = cluster.drain_metrics();

            output.push_str("\n# Cluster Replication Metrics\n");

            output.push_str("# HELP bolt_pending_replication_total Current number of pending replication entries\n");
            output.push_str("# TYPE bolt_pending_replication_total gauge\n");
            output.push_str(&format!("bolt_pending_replication_total {}\n", pending_count));

            output.push_str("# HELP bolt_drain_operations_total Total drain operations performed\n");
            output.push_str("# TYPE bolt_drain_operations_total counter\n");
            output.push_str(&format!("bolt_drain_operations_total {}\n", drain_ops));

            output.push_str("# HELP bolt_drain_skipped_total Drain operations skipped (threshold exceeded, Full Sync used)\n");
            output.push_str("# TYPE bolt_drain_skipped_total counter\n");
            output.push_str(&format!("bolt_drain_skipped_total {}\n", drain_skipped));

            output.push_str("# HELP bolt_drain_entries_total Total entries successfully drained to peers\n");
            output.push_str("# TYPE bolt_drain_entries_total counter\n");
            output.push_str(&format!("bolt_drain_entries_total {}\n", drain_entries));
        }

        output
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

