use std::time::Duration;

/// Cluster configuration
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_id: String,
    pub cluster_port: u16,
    pub client_port: u16,
    pub peers: Vec<String>,  // host:port format
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub replication_timeout: Duration,
    /// Threshold above which Full Sync is preferred over drain (default: 10000)
    pub pending_drain_threshold: usize,
    /// Batch size for pending drain (default: 200)
    pub pending_drain_batch_size: usize,
    /// Delay between batches during drain in ms (default: 20)
    pub pending_drain_batch_delay_ms: u64,
    /// Max consecutive errors before aborting drain (default: 3)
    pub pending_drain_max_errors: usize,
}

impl ClusterConfig {
    pub fn from_env() -> Option<Self> {
        let node_id = std::env::var("BOLT_NODE_ID").ok()?;
        let cluster_port_str = std::env::var("BOLT_CLUSTER_PORT").unwrap_or_else(|_| "8519".to_string());
        let cluster_port = cluster_port_str.parse().ok()?;

        let client_port_str = std::env::var("BOLT_PORT").unwrap_or_else(|_| "8518".to_string());
        let client_port = client_port_str.parse().unwrap_or(8518);

        let peers_str = std::env::var("BOLT_CLUSTER_PEERS").unwrap_or_default();
        let peers: Vec<String> = if peers_str.is_empty() {
            Vec::new()
        } else {
            peers_str.split(',').map(|s| s.trim().to_string()).collect()
        };

        let heartbeat_interval = std::env::var("BOLT_HEARTBEAT_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(1000));

        let heartbeat_timeout = std::env::var("BOLT_HEARTBEAT_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(5000));

        let replication_timeout = std::env::var("BOLT_REPLICATION_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(3000));

        let pending_drain_threshold = std::env::var("BOLT_PENDING_DRAIN_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);

        let pending_drain_batch_size = std::env::var("BOLT_PENDING_DRAIN_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200);

        let pending_drain_batch_delay_ms = std::env::var("BOLT_PENDING_DRAIN_BATCH_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20);

        let pending_drain_max_errors = std::env::var("BOLT_PENDING_DRAIN_MAX_ERRORS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);

        Some(ClusterConfig {
            node_id,
            cluster_port,
            client_port,
            peers,
            heartbeat_interval,
            heartbeat_timeout,
            replication_timeout,
            pending_drain_threshold,
            pending_drain_batch_size,
            pending_drain_batch_delay_ms,
            pending_drain_max_errors,
        })
    }
}
