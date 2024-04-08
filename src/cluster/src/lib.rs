pub mod config;
pub mod types;
pub mod replication;
pub mod manager;

// Re-exports for convenience
pub use config::ClusterConfig;
pub use types::*;
pub use replication::{ReplicationEntry, ReplicationOp};
pub use manager::{
    ClusterManager,
    replicate_to_peers,
    serialize_replication_entry,
    deserialize_replication_entry,
};
