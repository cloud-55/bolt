pub mod auth;
pub mod error;
pub mod http_metrics;
pub mod message;
pub mod metrics;
pub mod opcodes;
pub mod pubsub;
pub mod server;
pub mod tls;

// Re-export commonly used types
pub use error::ServerError;
pub use metrics::Metrics;
pub use opcodes::*;
pub use pubsub::{SubscriptionManager, Task, TaskStatus, Notification};

// Re-export cluster types from cluster crate for backwards compatibility
pub use cluster::{
    ClusterConfig,
    ClusterManager,
    NodeRole,
    NodeState,
    PeerNode,
    PeerConnection,
    ReplicationEntry,
    ReplicationOp,
    CLUSTER_OP_HEARTBEAT,
    CLUSTER_OP_HEARTBEAT_ACK,
    CLUSTER_OP_REPLICATE,
    CLUSTER_OP_REPLICATE_ACK,
    CLUSTER_OP_JOIN,
    CLUSTER_OP_JOIN_ACK,
    CLUSTER_OP_SYNC_REQUEST,
    CLUSTER_OP_SYNC_DATA,
    CLUSTER_OP_SYNC_COMPLETE,
    CLUSTER_OP_ELECTION,
    CLUSTER_OP_ELECTION_ACK,
    CLUSTER_OP_LEADER_ANNOUNCE,
    replicate_to_peers,
    serialize_replication_entry,
    deserialize_replication_entry,
};
