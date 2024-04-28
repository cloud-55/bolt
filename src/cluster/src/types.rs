use std::time::Instant;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// Operation codes for cluster protocol
pub const CLUSTER_OP_HEARTBEAT: u8 = 1;
pub const CLUSTER_OP_HEARTBEAT_ACK: u8 = 2;
pub const CLUSTER_OP_REPLICATE: u8 = 3;
pub const CLUSTER_OP_REPLICATE_ACK: u8 = 4;
pub const CLUSTER_OP_JOIN: u8 = 5;
pub const CLUSTER_OP_JOIN_ACK: u8 = 6;
pub const CLUSTER_OP_SYNC_REQUEST: u8 = 7;
pub const CLUSTER_OP_SYNC_DATA: u8 = 8;
pub const CLUSTER_OP_SYNC_COMPLETE: u8 = 9;
pub const CLUSTER_OP_ELECTION: u8 = 10;
pub const CLUSTER_OP_ELECTION_ACK: u8 = 11;
pub const CLUSTER_OP_LEADER_ANNOUNCE: u8 = 12;

/// Persistent connection to a peer
pub struct PeerConnection {
    pub reader: OwnedReadHalf,
    pub writer: OwnedWriteHalf,
    pub last_used: Instant,
}

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Leader,
    Follower,
    Candidate,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Leader => write!(f, "leader"),
            NodeRole::Follower => write!(f, "follower"),
            NodeRole::Candidate => write!(f, "candidate"),
        }
    }
}

/// Node state in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Healthy,
    Suspect,
    Dead,
}

/// Peer node information
#[derive(Debug, Clone)]
pub struct PeerNode {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub cluster_port: u16,
    pub state: NodeState,
    pub last_heartbeat: Instant,
    pub role: NodeRole,
}

impl PeerNode {
    pub fn new(id: String, host: String, port: u16, cluster_port: u16) -> Self {
        PeerNode {
            id,
            host,
            port,
            cluster_port,
            state: NodeState::Healthy,
            last_heartbeat: Instant::now(),
            role: NodeRole::Follower,
        }
    }

    pub fn cluster_addr(&self) -> String {
        format!("{}:{}", self.host, self.cluster_port)
    }
}
