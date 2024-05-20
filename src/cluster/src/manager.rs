use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use log::info;
use storage::{Storage, DatabaseId};

use crate::config::ClusterConfig;
use crate::types::*;
use crate::replication::{ReplicationEntry, ReplicationOp};

/// Cluster manager handles all cluster operations
pub struct ClusterManager {
    config: ClusterConfig,
    role: Arc<RwLock<NodeRole>>,
    leader_id: Arc<RwLock<Option<String>>>,
    peers: Arc<RwLock<HashMap<String, PeerNode>>>,
    storage: Storage,
    sequence: Arc<std::sync::atomic::AtomicU64>,
    last_applied_sequence: Arc<std::sync::atomic::AtomicU64>,
    replication_tx: mpsc::Sender<ReplicationEntry>,
    shutdown_tx: broadcast::Sender<()>,
}

impl ClusterManager {
    pub fn new(config: ClusterConfig, storage: Storage) -> (Self, mpsc::Receiver<ReplicationEntry>) {
        let (replication_tx, replication_rx) = mpsc::channel(10000);
        let (shutdown_tx, _) = broadcast::channel(1);

        let manager = ClusterManager {
            config,
            role: Arc::new(RwLock::new(NodeRole::Follower)),
            leader_id: Arc::new(RwLock::new(None)),
            peers: Arc::new(RwLock::new(HashMap::new())),
            storage,
            sequence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_applied_sequence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            replication_tx,
            shutdown_tx,
        };

        (manager, replication_rx)
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub async fn role(&self) -> NodeRole {
        *self.role.read().await
    }

    pub async fn set_role(&self, role: NodeRole) {
        let mut r = self.role.write().await;
        *r = role;
    }

    pub async fn is_leader(&self) -> bool {
        *self.role.read().await == NodeRole::Leader
    }

    pub async fn leader_id(&self) -> Option<String> {
        self.leader_id.read().await.clone()
    }

    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    pub fn peers_ref(&self) -> &Arc<RwLock<HashMap<String, PeerNode>>> {
        &self.peers
    }

    /// Get the next sequence number for replication
    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Queue a replication entry
    pub async fn replicate(&self, entry: ReplicationEntry) -> Result<(), mpsc::error::SendError<ReplicationEntry>> {
        self.replication_tx.send(entry).await
    }

    /// Add or update a peer
    pub async fn add_peer(&self, peer: PeerNode) {
        let mut peers = self.peers.write().await;
        peers.insert(peer.id.clone(), peer);
    }

    /// Get all healthy peers
    pub async fn healthy_peers(&self) -> Vec<PeerNode> {
        let peers = self.peers.read().await;
        peers.values()
            .filter(|p| p.state == NodeState::Healthy)
            .cloned()
            .collect()
    }

    /// Get peer count
    pub async fn peer_count(&self) -> usize {
        self.peers.read().await.len()
    }

    /// Update peer heartbeat
    pub async fn update_peer_heartbeat(&self, peer_id: &str, role: NodeRole) -> bool {
        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.get_mut(peer_id) {
            let was_unhealthy = peer.state != NodeState::Healthy;
            peer.last_heartbeat = Instant::now();
            peer.state = NodeState::Healthy;
            peer.role = role;
            return was_unhealthy;
        }
        false
    }

    /// Apply a replication entry to local storage
    pub fn apply_replication_entry(&self, entry: &ReplicationEntry) -> impl std::future::Future<Output = ()> + Send + '_ {
        let db_id = DatabaseId::from_wal_string(&entry.database_id);
        let entry_sequence = entry.sequence;
        let operation = entry.operation.clone();
        let storage = &self.storage;
        let last_applied = &self.last_applied_sequence;

        async move {
            match operation {
                ReplicationOp::Set { key, value } => {
                    storage.set(db_id, &key, &value, None).await;
                }
                ReplicationOp::Delete { key } => {
                    storage.delete(db_id, &key).await;
                }
                ReplicationOp::Incr { key } => {
                    storage.incr(db_id, &key).await;
                }
                ReplicationOp::Decr { key } => {
                    storage.decr(db_id, &key).await;
                }
                ReplicationOp::IncrBy { key, delta } => {
                    storage.incr_by(db_id, &key, delta).await;
                }
                _ => {
                    info!("Unsupported replication operation: {:?}", operation);
                }
            }

            last_applied.store(entry_sequence, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Connect to a peer node
    /// addr format: "node_id:host:cluster_port" (e.g., "node2:127.0.0.1:3013")
    pub async fn connect_to_peer(&self, peer_spec: &str) -> tokio::io::Result<()> {
        let parts: Vec<&str> = peer_spec.split(':').collect();
        if parts.len() < 3 {
            return Err(tokio::io::Error::new(
                tokio::io::ErrorKind::InvalidInput,
                format!("Invalid peer spec: {} (expected node_id:host:port)", peer_spec),
            ));
        }

        let expected_peer_id = parts[0];
        let host = parts[1];
        let cluster_port: u16 = parts[2].parse().map_err(|_| {
            tokio::io::Error::new(tokio::io::ErrorKind::InvalidInput, "Invalid port")
        })?;

        let connect_addr = format!("{}:{}", host, cluster_port);
        info!("Connecting to peer {} at {}", expected_peer_id, connect_addr);

        let stream = TcpStream::connect(&connect_addr).await?;
        let (mut reader, mut writer) = stream.into_split();

        let join_msg = format!("{}:{}:{}", self.config.node_id, self.config.cluster_port, self.config.client_port);
        writer.write_u8(CLUSTER_OP_JOIN).await?;
        writer.write_u32(join_msg.len() as u32).await?;
        writer.write_all(join_msg.as_bytes()).await?;
        writer.flush().await?;

        let op = reader.read_u8().await?;
        if op == CLUSTER_OP_JOIN_ACK {
            let len = reader.read_u32().await? as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).await?;
            let peer_info = String::from_utf8_lossy(&buf);

            let info_parts: Vec<&str> = peer_info.split(':').collect();
            if info_parts.len() >= 3 {
                let peer_id = info_parts[0].to_string();
                let client_port: u16 = info_parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(2012);
                let peer_role = match info_parts.get(2) {
                    Some(&"leader") => NodeRole::Leader,
                    _ => NodeRole::Follower,
                };

                let peer = PeerNode::new(peer_id.clone(), host.to_string(), client_port, cluster_port);
                self.add_peer(peer).await;

                info!("Connected to peer: {} (role: {})", peer_id, peer_role);
            }
        }

        Ok(())
    }

    /// Perform leader election (lowest node ID wins)
    pub async fn perform_leader_election(&self) {
        let peers = self.peers.read().await;
        let mut all_ids: Vec<&str> = peers.values()
            .filter(|p| p.state != NodeState::Dead)
            .map(|p| p.id.as_str())
            .collect();
        all_ids.push(&self.config.node_id);
        all_ids.sort();

        let new_leader = all_ids.first().map(|s| s.to_string());

        if new_leader.as_deref() == Some(&self.config.node_id) {
            let current_role = *self.role.read().await;
            if current_role != NodeRole::Leader {
                info!("This node ({}) elected as leader", self.config.node_id);
                *self.role.write().await = NodeRole::Leader;
                *self.leader_id.write().await = Some(self.config.node_id.clone());

                drop(peers);
                self.announce_leadership().await;
            }
        } else {
            info!("Node {} is follower (leader: {:?})", self.config.node_id, new_leader);
            *self.role.write().await = NodeRole::Follower;
            *self.leader_id.write().await = new_leader;
        }
    }

    /// Announce this node as leader to all peers
    async fn announce_leadership(&self) {
        let peer_addrs: Vec<String> = {
            let peers = self.peers.read().await;
            peers.values()
                .filter(|p| p.state != NodeState::Dead)
                .map(|p| p.cluster_addr())
                .collect()
        };

        for addr in peer_addrs {
            if let Err(e) = send_leader_announcement(&addr, &self.config.node_id).await {
                info!("Failed to announce leadership to {}: {}", addr, e);
            }
        }
    }

    /// Shutdown the cluster manager
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

async fn handle_cluster_connection(
    mut stream: TcpStream,
    manager: Arc<ClusterManager>,
) -> tokio::io::Result<()> {
    loop {
        let op = match stream.read_u8().await {
            Ok(op) => op,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
            Err(e) => return Err(e),
        };

        match op {
            CLUSTER_OP_HEARTBEAT => {
                let len = stream.read_u32().await? as usize;
                let mut buf = vec![0u8; len];
                stream.read_exact(&mut buf).await?;
                let heartbeat_info = String::from_utf8_lossy(&buf).to_string();

                let parts: Vec<&str> = heartbeat_info.split(':').collect();
                let peer_id = parts.first().map(|s| s.to_string()).unwrap_or_default();
                let peer_role = match parts.get(1) {
                    Some(&"leader") => NodeRole::Leader,
                    Some(&"candidate") => NodeRole::Candidate,
                    _ => NodeRole::Follower,
                };

                manager.update_peer_heartbeat(&peer_id, peer_role).await;

                let our_role = manager.role().await;
                let ack_msg = format!("{}:{}", manager.node_id(), our_role);
                stream.write_u8(CLUSTER_OP_HEARTBEAT_ACK).await?;
                stream.write_u32(ack_msg.len() as u32).await?;
                stream.write_all(ack_msg.as_bytes()).await?;
                stream.flush().await?;
            }
            CLUSTER_OP_JOIN => {
                let len = stream.read_u32().await? as usize;
                let mut buf = vec![0u8; len];
                stream.read_exact(&mut buf).await?;
                let join_info = String::from_utf8_lossy(&buf);

                let parts: Vec<&str> = join_info.split(':').collect();
                if parts.len() >= 2 {
                    let peer_id = parts[0].to_string();
                    let cluster_port: u16 = parts[1].parse().unwrap_or(2013);
                    let client_port: u16 = parts.get(2).and_then(|p| p.parse().ok()).unwrap_or(2012);

                    let peer_addr = stream.peer_addr()?;
                    let host = peer_addr.ip().to_string();

                    let peer = PeerNode::new(peer_id.clone(), host, client_port, cluster_port);
                    manager.add_peer(peer).await;
                    info!("Peer {} joined the cluster", peer_id);
                }

                let current_role = manager.role().await;
                let sequence = manager.current_sequence();
                let ack_msg = format!("{}:{}:{}:{}",
                    manager.config().node_id,
                    manager.config().client_port,
                    current_role,
                    sequence
                );
                stream.write_u8(CLUSTER_OP_JOIN_ACK).await?;
                stream.write_u32(ack_msg.len() as u32).await?;
                stream.write_all(ack_msg.as_bytes()).await?;
                stream.flush().await?;
            }
            CLUSTER_OP_REPLICATE => {
                let len = stream.read_u32().await? as usize;
                let mut buf = vec![0u8; len];
                stream.read_exact(&mut buf).await?;

                if let Some(entry) = deserialize_replication_entry(&buf) {
                    manager.apply_replication_entry(&entry).await;
                    info!("Applied replication entry seq={} op={:?}", entry.sequence,
                        match &entry.operation {
                            ReplicationOp::Set { key, .. } => format!("SET {}", key),
                            ReplicationOp::Delete { key } => format!("DEL {}", key),
                            _ => "OTHER".to_string(),
                        }
                    );
                }

                stream.write_u8(CLUSTER_OP_REPLICATE_ACK).await?;
                stream.flush().await?;
            }
            _ => {
                info!("Unknown cluster operation: {}", op);
            }
        }
    }
}

async fn send_heartbeats(manager: &Arc<ClusterManager>) {
    let peer_addrs: Vec<String> = {
        let peers = manager.peers.read().await;
        peers.values()
            .filter(|p| p.state != NodeState::Dead)
            .map(|p| p.cluster_addr())
            .collect()
    };

    let our_role = manager.role().await;
    let heartbeat_msg = format!("{}:{}", manager.config().node_id, our_role);

    for addr in peer_addrs {
        let timeout = Duration::from_secs(2);
        match tokio::time::timeout(timeout, TcpStream::connect(&addr)).await {
            Ok(Ok(stream)) => {
                let (mut reader, mut writer) = stream.into_split();

                if let Err(e) = async {
                    writer.write_u8(CLUSTER_OP_HEARTBEAT).await?;
                    writer.write_u32(heartbeat_msg.len() as u32).await?;
                    writer.write_all(heartbeat_msg.as_bytes()).await?;
                    writer.flush().await?;

                    let _ack = reader.read_u8().await?;
                    let ack_len = reader.read_u32().await? as usize;
                    if ack_len > 0 {
                        let mut buf = vec![0u8; ack_len];
                        reader.read_exact(&mut buf).await?;
                    }

                    Ok::<(), tokio::io::Error>(())
                }.await {
                    info!("Failed to send heartbeat to {}: {}", addr, e);
                }
            }
            Ok(Err(e)) => {
                info!("Failed to connect for heartbeat to {}: {}", addr, e);
            }
            Err(_) => {
                info!("Heartbeat connection timeout to {}", addr);
            }
        }
    }
}

async fn send_leader_announcement(addr: &str, leader_id: &str) -> tokio::io::Result<()> {
    let timeout = Duration::from_secs(2);
    let stream = tokio::time::timeout(timeout, TcpStream::connect(addr)).await??;
    let (_, mut writer) = stream.into_split();

    writer.write_u8(CLUSTER_OP_LEADER_ANNOUNCE).await?;
    writer.write_u32(leader_id.len() as u32).await?;
    writer.write_all(leader_id.as_bytes()).await?;
    writer.flush().await?;

    Ok(())
}

/// Replicate an entry to all healthy peers
pub async fn replicate_to_peers(
    entry: &ReplicationEntry,
    peers: &Arc<RwLock<HashMap<String, PeerNode>>>,
    timeout: Duration,
) -> usize {
    let peer_addrs: Vec<String> = {
        let peers = peers.read().await;
        peers.values()
            .filter(|p| p.state == NodeState::Healthy)
            .map(|p| p.cluster_addr())
            .collect()
    };

    let mut success_count = 0;

    for addr in peer_addrs {
        if replicate_to_peer(&addr, entry, timeout).await.is_ok() {
            success_count += 1;
        }
    }

    success_count
}

async fn replicate_to_peer(
    addr: &str,
    entry: &ReplicationEntry,
    timeout: Duration,
) -> tokio::io::Result<()> {
    let stream = tokio::time::timeout(timeout, TcpStream::connect(addr)).await??;
    let (mut reader, mut writer) = stream.into_split();

    let data = serialize_replication_entry(entry);

    writer.write_u8(CLUSTER_OP_REPLICATE).await?;
    writer.write_u32(data.len() as u32).await?;
    writer.write_all(&data).await?;
    writer.flush().await?;

    let ack = tokio::time::timeout(timeout, reader.read_u8()).await??;
    if ack != CLUSTER_OP_REPLICATE_ACK {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid replication ack"));
    }

    Ok(())
}

pub fn serialize_replication_entry(entry: &ReplicationEntry) -> Vec<u8> {
    let mut data = Vec::new();

    data.extend_from_slice(&entry.sequence.to_be_bytes());
    data.extend_from_slice(&entry.timestamp.to_be_bytes());

    let origin_bytes = entry.origin_node.as_bytes();
    data.extend_from_slice(&(origin_bytes.len() as u32).to_be_bytes());
    data.extend_from_slice(origin_bytes);

    let db_bytes = entry.database_id.as_bytes();
    data.extend_from_slice(&(db_bytes.len() as u32).to_be_bytes());
    data.extend_from_slice(db_bytes);

    match &entry.operation {
        ReplicationOp::Set { key, value } => {
            data.push(1);
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
            data.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(value_bytes);
        }
        ReplicationOp::Delete { key } => {
            data.push(2);
            let key_bytes = key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
        }
        ReplicationOp::Incr { key } => {
            data.push(3);
            let key_bytes = key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
        }
        ReplicationOp::Decr { key } => {
            data.push(4);
            let key_bytes = key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
        }
        ReplicationOp::IncrBy { key, delta } => {
            data.push(5);
            let key_bytes = key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
            data.extend_from_slice(&delta.to_be_bytes());
        }
        _ => {
            // Unsupported operation for serialization
            data.push(0);
        }
    }

    data
}

pub fn deserialize_replication_entry(data: &[u8]) -> Option<ReplicationEntry> {
    if data.len() < 21 {
        return None;
    }

    let mut offset = 0;

    let sequence = u64::from_be_bytes(data[offset..offset+8].try_into().ok()?);
    offset += 8;

    let timestamp = u64::from_be_bytes(data[offset..offset+8].try_into().ok()?);
    offset += 8;

    let origin_len = u32::from_be_bytes(data[offset..offset+4].try_into().ok()?) as usize;
    offset += 4;
    let origin_node = String::from_utf8_lossy(&data[offset..offset+origin_len]).to_string();
    offset += origin_len;

    let db_len = u32::from_be_bytes(data[offset..offset+4].try_into().ok()?) as usize;
    offset += 4;
    let database_id = String::from_utf8_lossy(&data[offset..offset+db_len]).to_string();
    offset += db_len;

    let op_type = data.get(offset)?;
    offset += 1;

    let operation = match op_type {
        1 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            offset += key_len;

            let value_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let value = String::from_utf8_lossy(data.get(offset..offset+value_len)?).to_string();

            ReplicationOp::Set { key, value }
        }
        2 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();

            ReplicationOp::Delete { key }
        }
        3 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            ReplicationOp::Incr { key }
        }
        4 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            ReplicationOp::Decr { key }
        }
        5 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            offset += key_len;
            let delta = i64::from_be_bytes(data.get(offset..offset+8)?.try_into().ok()?);
            ReplicationOp::IncrBy { key, delta }
        }
        _ => return None,
    };

    Some(ReplicationEntry {
        sequence,
        database_id,
        operation,
        timestamp,
        origin_node,
    })
}
