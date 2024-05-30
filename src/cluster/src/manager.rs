use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex, broadcast, mpsc};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use log::info;
use storage::{Storage, DatabaseId, current_timestamp_ms};

use crate::config::ClusterConfig;
use crate::types::*;
use crate::replication::{ReplicationEntry, ReplicationOp};

/// Cluster manager handles all cluster operations
pub struct ClusterManager {
    config: ClusterConfig,
    role: Arc<RwLock<NodeRole>>,
    leader_id: Arc<RwLock<Option<String>>>,
    peers: Arc<RwLock<HashMap<String, PeerNode>>>,
    /// Persistent connections to peers (addr -> connection)
    peer_connections: Arc<Mutex<HashMap<String, PeerConnection>>>,
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
            peer_connections: Arc::new(Mutex::new(HashMap::new())),
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

    /// Apply a replication entry to local storage using LWW (Last-Write-Wins)
    pub fn apply_replication_entry(&self, entry: &ReplicationEntry) -> impl std::future::Future<Output = ()> + Send + '_ {
        let db_id = DatabaseId::from_wal_string(&entry.database_id);
        let entry_sequence = entry.sequence;
        let operation = entry.operation.clone();
        let storage = &self.storage;
        let last_applied = &self.last_applied_sequence;
        let incoming_timestamp = entry.timestamp;

        async move {
            match operation {
                ReplicationOp::Set { key, value } => {
                    storage.set_if_newer(db_id, &key, &value, None, incoming_timestamp).await;
                }
                ReplicationOp::SetEx { key, value, ttl_seconds } => {
                    let ttl = Some(std::time::Duration::from_secs(ttl_seconds));
                    storage.set_if_newer(db_id, &key, &value, ttl, incoming_timestamp).await;
                }
                ReplicationOp::Delete { key } => {
                    storage.delete_if_newer(db_id, &key, incoming_timestamp).await;
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
                ReplicationOp::LPush { key, values } => {
                    storage.lpush(db_id, &key, &values).await;
                }
                ReplicationOp::RPush { key, values } => {
                    storage.rpush(db_id, &key, &values).await;
                }
                ReplicationOp::LPop { key } => {
                    storage.lpop(db_id, &key).await;
                }
                ReplicationOp::RPop { key } => {
                    storage.rpop(db_id, &key).await;
                }
                ReplicationOp::SAdd { key, members } => {
                    storage.sadd(db_id, &key, &members).await;
                }
                ReplicationOp::SRem { key, members } => {
                    storage.srem(db_id, &key, &members).await;
                }
                ReplicationOp::MSet { pairs } => {
                    storage.mset_if_newer(db_id, &pairs, incoming_timestamp).await;
                }
                ReplicationOp::MDel { keys } => {
                    for key in keys {
                        storage.delete_if_newer(db_id.clone(), &key, incoming_timestamp).await;
                    }
                }
                _ => {
                    info!("Unsupported replication operation: {:?}", operation);
                }
            }

            last_applied.store(entry_sequence, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Start the cluster manager background tasks
    pub async fn start(self: &Arc<Self>) -> tokio::io::Result<()> {
        let cluster_addr = format!("0.0.0.0:{}", self.config.cluster_port);
        info!("Starting cluster manager on {} as node {}", cluster_addr, self.config.node_id);

        let listener = tokio::net::TcpListener::bind(&cluster_addr).await?;

        let manager = Arc::clone(self);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Spawn cluster listener
        let manager_clone = Arc::clone(&manager);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        if let Ok((stream, _addr)) = result {
                            let manager = Arc::clone(&manager_clone);
                            tokio::spawn(async move {
                                if let Err(e) = handle_cluster_connection(stream, manager).await {
                                    match e.kind() {
                                        std::io::ErrorKind::BrokenPipe |
                                        std::io::ErrorKind::ConnectionReset |
                                        std::io::ErrorKind::UnexpectedEof => {}
                                        _ => info!("Cluster connection error: {}", e),
                                    }
                                }
                            });
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Cluster listener shutting down");
                        break;
                    }
                }
            }
        });

        // Spawn heartbeat task
        let manager_clone = Arc::clone(&manager);
        let mut heartbeat_shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(manager_clone.config.heartbeat_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        send_heartbeats(&manager_clone).await;
                    }
                    _ = heartbeat_shutdown_rx.recv() => {
                        info!("Heartbeat task shutting down");
                        break;
                    }
                }
            }
        });

        // Spawn health check and leader election task
        let manager_clone = Arc::clone(&manager);
        let mut health_shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(manager_clone.config.heartbeat_interval * 2);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        check_health_and_election(&manager_clone).await;
                    }
                    _ = health_shutdown_rx.recv() => {
                        info!("Health check task shutting down");
                        break;
                    }
                }
            }
        });

        // Spawn replication sender task (for leader)
        let manager_clone = Arc::clone(&manager);
        let mut repl_shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if manager_clone.is_leader().await {
                            // Replication is sent immediately in replicate_to_peers
                        }
                    }
                    _ = repl_shutdown_rx.recv() => {
                        info!("Replication sender shutting down");
                        break;
                    }
                }
            }
        });

        // Connect to initial peers
        info!("Attempting to connect to {} configured peers", self.config.peers.len());
        for peer_spec in &self.config.peers {
            if let Err(e) = self.connect_to_peer(peer_spec).await {
                info!("Failed to connect to peer {}: {}", peer_spec, e);
            }
        }

        let peer_count = self.peers.read().await.len();
        info!("Connected to {} peers before election", peer_count);

        tokio::time::sleep(Duration::from_millis(1000)).await;

        let peer_count_after = self.peers.read().await.len();
        info!("Starting election with {} known peers", peer_count_after);
        self.perform_leader_election().await;

        Ok(())
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
                let peer_sequence: u64 = info_parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);

                let mut peer = PeerNode::new(peer_id.clone(), host.to_string(), client_port, cluster_port);
                peer.role = peer_role;
                self.add_peer(peer).await;

                let our_sequence = self.last_applied_sequence.load(std::sync::atomic::Ordering::SeqCst);
                if peer_sequence > our_sequence {
                    info!("Peer {} has newer data (seq {} vs {}), requesting sync", peer_id, peer_sequence, our_sequence);
                    if let Err(e) = self.request_sync_from(&connect_addr).await {
                        info!("Failed to sync from {}: {}", connect_addr, e);
                    }
                }

                info!("Connected to peer: {} (role: {})", peer_id, peer_role);
            }
        }

        Ok(())
    }

    /// Request full sync from a peer (for new nodes joining)
    async fn request_sync_from(&self, addr: &str) -> tokio::io::Result<()> {
        let stream = TcpStream::connect(addr).await?;
        let (mut reader, mut writer) = stream.into_split();

        writer.write_u8(CLUSTER_OP_SYNC_REQUEST).await?;
        writer.write_u32(self.config.node_id.len() as u32).await?;
        writer.write_all(self.config.node_id.as_bytes()).await?;
        writer.flush().await?;

        let mut total_entries = 0u64;
        loop {
            let op = reader.read_u8().await?;

            if op == CLUSTER_OP_SYNC_COMPLETE {
                info!("Sync complete, received {} entries", total_entries);
                break;
            }

            if op == CLUSTER_OP_SYNC_DATA {
                let len = reader.read_u32().await? as usize;
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf).await?;

                if let Some(entry) = deserialize_replication_entry(&buf) {
                    self.apply_replication_entry(&entry).await;
                    total_entries += 1;
                }
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

    /// Get cluster status as JSON
    pub async fn status_json(&self) -> String {
        let role = self.role.read().await;
        let leader_id = self.leader_id.read().await;
        let peers = self.peers.read().await;
        let sequence = self.current_sequence();

        let peer_list: Vec<String> = peers.values().map(|p| {
            format!(r#"{{"id":"{}","host":"{}","port":{},"state":"{:?}","role":"{}"}}"#,
                p.id, p.host, p.port, p.state, p.role)
        }).collect();

        format!(
            r#"{{"node_id":"{}","role":"{}","leader":"{}","sequence":{},"peers":[{}],"peer_count":{}}}"#,
            self.config.node_id,
            role,
            leader_id.as_deref().unwrap_or("unknown"),
            sequence,
            peer_list.join(","),
            peers.len()
        )
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
            CLUSTER_OP_SYNC_REQUEST => {
                let len = stream.read_u32().await? as usize;
                let mut buf = vec![0u8; len];
                stream.read_exact(&mut buf).await?;
                let requester_id = String::from_utf8_lossy(&buf).to_string();

                info!("Sync request from {}", requester_id);

                let keys = manager.storage().keys(DatabaseId::Default, "*").await;
                for key in keys {
                    if let Some(value) = manager.storage().get(DatabaseId::Default, &key).await {
                        let timestamp = manager.storage().get_timestamp(&DatabaseId::Default, &key).await
                            .unwrap_or_else(current_timestamp_ms);
                        let entry = ReplicationEntry {
                            sequence: 0,
                            database_id: String::new(),
                            operation: ReplicationOp::Set { key: key.clone(), value },
                            timestamp,
                            origin_node: manager.config().node_id.clone(),
                        };
                        let data = serialize_replication_entry(&entry);
                        stream.write_u8(CLUSTER_OP_SYNC_DATA).await?;
                        stream.write_u32(data.len() as u32).await?;
                        stream.write_all(&data).await?;
                    }
                }

                stream.write_u8(CLUSTER_OP_SYNC_COMPLETE).await?;
                stream.flush().await?;
                info!("Sync complete to {}", requester_id);
            }
            CLUSTER_OP_LEADER_ANNOUNCE => {
                let len = stream.read_u32().await? as usize;
                let mut buf = vec![0u8; len];
                stream.read_exact(&mut buf).await?;
                let new_leader_id = String::from_utf8_lossy(&buf).to_string();

                info!("Leader announcement received: {}", new_leader_id);

                {
                    let mut leader = manager.leader_id.write().await;
                    *leader = Some(new_leader_id.clone());
                }

                if manager.is_leader().await && new_leader_id != manager.config().node_id {
                    info!("Stepping down as leader, new leader is {}", new_leader_id);
                    manager.set_role(NodeRole::Follower).await;
                }

                manager.update_peer_heartbeat(&new_leader_id, NodeRole::Leader).await;
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
        if let Err(e) = send_heartbeat_with_pool(manager, &addr, &heartbeat_msg).await {
            match e.kind() {
                std::io::ErrorKind::BrokenPipe |
                std::io::ErrorKind::ConnectionReset |
                std::io::ErrorKind::ConnectionRefused => {
                    let mut connections = manager.peer_connections.lock().await;
                    connections.remove(&addr);
                }
                _ => {
                    info!("Failed to send heartbeat to {}: {}", addr, e);
                    let mut connections = manager.peer_connections.lock().await;
                    connections.remove(&addr);
                }
            }
        }
    }
}

async fn send_heartbeat_with_pool(
    manager: &Arc<ClusterManager>,
    addr: &str,
    heartbeat_msg: &str
) -> tokio::io::Result<()> {
    let timeout = Duration::from_secs(2);

    let mut connections = manager.peer_connections.lock().await;

    if !connections.contains_key(addr) {
        let stream = match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Connection timeout"
            )),
        };

        let _ = stream.set_nodelay(true);

        let (reader, writer) = stream.into_split();
        connections.insert(addr.to_string(), PeerConnection {
            reader,
            writer,
            last_used: Instant::now(),
        });
    }

    let conn = connections.get_mut(addr).unwrap();
    conn.last_used = Instant::now();

    conn.writer.write_u8(CLUSTER_OP_HEARTBEAT).await?;
    conn.writer.write_u32(heartbeat_msg.len() as u32).await?;
    conn.writer.write_all(heartbeat_msg.as_bytes()).await?;
    conn.writer.flush().await?;

    let ack = match tokio::time::timeout(timeout, conn.reader.read_u8()).await {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => {
            connections.remove(addr);
            return Err(e);
        }
        Err(_) => {
            connections.remove(addr);
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Ack timeout"
            ));
        }
    };

    if ack != CLUSTER_OP_HEARTBEAT_ACK {
        connections.remove(addr);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid heartbeat ack"));
    }

    let ack_len = match tokio::time::timeout(timeout, conn.reader.read_u32()).await {
        Ok(Ok(len)) => len as usize,
        Ok(Err(e)) => {
            connections.remove(addr);
            return Err(e);
        }
        Err(_) => {
            connections.remove(addr);
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Ack len timeout"));
        }
    };

    if ack_len > 0 {
        let mut buf = vec![0u8; ack_len];
        if let Err(e) = tokio::time::timeout(timeout, conn.reader.read_exact(&mut buf)).await {
            connections.remove(addr);
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, format!("Ack body timeout: {:?}", e)));
        }
    }

    Ok(())
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

async fn check_health_and_election(manager: &Arc<ClusterManager>) {
    let mut peers = manager.peers.write().await;
    let now = Instant::now();
    let mut leader_dead = false;
    let current_leader = manager.leader_id.read().await.clone();

    for peer in peers.values_mut() {
        let elapsed = now.duration_since(peer.last_heartbeat);

        if elapsed > manager.config().heartbeat_timeout * 2 {
            if peer.state != NodeState::Dead {
                info!("Peer {} marked as dead (no heartbeat for {:?})", peer.id, elapsed);
                peer.state = NodeState::Dead;

                if Some(&peer.id) == current_leader.as_ref() {
                    leader_dead = true;
                }
            }
        } else if elapsed > manager.config().heartbeat_timeout {
            if peer.state == NodeState::Healthy {
                info!("Peer {} marked as suspect", peer.id);
                peer.state = NodeState::Suspect;
            }
        }
    }

    drop(peers);

    if leader_dead {
        info!("Leader {} is dead, triggering re-election", current_leader.as_deref().unwrap_or("unknown"));
        manager.perform_leader_election().await;
    }
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
        ReplicationOp::SetEx { key, value, ttl_seconds } => {
            data.push(10);
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
            data.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(value_bytes);
            data.extend_from_slice(&ttl_seconds.to_be_bytes());
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
        ReplicationOp::LPush { key, values } => {
            data.push(6);
            let key_bytes = key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
            let joined = values.join("\n");
            let values_bytes = joined.as_bytes();
            data.extend_from_slice(&(values_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(values_bytes);
        }
        ReplicationOp::RPush { key, values } => {
            data.push(7);
            let key_bytes = key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
            let joined = values.join("\n");
            let values_bytes = joined.as_bytes();
            data.extend_from_slice(&(values_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(values_bytes);
        }
        ReplicationOp::LPop { key } => {
            data.push(8);
            let key_bytes = key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
        }
        ReplicationOp::RPop { key } => {
            data.push(9);
            let key_bytes = key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
        }
        ReplicationOp::SAdd { key, members } => {
            data.push(11);
            let key_bytes = key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
            let joined = members.join("\n");
            let members_bytes = joined.as_bytes();
            data.extend_from_slice(&(members_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(members_bytes);
        }
        ReplicationOp::SRem { key, members } => {
            data.push(12);
            let key_bytes = key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(key_bytes);
            let joined = members.join("\n");
            let members_bytes = joined.as_bytes();
            data.extend_from_slice(&(members_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(members_bytes);
        }
        ReplicationOp::MSet { pairs } => {
            data.push(13);
            data.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
            for (key, value) in pairs {
                let key_bytes = key.as_bytes();
                let value_bytes = value.as_bytes();
                data.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
                data.extend_from_slice(key_bytes);
                data.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
                data.extend_from_slice(value_bytes);
            }
        }
        ReplicationOp::MDel { keys } => {
            data.push(14);
            let joined = keys.join("\n");
            let keys_bytes = joined.as_bytes();
            data.extend_from_slice(&(keys_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(keys_bytes);
        }
        _ => {
            // CRDTMerge not yet supported in serialization
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
        6 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            offset += key_len;
            let values_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let values_str = String::from_utf8_lossy(data.get(offset..offset+values_len)?).to_string();
            let values: Vec<String> = values_str.split('\n').map(|s| s.to_string()).collect();
            ReplicationOp::LPush { key, values }
        }
        7 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            offset += key_len;
            let values_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let values_str = String::from_utf8_lossy(data.get(offset..offset+values_len)?).to_string();
            let values: Vec<String> = values_str.split('\n').map(|s| s.to_string()).collect();
            ReplicationOp::RPush { key, values }
        }
        8 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            ReplicationOp::LPop { key }
        }
        9 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            ReplicationOp::RPop { key }
        }
        10 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            offset += key_len;
            let value_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let value = String::from_utf8_lossy(data.get(offset..offset+value_len)?).to_string();
            offset += value_len;
            let ttl_seconds = u64::from_be_bytes(data.get(offset..offset+8)?.try_into().ok()?);
            ReplicationOp::SetEx { key, value, ttl_seconds }
        }
        11 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            offset += key_len;
            let members_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let members_str = String::from_utf8_lossy(data.get(offset..offset+members_len)?).to_string();
            let members: Vec<String> = members_str.split('\n').map(|s| s.to_string()).collect();
            ReplicationOp::SAdd { key, members }
        }
        12 => {
            let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
            offset += key_len;
            let members_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let members_str = String::from_utf8_lossy(data.get(offset..offset+members_len)?).to_string();
            let members: Vec<String> = members_str.split('\n').map(|s| s.to_string()).collect();
            ReplicationOp::SRem { key, members }
        }
        13 => {
            let num_pairs = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let mut pairs = Vec::with_capacity(num_pairs);
            for _ in 0..num_pairs {
                let key_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
                offset += 4;
                let key = String::from_utf8_lossy(data.get(offset..offset+key_len)?).to_string();
                offset += key_len;
                let value_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
                offset += 4;
                let value = String::from_utf8_lossy(data.get(offset..offset+value_len)?).to_string();
                offset += value_len;
                pairs.push((key, value));
            }
            ReplicationOp::MSet { pairs }
        }
        14 => {
            let keys_len = u32::from_be_bytes(data.get(offset..offset+4)?.try_into().ok()?) as usize;
            offset += 4;
            let keys_str = String::from_utf8_lossy(data.get(offset..offset+keys_len)?).to_string();
            let keys: Vec<String> = keys_str.split('\n').map(|s| s.to_string()).collect();
            ReplicationOp::MDel { keys }
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
