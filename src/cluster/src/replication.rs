/// Replication entry to propagate to peers
#[derive(Debug, Clone)]
pub struct ReplicationEntry {
    pub sequence: u64,
    pub database_id: String,
    pub operation: ReplicationOp,
    /// Timestamp for LWW conflict resolution (Unix ms)
    pub timestamp: u64,
    /// Origin node that created this entry
    pub origin_node: String,
}

#[derive(Debug, Clone)]
pub enum ReplicationOp {
    Set { key: String, value: String },
    SetEx { key: String, value: String, ttl_seconds: u64 },
    Delete { key: String },
    Incr { key: String },
    Decr { key: String },
    IncrBy { key: String, delta: i64 },
    LPush { key: String, values: Vec<String> },
    RPush { key: String, values: Vec<String> },
    LPop { key: String },
    RPop { key: String },
    SAdd { key: String, members: Vec<String> },
    SRem { key: String, members: Vec<String> },
    MSet { pairs: Vec<(String, String)> },
    MDel { keys: Vec<String> },
    /// CRDT Counter merge - contains the full counter state (P and N counters)
    CRDTMerge {
        key: String,
        /// Serialized PNCounter state (JSON)
        counter_state: String,
    },
}

