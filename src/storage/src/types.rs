use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Data types supported by the storage
#[derive(Clone, Debug)]
pub enum DataType {
    String(String),
    /// LZ4-compressed string for large values (saves memory)
    CompressedString(Vec<u8>),
    Counter(i64),
    List(VecDeque<String>),
    Set(HashSet<String>),
    /// CRDT Counter for distributed increment/decrement operations
    CRDTCounter(crate::crdt::PNCounter),
}

impl DataType {
    /// Get the type name as a string
    pub fn type_name(&self) -> &'static str {
        match self {
            DataType::String(_) => "string",
            DataType::CompressedString(_) => "string", // Appears as string to users
            DataType::Counter(_) => "counter",
            DataType::List(_) => "list",
            DataType::Set(_) => "set",
            DataType::CRDTCounter(_) => "crdt_counter",
        }
    }

    /// Try to get as string value (decompresses if needed)
    pub fn as_string(&self) -> Option<String> {
        match self {
            DataType::String(s) => Some(s.clone()),
            DataType::CompressedString(compressed) => {
                // Decompress the value
                match crate::compression::decompress(compressed) {
                    Ok(bytes) => String::from_utf8(bytes).ok(),
                    Err(_) => None,
                }
            }
            DataType::Counter(n) => Some(n.to_string()),
            _ => None,
        }
    }

    /// Create a string DataType, compressing if the value is large
    pub fn new_string(value: String) -> Self {
        if let Some(compressed) = crate::compression::compress_if_needed(value.as_bytes()) {
            DataType::CompressedString(compressed)
        } else {
            DataType::String(value)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum DatabaseId {
    #[default]
    Default,
    Custom(String),
}

impl DatabaseId {
    /// Convert to string representation for WAL
    pub fn to_wal_string(&self) -> String {
        match self {
            DatabaseId::Default => String::new(),
            DatabaseId::Custom(name) => name.clone(),
        }
    }

    /// Create from WAL string representation
    pub fn from_wal_string(s: &str) -> Self {
        if s.is_empty() {
            DatabaseId::Default
        } else {
            DatabaseId::Custom(s.to_string())
        }
    }
}

/// Value with optional TTL and LWW timestamp
/// Note: origin_node was removed to save ~32 bytes per key (1M keys = ~30MB savings)
#[derive(Clone, Debug)]
pub struct StoredValue {
    pub data: DataType,
    pub expires_at: Option<Instant>,
    /// Timestamp for Last-Write-Wins conflict resolution (Unix ms)
    pub timestamp: u64,
}

/// Get current timestamp in milliseconds
pub fn current_timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl StoredValue {
    pub fn new(value: String) -> Self {
        StoredValue {
            data: DataType::new_string(value),
            expires_at: None,
            timestamp: current_timestamp_ms(),
        }
    }

    pub fn new_with_timestamp(value: String, timestamp: u64) -> Self {
        StoredValue {
            data: DataType::new_string(value),
            expires_at: None,
            timestamp,
        }
    }

    pub fn with_ttl(value: String, ttl: Duration) -> Self {
        StoredValue {
            data: DataType::new_string(value),
            expires_at: Some(Instant::now() + ttl),
            timestamp: current_timestamp_ms(),
        }
    }

    pub fn with_ttl_and_timestamp(value: String, ttl: Duration, timestamp: u64) -> Self {
        StoredValue {
            data: DataType::new_string(value),
            expires_at: Some(Instant::now() + ttl),
            timestamp,
        }
    }

    pub fn new_counter(value: i64) -> Self {
        StoredValue {
            data: DataType::Counter(value),
            expires_at: None,
            timestamp: current_timestamp_ms(),
        }
    }

    pub fn new_list() -> Self {
        StoredValue {
            data: DataType::List(VecDeque::new()),
            expires_at: None,
            timestamp: current_timestamp_ms(),
        }
    }

    pub fn new_set() -> Self {
        StoredValue {
            data: DataType::Set(HashSet::new()),
            expires_at: None,
            timestamp: current_timestamp_ms(),
        }
    }

    /// Get the timestamp
    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Update timestamp
    pub fn update_timestamp(&mut self, timestamp: u64) {
        self.timestamp = timestamp;
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Instant::now() > expires_at
        } else {
            false
        }
    }

    /// Get remaining TTL in seconds, None if no TTL set
    pub fn ttl_seconds(&self) -> Option<u64> {
        self.expires_at.map(|expires_at| {
            let now = Instant::now();
            if now > expires_at {
                0
            } else {
                (expires_at - now).as_secs()
            }
        })
    }

    /// Get value as string (for strings and counters)
    pub fn value(&self) -> Option<String> {
        self.data.as_string()
    }

    /// Get the data type name
    pub fn type_name(&self) -> &'static str {
        self.data.type_name()
    }
}


