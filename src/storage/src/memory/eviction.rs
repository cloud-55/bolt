use std::fmt;

/// Policy for evicting keys when memory limit is reached
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Don't evict - return error when memory is full
    NoEviction,

    /// Evict least recently used key (any key)
    AllKeysLRU,

    /// Evict a random key (faster but less optimal)
    AllKeysRandom,

    /// Evict least recently used key that has a TTL
    VolatileLRU,

    /// Evict key with shortest remaining TTL
    VolatileTTL,
}

impl EvictionPolicy {
    /// Parse from string (case-insensitive)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().replace(['-', '_'], "").as_str() {
            "noeviction" | "none" => Some(EvictionPolicy::NoEviction),
            "allkeyslru" | "lru" => Some(EvictionPolicy::AllKeysLRU),
            "allkeysrandom" | "random" => Some(EvictionPolicy::AllKeysRandom),
            "volatilelru" => Some(EvictionPolicy::VolatileLRU),
            "volatilettl" | "ttl" => Some(EvictionPolicy::VolatileTTL),
            _ => None,
        }
    }

    /// Get the default policy
    pub fn default_policy() -> Self {
        EvictionPolicy::AllKeysLRU
    }

    /// Check if this policy requires LRU tracking
    pub fn requires_lru(&self) -> bool {
        matches!(self, EvictionPolicy::AllKeysLRU | EvictionPolicy::VolatileLRU)
    }

    /// Check if this policy can evict any key
    pub fn can_evict(&self) -> bool {
        !matches!(self, EvictionPolicy::NoEviction)
    }

    /// Get description for logging
    pub fn description(&self) -> &'static str {
        match self {
            EvictionPolicy::NoEviction => "no eviction (returns error when full)",
            EvictionPolicy::AllKeysLRU => "evict least recently used keys",
            EvictionPolicy::AllKeysRandom => "evict random keys",
            EvictionPolicy::VolatileLRU => "evict LRU keys with TTL only",
            EvictionPolicy::VolatileTTL => "evict keys closest to expiration",
        }
    }
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

impl fmt::Display for EvictionPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EvictionPolicy::NoEviction => write!(f, "noeviction"),
            EvictionPolicy::AllKeysLRU => write!(f, "allkeys-lru"),
            EvictionPolicy::AllKeysRandom => write!(f, "allkeys-random"),
            EvictionPolicy::VolatileLRU => write!(f, "volatile-lru"),
            EvictionPolicy::VolatileTTL => write!(f, "volatile-ttl"),
        }
    }
}

/// Result of an eviction attempt
#[derive(Debug)]
pub enum EvictionResult {
    /// Successfully evicted a key
    Evicted(String),
    /// No suitable key found for eviction (volatile policies with no TTL keys)
    NoCandidate,
    /// Eviction is disabled (NoEviction policy)
    Disabled,
}

impl EvictionResult {
    pub fn is_success(&self) -> bool {
        matches!(self, EvictionResult::Evicted(_))
    }

    pub fn evicted_key(&self) -> Option<&str> {
        match self {
            EvictionResult::Evicted(key) => Some(key),
            _ => None,
        }
    }
}

/// Error returned when storage is full and NoEviction policy is set
#[derive(Debug, Clone)]
pub struct OutOfMemoryError {
    pub current_bytes: usize,
    pub max_bytes: usize,
    pub requested_bytes: usize,
}

impl fmt::Display for OutOfMemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OOM: memory limit reached ({} / {}), cannot allocate {} more bytes",
            super::format_bytes(self.current_bytes),
            super::format_bytes(self.max_bytes),
            super::format_bytes(self.requested_bytes)
        )
    }
}

impl std::error::Error for OutOfMemoryError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_from_str() {
        assert_eq!(
            EvictionPolicy::from_str("allkeys-lru"),
            Some(EvictionPolicy::AllKeysLRU)
        );
        assert_eq!(
            EvictionPolicy::from_str("ALLKEYS_LRU"),
            Some(EvictionPolicy::AllKeysLRU)
        );
        assert_eq!(
            EvictionPolicy::from_str("lru"),
            Some(EvictionPolicy::AllKeysLRU)
        );
        assert_eq!(
            EvictionPolicy::from_str("noeviction"),
            Some(EvictionPolicy::NoEviction)
        );
        assert_eq!(EvictionPolicy::from_str("invalid"), None);
    }

    #[test]
    fn test_policy_display() {
        assert_eq!(EvictionPolicy::AllKeysLRU.to_string(), "allkeys-lru");
        assert_eq!(EvictionPolicy::NoEviction.to_string(), "noeviction");
    }

    #[test]
    fn test_requires_lru() {
        assert!(EvictionPolicy::AllKeysLRU.requires_lru());
        assert!(EvictionPolicy::VolatileLRU.requires_lru());
        assert!(!EvictionPolicy::AllKeysRandom.requires_lru());
        assert!(!EvictionPolicy::NoEviction.requires_lru());
    }
}
