use std::sync::atomic::{AtomicUsize, Ordering};
use crate::types::StoredValue;

/// Tracks memory usage with atomic operations for thread safety
pub struct MemoryTracker {
    /// Current memory usage in bytes
    current_bytes: AtomicUsize,
    /// Maximum allowed memory in bytes (0 = unlimited)
    max_bytes: usize,
    /// Warning threshold (percentage, e.g., 90 = 90%)
    warning_threshold: usize,
    /// Whether warning has been logged (to avoid spam)
    warning_logged: AtomicUsize,
}

/// Estimated overhead per HashMap entry (key String + value + HashMap node)
const ENTRY_OVERHEAD: usize = 64;

/// Estimated overhead per String (capacity, length, pointer)
const STRING_OVERHEAD: usize = 24;

impl MemoryTracker {
    /// Create a new memory tracker
    pub fn new(max_bytes: usize) -> Self {
        MemoryTracker {
            current_bytes: AtomicUsize::new(0),
            max_bytes,
            warning_threshold: 90,
            warning_logged: AtomicUsize::new(0),
        }
    }

    /// Create an unlimited memory tracker (for backwards compatibility)
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// Set the warning threshold percentage
    pub fn with_warning_threshold(mut self, threshold: usize) -> Self {
        self.warning_threshold = threshold.min(100);
        self
    }

    /// Get current memory usage in bytes
    pub fn current(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Get maximum memory limit in bytes
    pub fn max(&self) -> usize {
        self.max_bytes
    }

    /// Check if memory tracking is enabled (max > 0)
    pub fn is_limited(&self) -> bool {
        self.max_bytes > 0
    }

    /// Get current usage percentage
    pub fn usage_percent(&self) -> f64 {
        if self.max_bytes == 0 {
            0.0
        } else {
            (self.current() as f64 / self.max_bytes as f64) * 100.0
        }
    }

    /// Check if adding bytes would exceed the limit
    pub fn would_exceed(&self, additional_bytes: usize) -> bool {
        if self.max_bytes == 0 {
            return false; // Unlimited
        }
        self.current() + additional_bytes > self.max_bytes
    }

    /// Add bytes to the current usage
    pub fn add(&self, bytes: usize) {
        let new_value = self.current_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.check_warning(new_value);
    }

    /// Subtract bytes from the current usage
    pub fn subtract(&self, bytes: usize) {
        self.current_bytes.fetch_sub(bytes.min(self.current()), Ordering::Relaxed);
        // Reset warning flag when we go below threshold
        if self.usage_percent() < self.warning_threshold as f64 {
            self.warning_logged.store(0, Ordering::Relaxed);
        }
    }

    /// Check and log warning if threshold exceeded
    fn check_warning(&self, current: usize) {
        if self.max_bytes == 0 {
            return;
        }

        let percent = (current as f64 / self.max_bytes as f64) * 100.0;
        if percent >= self.warning_threshold as f64 {
            // Only log once until we go below threshold
            if self.warning_logged.swap(1, Ordering::Relaxed) == 0 {
                log::warn!(
                    "Memory usage at {:.1}% ({} / {})",
                    percent,
                    super::format_bytes(current),
                    super::format_bytes(self.max_bytes)
                );
            }
        }
    }

    /// Estimate the size of a key-value entry
    pub fn estimate_entry_size(key: &str, value: &str) -> usize {
        let key_size = key.len() + STRING_OVERHEAD;
        let value_size = value.len() + STRING_OVERHEAD;
        key_size + value_size + ENTRY_OVERHEAD
    }

    /// Estimate the size of a stored value
    pub fn estimate_stored_value_size(key: &str, value: &StoredValue) -> usize {
        use crate::types::DataType;

        let key_size = key.len() + STRING_OVERHEAD;

        let value_size = match &value.data {
            DataType::String(s) => s.len() + STRING_OVERHEAD,
            DataType::CompressedString(bytes) => bytes.len() + 24, // Vec overhead
            DataType::List(v) => {
                v.iter().map(|s| s.len() + STRING_OVERHEAD).sum::<usize>() + 24 // VecDeque overhead
            }
            DataType::Set(s) => {
                s.iter().map(|item| item.len() + STRING_OVERHEAD).sum::<usize>() + 48 // HashSet overhead
            }
            DataType::Counter(_) => 8, // i64
            DataType::CRDTCounter(pn) => {
                // PNCounter has two HashMaps
                std::mem::size_of_val(pn) + 256 // Rough estimate
            }
        };

        key_size + value_size + ENTRY_OVERHEAD
    }

    /// Get memory stats as a struct
    pub fn stats(&self) -> MemoryStats {
        MemoryStats {
            current_bytes: self.current(),
            max_bytes: self.max_bytes,
            usage_percent: self.usage_percent(),
            is_limited: self.is_limited(),
        }
    }
}

/// Memory statistics
#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub current_bytes: usize,
    pub max_bytes: usize,
    pub usage_percent: f64,
    pub is_limited: bool,
}

impl MemoryStats {
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"used":"{}","max":"{}","usage_percent":{:.2},"limited":{}}}"#,
            super::format_bytes(self.current_bytes),
            if self.is_limited {
                super::format_bytes(self.max_bytes)
            } else {
                "unlimited".to_string()
            },
            self.usage_percent,
            self.is_limited
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_tracker_basic() {
        let tracker = MemoryTracker::new(1000);

        assert_eq!(tracker.current(), 0);
        assert_eq!(tracker.max(), 1000);
        assert!(tracker.is_limited());

        tracker.add(500);
        assert_eq!(tracker.current(), 500);
        assert!(!tracker.would_exceed(400));
        assert!(tracker.would_exceed(600));

        tracker.subtract(200);
        assert_eq!(tracker.current(), 300);
    }

    #[test]
    fn test_unlimited_tracker() {
        let tracker = MemoryTracker::unlimited();

        assert!(!tracker.is_limited());
        assert!(!tracker.would_exceed(usize::MAX / 2));
    }

    #[test]
    fn test_estimate_entry_size() {
        let size = MemoryTracker::estimate_entry_size("key", "value");
        assert!(size > "key".len() + "value".len());
        assert!(size < 200); // Reasonable upper bound
    }
}
