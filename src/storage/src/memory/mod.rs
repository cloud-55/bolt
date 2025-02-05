mod tracker;
mod lru;
mod eviction;

pub use tracker::{MemoryTracker, MemoryStats};
pub use lru::LRUTracker;
pub use eviction::EvictionPolicy;

/// Parse memory size from string (e.g., "1gb", "512mb", "1073741824")
pub fn parse_memory_size(s: &str) -> Result<usize, String> {
    let s = s.trim().to_lowercase();

    // Check for percentage
    if let Some(pct) = s.strip_suffix('%') {
        let percent: f64 = pct.parse().map_err(|_| format!("Invalid percentage: {}", s))?;
        if !(0.0..=100.0).contains(&percent) {
            return Err(format!("Percentage must be 0-100: {}", percent));
        }
        // Will be resolved later with system memory
        return Ok((percent * 1024.0 * 1024.0 * 1024.0 / 100.0) as usize); // Placeholder
    }

    // Parse with suffix
    if let Some(gb) = s.strip_suffix("gb") {
        let num: f64 = gb.trim().parse().map_err(|_| format!("Invalid number: {}", gb))?;
        return Ok((num * 1024.0 * 1024.0 * 1024.0) as usize);
    }
    if let Some(mb) = s.strip_suffix("mb") {
        let num: f64 = mb.trim().parse().map_err(|_| format!("Invalid number: {}", mb))?;
        return Ok((num * 1024.0 * 1024.0) as usize);
    }
    if let Some(kb) = s.strip_suffix("kb") {
        let num: f64 = kb.trim().parse().map_err(|_| format!("Invalid number: {}", kb))?;
        return Ok((num * 1024.0) as usize);
    }

    // Plain number (bytes)
    s.parse().map_err(|_| format!("Invalid memory size: {}", s))
}

/// Format bytes as human-readable string
pub fn format_bytes(bytes: usize) -> String {
    const GB: usize = 1024 * 1024 * 1024;
    const MB: usize = 1024 * 1024;
    const KB: usize = 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Get system total memory in bytes
pub fn get_system_memory() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if line.starts_with("MemTotal:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<usize>() {
                            return Some(kb * 1024); // KB to bytes
                        }
                    }
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        if let Ok(output) = Command::new("sysctl").args(["-n", "hw.memsize"]).output() {
            if let Ok(s) = String::from_utf8(output.stdout) {
                if let Ok(bytes) = s.trim().parse::<usize>() {
                    return Some(bytes);
                }
            }
        }
    }

    #[cfg(target_os = "windows")]
    {
        // Windows fallback - could use winapi but keep it simple
        None
    }

    None
}

/// Calculate default max memory (75% of system RAM or 512MB fallback)
pub fn default_max_memory() -> usize {
    match get_system_memory() {
        Some(total) => {
            let default = (total as f64 * 0.75) as usize;
            log::info!(
                "System memory: {}, using 75% as default max: {}",
                format_bytes(total),
                format_bytes(default)
            );
            default
        }
        None => {
            let fallback = 512 * 1024 * 1024; // 512MB
            log::warn!(
                "Could not detect system memory, using fallback: {}",
                format_bytes(fallback)
            );
            fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_size() {
        assert_eq!(parse_memory_size("1gb").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("512mb").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("100kb").unwrap(), 100 * 1024);
        assert_eq!(parse_memory_size("1024").unwrap(), 1024);
        assert_eq!(parse_memory_size("1.5gb").unwrap(), (1.5 * 1024.0 * 1024.0 * 1024.0) as usize);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_bytes(512 * 1024 * 1024), "512.00 MB");
        assert_eq!(format_bytes(100 * 1024), "100.00 KB");
        assert_eq!(format_bytes(500), "500 bytes");
    }
}
