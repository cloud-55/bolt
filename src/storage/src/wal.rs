use std::io::{self, BufReader, BufWriter, Read, Write, Seek, SeekFrom};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use log::info;

/// Binary WAL format constants
const WAL_MAGIC: &[u8; 4] = b"BWAL";
const WAL_VERSION: u8 = 1;

/// Operation types
const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;

/// Header size: magic(4) + version(1) + op(1) + db_len(2) + key_len(4) + value_len(4) + ttl_ms(8) = 24 bytes
const HEADER_SIZE: usize = 24;
/// CRC32 size
const CRC_SIZE: usize = 4;

/// WAL entry representing a single operation
#[derive(Debug, Clone)]
pub enum WalEntry {
    Set {
        database_id: String,
        key: String,
        value: String,
        ttl_ms: Option<u64>,
    },
    Delete {
        database_id: String,
        key: String,
    },
}

impl WalEntry {
    /// Encode entry to binary format
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + 256);

        // Magic + Version
        buf.extend_from_slice(WAL_MAGIC);
        buf.push(WAL_VERSION);

        match self {
            WalEntry::Set { database_id, key, value, ttl_ms } => {
                let db_bytes = database_id.as_bytes();
                let key_bytes = key.as_bytes();
                let value_bytes = value.as_bytes();
                let ttl = ttl_ms.unwrap_or(0);

                buf.push(OP_SET);
                buf.extend_from_slice(&(db_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&ttl.to_le_bytes());
                buf.extend_from_slice(db_bytes);
                buf.extend_from_slice(key_bytes);
                buf.extend_from_slice(value_bytes);
            }
            WalEntry::Delete { database_id, key } => {
                let db_bytes = database_id.as_bytes();
                let key_bytes = key.as_bytes();

                buf.push(OP_DELETE);
                buf.extend_from_slice(&(db_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&0u32.to_le_bytes()); // value_len = 0
                buf.extend_from_slice(&0u64.to_le_bytes()); // ttl = 0
                buf.extend_from_slice(db_bytes);
                buf.extend_from_slice(key_bytes);
            }
        }

        // Calculate CRC32 of the entry (excluding the CRC itself)
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Decode entry from binary format
    fn decode(data: &[u8]) -> io::Result<(Self, usize)> {
        if data.len() < HEADER_SIZE + CRC_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Entry too short"));
        }

        // Verify magic
        if &data[0..4] != WAL_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid magic"));
        }

        // Version check
        let version = data[4];
        if version != WAL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported WAL version: {}", version)
            ));
        }

        let op = data[5];
        let db_len = u16::from_le_bytes([data[6], data[7]]) as usize;
        let key_len = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let value_len = u32::from_le_bytes([data[12], data[13], data[14], data[15]]) as usize;
        let ttl_ms = u64::from_le_bytes([
            data[16], data[17], data[18], data[19],
            data[20], data[21], data[22], data[23]
        ]);

        let total_len = HEADER_SIZE + db_len + key_len + value_len + CRC_SIZE;

        if data.len() < total_len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Entry truncated"));
        }

        // Verify CRC32
        let stored_crc = u32::from_le_bytes([
            data[total_len - 4],
            data[total_len - 3],
            data[total_len - 2],
            data[total_len - 1],
        ]);
        let calculated_crc = crc32fast::hash(&data[..total_len - CRC_SIZE]);

        if stored_crc != calculated_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CRC mismatch: stored={}, calculated={}", stored_crc, calculated_crc)
            ));
        }

        let mut offset = HEADER_SIZE;

        let database_id = String::from_utf8_lossy(&data[offset..offset + db_len]).to_string();
        offset += db_len;

        let key = String::from_utf8_lossy(&data[offset..offset + key_len]).to_string();
        offset += key_len;

        let entry = match op {
            OP_SET => {
                let value = String::from_utf8_lossy(&data[offset..offset + value_len]).to_string();
                WalEntry::Set {
                    database_id,
                    key,
                    value,
                    ttl_ms: if ttl_ms > 0 { Some(ttl_ms) } else { None },
                }
            }
            OP_DELETE => {
                WalEntry::Delete {
                    database_id,
                    key,
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown operation: {}", op)
                ));
            }
        };

        Ok((entry, total_len))
    }
}

/// Write-Ahead Log for durability (binary format)
pub struct Wal {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    enabled: bool,
}

impl Wal {
    /// Create a new WAL at the given path
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Check if we need to migrate from JSON
        if path.exists() {
            Self::migrate_if_json(&path)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        info!("WAL initialized at {:?} (binary format v{})", path, WAL_VERSION);

        Ok(Wal {
            path,
            writer: Some(BufWriter::new(file)),
            enabled: true,
        })
    }

    /// Migrate JSON WAL to binary format if needed
    fn migrate_if_json(path: &Path) -> io::Result<()> {
        let mut file = File::open(path)?;
        let mut first_bytes = [0u8; 4];

        if file.read(&mut first_bytes)? < 4 {
            return Ok(()); // Empty or too small
        }

        // If it starts with '{', it's JSON
        if first_bytes[0] == b'{' {
            info!("Migrating JSON WAL to binary format...");

            file.seek(SeekFrom::Start(0))?;
            let reader = BufReader::new(file);
            let mut entries = Vec::new();

            // Read JSON entries
            use std::io::BufRead;
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                // Parse old JSON format
                #[derive(serde::Deserialize)]
                #[serde(tag = "type", rename_all = "PascalCase")]
                enum OldWalEntry {
                    Set {
                        database_id: String,
                        key: String,
                        value: String,
                        #[serde(default)]
                        ttl_ms: Option<u64>,
                    },
                    Delete {
                        database_id: String,
                        key: String,
                    },
                }

                // Try to parse as the old format
                if let Ok(old_entry) = serde_json::from_str::<serde_json::Value>(&line) {
                    let entry = if old_entry.get("Set").is_some() || old_entry.get("value").is_some() {
                        WalEntry::Set {
                            database_id: old_entry.get("database_id")
                                .or_else(|| old_entry.get("Set").and_then(|s| s.get("database_id")))
                                .and_then(|v| v.as_str())
                                .unwrap_or("default")
                                .to_string(),
                            key: old_entry.get("key")
                                .or_else(|| old_entry.get("Set").and_then(|s| s.get("key")))
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            value: old_entry.get("value")
                                .or_else(|| old_entry.get("Set").and_then(|s| s.get("value")))
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            ttl_ms: old_entry.get("ttl_ms")
                                .or_else(|| old_entry.get("Set").and_then(|s| s.get("ttl_ms")))
                                .and_then(|v| v.as_u64()),
                        }
                    } else if old_entry.get("Delete").is_some() {
                        WalEntry::Delete {
                            database_id: old_entry.get("Delete")
                                .and_then(|d| d.get("database_id"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("default")
                                .to_string(),
                            key: old_entry.get("Delete")
                                .and_then(|d| d.get("key"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                        }
                    } else {
                        continue;
                    };
                    entries.push(entry);
                }
            }

            // Write binary format
            let backup_path = path.with_extension("wal.json.bak");
            std::fs::rename(path, &backup_path)?;

            let mut file = BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(path)?
            );

            for entry in &entries {
                file.write_all(&entry.encode())?;
            }
            file.flush()?;

            info!("Migrated {} entries from JSON to binary format", entries.len());

            // Keep backup for safety
            info!("JSON backup saved to {:?}", backup_path);
        }

        Ok(())
    }

    /// Create a disabled WAL (in-memory only mode)
    pub fn disabled() -> Self {
        Wal {
            path: PathBuf::new(),
            writer: None,
            enabled: false,
        }
    }

    /// Check if WAL is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Append a SET operation to the WAL (with optional TTL)
    pub fn log_set(&mut self, database_id: &str, key: &str, value: &str, ttl_ms: Option<u64>) -> io::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let entry = WalEntry::Set {
            database_id: database_id.to_string(),
            key: key.to_string(),
            value: value.to_string(),
            ttl_ms,
        };
        self.append(&entry)
    }

    /// Append a DELETE operation to the WAL
    pub fn log_delete(&mut self, database_id: &str, key: &str) -> io::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let entry = WalEntry::Delete {
            database_id: database_id.to_string(),
            key: key.to_string(),
        };
        self.append(&entry)
    }

    /// Append an entry to the WAL file
    fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.write_all(&entry.encode())?;
            writer.flush()?;
        }
        Ok(())
    }

    /// Read all entries from the WAL file
    pub fn read_entries(&self) -> io::Result<Vec<WalEntry>> {
        if !self.enabled || !self.path.exists() {
            return Ok(Vec::new());
        }

        let mut file = File::open(&self.path)?;
        let file_size = file.metadata()?.len() as usize;

        if file_size == 0 {
            return Ok(Vec::new());
        }

        let mut data = vec![0u8; file_size];
        file.read_exact(&mut data)?;

        let mut entries = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            match WalEntry::decode(&data[offset..]) {
                Ok((entry, consumed)) => {
                    entries.push(entry);
                    offset += consumed;
                }
                Err(e) => {
                    info!("Skipping corrupted WAL entry at offset {}: {}", offset, e);
                    // Try to find next valid entry by scanning for magic
                    offset += 1;
                    while offset + 4 <= data.len() {
                        if &data[offset..offset + 4] == WAL_MAGIC {
                            break;
                        }
                        offset += 1;
                    }
                }
            }
        }

        Ok(entries)
    }

    /// Compact the WAL by writing a snapshot and clearing old entries
    pub fn compact(&mut self, snapshot: &[(String, String, String, Option<u64>)]) -> io::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        // Close current file
        self.writer = None;

        // Create backup
        let backup_path = self.path.with_extension("wal.bak");
        if self.path.exists() {
            std::fs::rename(&self.path, &backup_path)?;
        }

        // Write new compacted WAL
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;

        let mut writer = BufWriter::new(file);

        for (db_id, key, value, ttl_ms) in snapshot {
            let entry = WalEntry::Set {
                database_id: db_id.clone(),
                key: key.clone(),
                value: value.clone(),
                ttl_ms: *ttl_ms,
            };
            writer.write_all(&entry.encode())?;
        }
        writer.flush()?;

        // Remove backup
        if backup_path.exists() {
            std::fs::remove_file(&backup_path)?;
        }

        // Reopen for appending
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)?;
        self.writer = Some(BufWriter::new(file));

        info!("WAL compacted with {} entries (binary format)", snapshot.len());
        Ok(())
    }

    /// Get the WAL file path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_wal_binary_write_read() {
        let path = "/tmp/test_bolt_wal_binary.log";
        let _ = fs::remove_file(path);

        let mut wal = Wal::new(path).unwrap();
        wal.log_set("default", "key1", "value1", None).unwrap();
        wal.log_set("db1", "key2", "value2", Some(60000)).unwrap();
        wal.log_delete("default", "key1").unwrap();

        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 3);

        // Verify first entry
        if let WalEntry::Set { database_id, key, value, ttl_ms } = &entries[0] {
            assert_eq!(database_id, "default");
            assert_eq!(key, "key1");
            assert_eq!(value, "value1");
            assert_eq!(*ttl_ms, None);
        } else {
            panic!("Expected Set entry");
        }

        // Verify second entry with TTL
        if let WalEntry::Set { ttl_ms, .. } = &entries[1] {
            assert_eq!(*ttl_ms, Some(60000));
        } else {
            panic!("Expected Set entry");
        }

        // Verify delete entry
        if let WalEntry::Delete { database_id, key } = &entries[2] {
            assert_eq!(database_id, "default");
            assert_eq!(key, "key1");
        } else {
            panic!("Expected Delete entry");
        }

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_entry_encode_decode() {
        let entry = WalEntry::Set {
            database_id: "test_db".to_string(),
            key: "test_key".to_string(),
            value: "test_value".to_string(),
            ttl_ms: Some(5000),
        };

        let encoded = entry.encode();
        let (decoded, size) = WalEntry::decode(&encoded).unwrap();

        assert_eq!(size, encoded.len());

        if let WalEntry::Set { database_id, key, value, ttl_ms } = decoded {
            assert_eq!(database_id, "test_db");
            assert_eq!(key, "test_key");
            assert_eq!(value, "test_value");
            assert_eq!(ttl_ms, Some(5000));
        } else {
            panic!("Expected Set entry");
        }
    }

    #[test]
    fn test_crc_validation() {
        let entry = WalEntry::Set {
            database_id: "db".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
            ttl_ms: None,
        };

        let mut encoded = entry.encode();

        // Corrupt the data
        encoded[10] ^= 0xFF;

        // Should fail CRC check
        let result = WalEntry::decode(&encoded);
        assert!(result.is_err());
    }
}
