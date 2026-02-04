use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, ErrorKind};
use std::path::PathBuf;

const CONFIG_FILE: &str = ".boltrc";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Config {
    /// Get the path to the config file (~/.boltrc)
    pub fn path() -> io::Result<PathBuf> {
        dirs::home_dir()
            .map(|h| h.join(CONFIG_FILE))
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "Could not find home directory"))
    }

    /// Load config from ~/.boltrc
    pub fn load() -> io::Result<Self> {
        let path = Self::path()?;
        if !path.exists() {
            return Ok(Config::default());
        }

        let content = fs::read_to_string(&path)?;
        toml::from_str(&content)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))
    }

    /// Save config to ~/.boltrc
    pub fn save(&self) -> io::Result<()> {
        let path = Self::path()?;
        let content = toml::to_string_pretty(self)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?;

        // Set restrictive permissions on the file (600)
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut opts = fs::OpenOptions::new();
            opts.write(true).create(true).truncate(true).mode(0o600);
            use std::io::Write;
            let mut file = opts.open(&path)?;
            file.write_all(content.as_bytes())?;
            return Ok(());
        }

        #[cfg(not(unix))]
        {
            fs::write(&path, content)
        }
    }

    /// Remove credentials from config (logout)
    pub fn clear_credentials(&mut self) {
        self.username = None;
        self.password = None;
    }

    /// Check if credentials are present
    pub fn has_credentials(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }

    /// Delete the config file
    pub fn delete() -> io::Result<()> {
        let path = Self::path()?;
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}
