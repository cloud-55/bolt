use std::fs::File;
use std::io::{self, BufReader};
use std::sync::Arc;
use tokio_rustls::rustls::{Certificate, PrivateKey, ServerConfig};
use tokio_rustls::TlsAcceptor;
use log::info;

/// TLS configuration for the server
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

impl TlsConfig {
    /// Create TLS configuration from environment variables
    pub fn from_env() -> Option<Self> {
        let cert_path = std::env::var("BOLT_TLS_CERT").ok()?;
        let key_path = std::env::var("BOLT_TLS_KEY").ok()?;
        Some(TlsConfig { cert_path, key_path })
    }

    /// Build a TLS acceptor from the configuration
    pub fn build_acceptor(&self) -> io::Result<TlsAcceptor> {
        let certs = load_certs(&self.cert_path)?;
        let key = load_key(&self.key_path)?;

        let config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        info!("TLS configured with cert: {}, key: {}", self.cert_path, self.key_path);
        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

fn load_certs(path: &str) -> io::Result<Vec<Certificate>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)?
        .into_iter()
        .map(Certificate)
        .collect();
    Ok(certs)
}

fn load_key(path: &str) -> io::Result<PrivateKey> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Try PKCS8 first
    let keys = rustls_pemfile::pkcs8_private_keys(&mut reader)?;
    if !keys.is_empty() {
        return Ok(PrivateKey(keys[0].clone()));
    }

    // Try RSA
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let keys = rustls_pemfile::rsa_private_keys(&mut reader)?;
    if !keys.is_empty() {
        return Ok(PrivateKey(keys[0].clone()));
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "No valid private key found",
    ))
}
