use storage::{Storage, StorageConfig, DatabaseId};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use log::info;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::io::{AsyncRead, AsyncWrite};
use crate::error::ServerError;
use crate::message::Message;
use crate::opcodes::*;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: &str = "2012";
const DEFAULT_DATA_DIR: &str = "./data";

pub struct Server {
    storage: Storage,
    host: String,
    port: u16,
}

impl Server {
    pub fn new() -> Result<Self, ServerError> {
        let host = env::var("BOLT_HOST").unwrap_or_else(|_| DEFAULT_HOST.to_string());
        let port_str = env::var("BOLT_PORT").unwrap_or_else(|_| DEFAULT_PORT.to_string());
        let port = port_str.parse::<u16>()
            .map_err(|e| ServerError::InvalidPort(format!("{}: {}", port_str, e)))?;

        // Configure storage with optional persistence
        let data_dir = env::var("BOLT_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
        let persistence_enabled = env::var("BOLT_PERSIST")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(true);

        let storage = if persistence_enabled {
            let wal_path = format!("{}/bolt.wal", data_dir);
            info!("Persistence enabled, WAL path: {}", wal_path);
            Storage::with_config(StorageConfig::with_wal(&wal_path))?
        } else {
            info!("Running in memory-only mode (no persistence)");
            Storage::with_config(StorageConfig::in_memory())?
        };

        Ok(Server {
            storage,
            host,
            port,
        })
    }

    pub async fn run(&self) -> Result<(), ServerError> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr).await?;

        info!("BOLT is running on {} (TCP, standalone) ...", addr);

        // Start TTL cleanup background task
        let storage_clone = Arc::new(self.storage.clone());
        let ttl_cleanup_interval = env::var("BOLT_TTL_CLEANUP_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60u64);
        Storage::start_ttl_cleanup(storage_clone.clone(), Duration::from_secs(ttl_cleanup_interval));
        info!("TTL cleanup interval: {}s", ttl_cleanup_interval);

        // Shutdown broadcast channel
        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        // Spawn signal handler
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                info!("Failed to listen for shutdown signal: {}", e);
                return;
            }
            info!("Received shutdown signal, stopping server...");
            let _ = shutdown_tx_clone.send(());
        });

        loop {
            let mut shutdown_rx = shutdown_tx.subscribe();

            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            let storage = self.storage.clone();
                            let mut client_shutdown_rx = shutdown_tx.subscribe();

                            info!("New connection from {}", peer_addr);

                            tokio::spawn(async move {
                                let result = handle_client(stream, storage, &mut client_shutdown_rx).await;

                                if let Err(e) = result {
                                    info!("Connection closed from {}: {}", peer_addr, e);
                                } else {
                                    info!("Connection closed from {}", peer_addr);
                                }
                            });
                        }
                        Err(e) => {
                            info!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received, stopping server...");
                    info!("BOLT server stopped");
                    return Ok(());
                }
            }
        }
    }
}

async fn handle_client<S>(
    mut stream: S,
    storage: Storage,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    loop {
        tokio::select! {
            result = Message::receive_async(&mut stream) => {
                match result {
                    Ok(message) => {
                        if let Err(e) = process_message(&message, &storage, &mut stream).await {
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            return Ok(());
                        }
                        return Err(e);
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Client handler received shutdown signal");
                return Ok(());
            }
        }
    }
}

async fn process_message<S>(
    message: &Message,
    storage: &Storage,
    stream: &mut S,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    match message.code {
        OP_DB_SWITCH => {
            info!("Switched to database: {:?}", message.database_id);
        }
        OP_PUT => {
            let key = &message.key;
            let value = &message.value;
            storage.set(message.database_id.clone(), key, value).await;

            info!("OK PUT {} {}", key, value);

            // Send response back to client
            let response = Message {
                code: OP_PUT,
                key: key.clone(),
                value: value.clone(),
                not_found: false,
                database_id: message.database_id.clone(),
            };
            response.send_async(stream).await?;
        }
        OP_SETEX => {
            // SET with TTL: key contains the key, value contains "ttl_seconds:actual_value"
            let key = &message.key;

            // Parse TTL from value (format: "ttl_seconds:value")
            let (ttl_secs, actual_value) = if let Some(colon_pos) = message.value.find(':') {
                let ttl_str = &message.value[..colon_pos];
                let value = &message.value[colon_pos + 1..];
                let ttl = ttl_str.parse::<u64>().unwrap_or(0);
                (ttl, value.to_string())
            } else {
                (0, message.value.clone())
            };

            let ttl = if ttl_secs > 0 {
                Some(Duration::from_secs(ttl_secs))
            } else {
                None
            };

            storage.set_with_ttl(message.database_id.clone(), key, &actual_value, ttl).await;

            info!("OK SETEX {} {} (TTL: {}s)", key, actual_value, ttl_secs);

            // Send response back to client
            let response = Message {
                code: OP_SETEX,
                key: key.clone(),
                value: actual_value,
                not_found: false,
                database_id: message.database_id.clone(),
            };
            response.send_async(stream).await?;
        }
        OP_TTL => {
            let key = &message.key;
            let database_id = message.database_id.clone();
            let ttl = storage.get_ttl(database_id.clone(), key).await;

            let response = match ttl {
                Some(Some(secs)) => {
                    info!("OK TTL {} = {}s", key, secs);
                    Message {
                        code: OP_TTL,
                        key: key.clone(),
                        value: secs.to_string(),
                        not_found: false,
                        database_id,
                    }
                }
                Some(None) => {
                    info!("OK TTL {} = -1 (no expiration)", key);
                    Message {
                        code: OP_TTL,
                        key: key.clone(),
                        value: "-1".to_string(),
                        not_found: false,
                        database_id,
                    }
                }
                None => {
                    info!("ERR KEY_NOT_FOUND: {}", key);
                    Message::not_found_response()
                }
            };
            response.send_async(stream).await?;
        }
        OP_GET_ALL => {
            let entries = storage.get_all_entries(message.database_id.clone()).await;
            match entries {
                Some(entries) => {
                    for (key, value) in entries.iter() {
                        info!("Key: {}, Value: {}", key, value);
                    }
                }
                None => {
                    info!("DATABASE IS EMPTY");
                }
            }
        }
        OP_GET => {
            let key = &message.key;
            let database_id = message.database_id.clone();
            let value = storage.get(database_id.clone(), key).await;

            let response = match value {
                Some(value) => {
                    info!("OK GET {} {}", key, value);
                    Message {
                        code: OP_GET,
                        key: key.clone(),
                        value,
                        not_found: false,
                        database_id,
                    }
                }
                None => {
                    info!("ERR KEY_NOT_FOUND: {}", key);
                    Message::not_found_response()
                }
            };
            response.send_async(stream).await?;
        }
        OP_MGET => {
            // Batch GET: keys are separated by newlines in the key field
            let keys: Vec<String> = message.key.split('\n').map(|s| s.to_string()).collect();
            let database_id = message.database_id.clone();
            let values = storage.mget(database_id.clone(), &keys).await;

            // Return values as newline-separated, with empty string for missing keys
            let result: String = values.iter()
                .map(|v| v.as_deref().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n");

            info!("OK MGET {} keys", keys.len());
            let response = Message {
                code: OP_MGET,
                key: message.key.clone(),
                value: result,
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_MSET => {
            // Batch SET: key field contains "key1\nkey2\n...", value field contains "val1\nval2\n..."
            let keys: Vec<&str> = message.key.split('\n').collect();
            let values: Vec<&str> = message.value.split('\n').collect();
            let database_id = message.database_id.clone();

            let pairs: Vec<(String, String)> = keys.iter()
                .zip(values.iter())
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();

            storage.mset(database_id.clone(), &pairs).await;

            info!("OK MSET {} pairs", pairs.len());

            let response = Message {
                code: OP_MSET,
                key: String::new(),
                value: pairs.len().to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_MDEL => {
            // Batch DELETE: keys are separated by newlines in the key field
            let keys: Vec<String> = message.key.split('\n').map(|s| s.to_string()).collect();
            let database_id = message.database_id.clone();
            let deleted = storage.mdel(database_id.clone(), &keys).await;

            info!("OK MDEL {} keys (deleted {})", keys.len(), deleted);
            let response = Message {
                code: OP_MDEL,
                key: String::new(),
                value: deleted.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_DEL => {
            let key = &message.key;
            let database_id = message.database_id.clone();
            let value = storage.remove(database_id.clone(), key).await;

            let response = match value {
                Some(value) => {
                    info!("OK DEL {} {}", key, value);
                    Message {
                        code: OP_DEL,
                        key: key.clone(),
                        value,
                        not_found: false,
                        database_id,
                    }
                }
                None => {
                    info!("ERR KEY_NOT_FOUND: {}", key);
                    Message::not_found_response()
                }
            };
            response.send_async(stream).await?;
        }
        OP_STATS => {
            let uptime_secs = 0u64; // Basic stats without metrics struct
            let stats_json = format!(
                r#"{{"uptime_seconds":{}}}"#,
                uptime_secs,
            );
            info!("STATS requested");
            let response = Message {
                code: OP_STATS,
                key: String::new(),
                value: stats_json,
                not_found: false,
                database_id: DatabaseId::Default,
            };
            response.send_async(stream).await?;
        }
        // Utility operations
        OP_EXISTS => {
            let key = &message.key;
            let database_id = message.database_id.clone();
            let exists = storage.exists(database_id.clone(), key).await;
            info!("OK EXISTS {} = {}", key, exists);
            let response = Message {
                code: OP_EXISTS,
                key: key.clone(),
                value: if exists { "1" } else { "0" }.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_KEYS => {
            let pattern = &message.key;
            let database_id = message.database_id.clone();
            let keys = storage.keys(database_id.clone(), pattern).await;
            let result = keys.join("\n");
            info!("OK KEYS {} = {} keys", pattern, keys.len());
            let response = Message {
                code: OP_KEYS,
                key: pattern.clone(),
                value: result,
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        _ => {
            info!("Unknown operation: {}", message.code);
        }
    }
    Ok(())
}
