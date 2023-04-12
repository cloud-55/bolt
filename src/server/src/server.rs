use storage::{Storage, StorageConfig, DatabaseId};
use std::env;
use std::sync::Arc;
use log::info;
use tokio::net::TcpListener;
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

        // Store a reference for background tasks
        let _storage_ref = Arc::new(self.storage.clone());

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let storage = self.storage.clone();

                    info!("New connection from {}", peer_addr);

                    tokio::spawn(async move {
                        let result = handle_client(stream, storage).await;

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
    }
}

async fn handle_client<S>(
    mut stream: S,
    storage: Storage,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // Process messages in a loop until the client disconnects
    loop {
        match Message::receive_async(&mut stream).await {
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
            // Basic stats - just uptime for now
            let stats_json = format!(
                r#"{{"version":"0.1.0","status":"running"}}"#,
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
