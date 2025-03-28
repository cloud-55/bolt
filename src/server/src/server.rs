use storage::{Storage, StorageConfig, DatabaseId, EvictionPolicy, format_bytes};
use std::env;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use log::info;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::io::{AsyncRead, AsyncWrite};
use crate::auth::{UserStore, Session, UserRole, AuthError};
use crate::error::ServerError;
use crate::message::Message;
use crate::metrics::Metrics;
use crate::opcodes::*;
use crate::http_metrics::HttpMetricsServer;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: &str = "2012";
const DEFAULT_DATA_DIR: &str = "./data";
const DEFAULT_MAX_CONNECTIONS: usize = 1000;
const DEFAULT_METRICS_PORT: &str = "9091";

pub struct Server {
    storage: Storage,
    host: String,
    port: u16,
    user_store: UserStore,
    max_connections: usize,
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

        // High-performance mode: sharded storage with batched WAL
        let high_performance = env::var("BOLT_HIGH_PERF")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        let shard_count = env::var("BOLT_SHARD_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64usize);

        let wal_batch_size = env::var("BOLT_WAL_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000usize);

        let wal_flush_interval_ms = env::var("BOLT_WAL_FLUSH_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10u64);

        // Memory management configuration
        let max_memory = storage::memory::parse_memory_size(
            &env::var("BOLT_MAX_MEMORY").unwrap_or_default()
        ).unwrap_or_else(|_| storage::memory::default_max_memory());

        let eviction_policy = env::var("BOLT_EVICTION_POLICY")
            .ok()
            .and_then(|s| EvictionPolicy::from_str(&s))
            .unwrap_or(EvictionPolicy::AllKeysLRU);

        let memory_warning_threshold = env::var("BOLT_MEMORY_WARNING_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(90usize);

        let storage = if persistence_enabled {
            let wal_path = format!("{}/bolt.wal", data_dir);
            info!("Persistence enabled, WAL path: {}", wal_path);

            let mut config = StorageConfig::with_wal(&wal_path)
                .with_max_memory(max_memory)
                .with_eviction_policy(eviction_policy)
                .with_memory_warning_threshold(memory_warning_threshold);

            if high_performance {
                config = config
                    .with_high_performance()
                    .with_shard_count(shard_count)
                    .with_wal_batch_size(wal_batch_size)
                    .with_wal_flush_interval_ms(wal_flush_interval_ms);
                info!("High-performance mode enabled (shards: {}, batch_size: {}, flush_interval: {}ms)",
                      shard_count, wal_batch_size, wal_flush_interval_ms);
            }

            Storage::with_config(config)?
        } else {
            info!("Running in memory-only mode (no persistence)");
            let config = StorageConfig::in_memory()
                .with_max_memory(max_memory)
                .with_eviction_policy(eviction_policy)
                .with_memory_warning_threshold(memory_warning_threshold);

            if high_performance {
                info!("High-performance mode enabled (shards: {})", shard_count);
                let config = config
                    .with_high_performance()
                    .with_shard_count(shard_count);
                Storage::with_config(config)?
            } else {
                Storage::with_config(config)?
            }
        };

        // Log memory configuration
        if max_memory > 0 {
            info!("Memory limit: {} (eviction: {}, warning at {}%)",
                  format_bytes(max_memory), eviction_policy, memory_warning_threshold);
        } else {
            info!("Memory limit: unlimited (no eviction)");
        }

        // User authentication configuration
        let users_file = format!("{}/users.json", data_dir);
        let user_store = if persistence_enabled {
            UserStore::with_persistence(&users_file)?
        } else {
            UserStore::new()
        };

        // Create default admin user if no users exist
        let default_admin_password = env::var("BOLT_ADMIN_PASSWORD")
            .unwrap_or_else(|_| "admin".to_string());
        let is_default_password = default_admin_password == "admin";

        // We need to run this in a blocking context since we're in a sync function
        let user_store_clone = user_store.clone();
        let created_admin = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                user_store_clone.ensure_default_admin(&default_admin_password).await
            })
        }).join().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Failed to create admin user"))??;

        if created_admin {
            info!("Created default admin user (username: admin, password: {})",
                  if is_default_password { "admin - CHANGE THIS!" } else { "****" });
        }

        let user_count = std::thread::spawn({
            let user_store = user_store.clone();
            move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async { user_store.user_count().await })
            }
        }).join().unwrap_or(0);

        info!("Authentication enabled ({} users configured)", user_count);

        // Connection limits
        let max_connections = env::var("BOLT_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_CONNECTIONS);
        info!("Max connections: {}", max_connections);

        Ok(Server {
            storage,
            host,
            port,
            user_store,
            max_connections,
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
        let metrics = Arc::new(Metrics::new());

        // Start HTTP metrics server for Prometheus
        let metrics_port = env::var("BOLT_METRICS_PORT")
            .unwrap_or_else(|_| DEFAULT_METRICS_PORT.to_string())
            .parse::<u16>()
            .unwrap_or(9091);

        let metrics_enabled = env::var("BOLT_METRICS_ENABLED")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);

        if metrics_enabled {
            let http_server = HttpMetricsServer::new(
                storage_clone.clone(),
                metrics.clone(),
                None, // No cluster in this version
                metrics_port,
            );
            tokio::spawn(async move {
                if let Err(e) = http_server.start().await {
                    log::error!("HTTP metrics server error: {}", e);
                }
            });
        }

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
                            // Check connection limit
                            let current = metrics.active_connections.load(Ordering::SeqCst);
                            if current >= self.max_connections {
                                info!("Connection limit reached ({}), rejecting connection from {}", self.max_connections, peer_addr);
                                drop(stream);
                                continue;
                            }

                            let storage = self.storage.clone();
                            let metrics = metrics.clone();
                            let mut client_shutdown_rx = shutdown_tx.subscribe();
                            let user_store = self.user_store.clone();
                            let max_connections = self.max_connections;

                            metrics.active_connections.fetch_add(1, Ordering::SeqCst);
                            metrics.total_connections.fetch_add(1, Ordering::SeqCst);
                            info!("New connection from {} (active: {}/{})", peer_addr, metrics.active_connections.load(Ordering::SeqCst), max_connections);

                            tokio::spawn(async move {
                                let result = handle_client(stream, storage, &mut client_shutdown_rx, user_store, metrics.clone()).await;

                                metrics.active_connections.fetch_sub(1, Ordering::SeqCst);
                                if let Err(e) = result {
                                    info!("Connection closed from {}: {} (active: {})", peer_addr, e, metrics.active_connections.load(Ordering::SeqCst));
                                } else {
                                    info!("Connection closed from {} (active: {})", peer_addr, metrics.active_connections.load(Ordering::SeqCst));
                                }
                            });
                        }
                        Err(e) => {
                            info!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received, waiting for {} active connections to close...",
                          metrics.active_connections.load(Ordering::SeqCst));

                    // Wait for active connections with timeout
                    let timeout = tokio::time::Duration::from_secs(10);
                    let start = tokio::time::Instant::now();

                    while metrics.active_connections.load(Ordering::SeqCst) > 0 && start.elapsed() < timeout {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }

                    let remaining = metrics.active_connections.load(Ordering::SeqCst);
                    if remaining > 0 {
                        info!("Timeout waiting for {} connections, forcing shutdown", remaining);
                    } else {
                        info!("All connections closed gracefully");
                    }

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
    user_store: UserStore,
    metrics: Arc<Metrics>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut session = Session::new();

    // Authenticate the client first
    match authenticate_client(&mut stream, &user_store, shutdown_rx).await {
        Ok(Some(user)) => {
            session.authenticate(user);
            info!("Client authenticated as '{}'", session.username().unwrap_or("unknown"));
        }
        Ok(None) => {
            info!("Authentication failed, closing connection");
            return Ok(());
        }
        Err(e) => {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(());
            }
            return Err(e);
        }
    }

    // Process messages
    loop {
        tokio::select! {
            result = Message::receive_async(&mut stream) => {
                match result {
                    Ok(message) => {
                        if let Err(e) = process_message(&message, &storage, &mut stream, &metrics, &session, &user_store).await {
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

async fn authenticate_client<S>(
    stream: &mut S,
    user_store: &UserStore,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> std::io::Result<Option<crate::auth::User>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    tokio::select! {
        result = Message::receive_async(stream) => {
            match result {
                Ok(message) => {
                    if message.code != OP_AUTH {
                        // Client didn't send auth message first
                        let response = Message {
                            code: OP_AUTH_FAIL,
                            key: String::new(),
                            value: "Authentication required. Send AUTH with username:password".to_string(),
                            not_found: false,
                            database_id: DatabaseId::Default,
                        };
                        response.send_async(stream).await?;
                        return Ok(None);
                    }

                    // Parse username:password from the value field
                    // Format: "username:password" in value field
                    let credentials = &message.value;
                    let (username, password) = if let Some(colon_pos) = credentials.find(':') {
                        let user = &credentials[..colon_pos];
                        let pass = &credentials[colon_pos + 1..];
                        (user, pass)
                    } else {
                        // Also support username in key field and password in value field
                        (message.key.as_str(), credentials.as_str())
                    };

                    // Authenticate with user store
                    match user_store.authenticate(username, password).await {
                        Ok(user) => {
                            let response = Message {
                                code: OP_AUTH_OK,
                                key: user.username.clone(),
                                value: format!("Authenticated as {} (role: {})", user.username, user.role.as_str()),
                                not_found: false,
                                database_id: DatabaseId::Default,
                            };
                            response.send_async(stream).await?;
                            Ok(Some(user))
                        }
                        Err(AuthError::InvalidCredentials) => {
                            let response = Message {
                                code: OP_AUTH_FAIL,
                                key: String::new(),
                                value: "Invalid username or password".to_string(),
                                not_found: false,
                                database_id: DatabaseId::Default,
                            };
                            response.send_async(stream).await?;
                            Ok(None)
                        }
                        Err(e) => {
                            let response = Message {
                                code: OP_AUTH_FAIL,
                                key: String::new(),
                                value: format!("Authentication error: {}", e),
                                not_found: false,
                                database_id: DatabaseId::Default,
                            };
                            response.send_async(stream).await?;
                            Ok(None)
                        }
                    }
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(None);
                    }
                    Err(e)
                }
            }
        }
        _ = shutdown_rx.recv() => {
            Ok(None)
        }
    }
}

async fn process_message<S>(
    message: &Message,
    storage: &Storage,
    stream: &mut S,
    metrics: &Metrics,
    session: &Session,
    user_store: &UserStore,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // Check write permissions for write operations
    let requires_write = matches!(
        message.code,
        OP_PUT | OP_DEL | OP_SETEX | OP_MSET | OP_MDEL |
        OP_INCR | OP_DECR | OP_INCRBY |
        OP_LPUSH | OP_RPUSH | OP_LPOP | OP_RPOP |
        OP_SADD | OP_SREM
    );

    if requires_write && !session.can_write() {
        let response = Message {
            code: OP_AUTH_FAIL,
            key: String::new(),
            value: "Permission denied: write access required".to_string(),
            not_found: false,
            database_id: DatabaseId::Default,
        };
        response.send_async(stream).await?;
        return Ok(());
    }
    match message.code {
        OP_DB_SWITCH => {
            info!("Switched to database: {:?}", message.database_id);
        }
        OP_PUT => {
            metrics.total_puts.fetch_add(1, Ordering::SeqCst);
            metrics.total_keys_written.fetch_add(1, Ordering::SeqCst);
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
            metrics.total_puts.fetch_add(1, Ordering::SeqCst);
            metrics.total_keys_written.fetch_add(1, Ordering::SeqCst);
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
            metrics.total_gets.fetch_add(1, Ordering::SeqCst);
            metrics.total_keys_read.fetch_add(1, Ordering::SeqCst);
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
            metrics.total_mgets.fetch_add(1, Ordering::SeqCst);
            let keys: Vec<String> = message.key.split('\n').map(|s| s.to_string()).collect();
            metrics.total_keys_read.fetch_add(keys.len(), Ordering::SeqCst);
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
            metrics.total_msets.fetch_add(1, Ordering::SeqCst);
            let keys: Vec<&str> = message.key.split('\n').collect();
            let values: Vec<&str> = message.value.split('\n').collect();
            let database_id = message.database_id.clone();

            let pairs: Vec<(String, String)> = keys.iter()
                .zip(values.iter())
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();

            storage.mset(database_id.clone(), &pairs).await;
            metrics.total_keys_written.fetch_add(pairs.len(), Ordering::SeqCst);

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
            metrics.total_mdels.fetch_add(1, Ordering::SeqCst);
            let keys: Vec<String> = message.key.split('\n').map(|s| s.to_string()).collect();
            let database_id = message.database_id.clone();
            let deleted = storage.mdel(database_id.clone(), &keys).await;
            metrics.total_keys_deleted.fetch_add(deleted, Ordering::SeqCst);

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
            metrics.total_deletes.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let value = storage.remove(database_id.clone(), key).await;
            if value.is_some() {
                metrics.total_keys_deleted.fetch_add(1, Ordering::SeqCst);
            }

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
            let stats_json = metrics.to_json(storage);
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
        OP_METRICS => {
            let prometheus_metrics = metrics.to_prometheus(storage, None).await;
            info!("METRICS (Prometheus) requested");
            let response = Message {
                code: OP_METRICS,
                key: String::new(),
                value: prometheus_metrics,
                not_found: false,
                database_id: DatabaseId::Default,
            };
            response.send_async(stream).await?;
        }
        OP_CLUSTER_STATUS => {
            let cluster_json = r#"{"mode":"standalone"}"#.to_string();
            info!("CLUSTER STATUS requested");
            let response = Message {
                code: OP_CLUSTER_STATUS,
                key: String::new(),
                value: cluster_json,
                not_found: false,
                database_id: DatabaseId::Default,
            };
            response.send_async(stream).await?;
        }
        // Counter operations
        OP_INCR => {
            metrics.total_incrs.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let new_value = storage.incr(database_id.clone(), key).await;

            info!("OK INCR {} = {}", key, new_value);
            let response = Message {
                code: OP_INCR,
                key: key.clone(),
                value: new_value.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_DECR => {
            metrics.total_incrs.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let new_value = storage.decr(database_id.clone(), key).await;

            info!("OK DECR {} = {}", key, new_value);
            let response = Message {
                code: OP_DECR,
                key: key.clone(),
                value: new_value.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_INCRBY => {
            metrics.total_incrs.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let delta: i64 = message.value.parse().unwrap_or(0);
            let new_value = storage.incr_by(database_id.clone(), key, delta).await;

            info!("OK INCRBY {} {} = {}", key, delta, new_value);
            let response = Message {
                code: OP_INCRBY,
                key: key.clone(),
                value: new_value.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        // CRDT Counter operations
        OP_CINCR => {
            metrics.total_incrs.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();

            let node_id = "standalone".to_string();
            let new_value = storage.crdt_incr(database_id.clone(), key, &node_id).await;

            info!("OK CINCR {} = {}", key, new_value);
            let response = Message {
                code: OP_CINCR,
                key: key.clone(),
                value: new_value.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_CDECR => {
            metrics.total_incrs.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();

            let node_id = "standalone".to_string();
            let new_value = storage.crdt_decr(database_id.clone(), key, &node_id).await;

            info!("OK CDECR {} = {}", key, new_value);
            let response = Message {
                code: OP_CDECR,
                key: key.clone(),
                value: new_value.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_CGET => {
            let key = &message.key;
            let database_id = message.database_id.clone();
            let value = storage.crdt_get(database_id.clone(), key).await;

            let (value_str, not_found) = match value {
                Some(v) => (v.to_string(), false),
                None => (String::new(), true),
            };

            info!("OK CGET {} = {:?}", key, value);
            let response = Message {
                code: OP_CGET,
                key: key.clone(),
                value: value_str,
                not_found,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_CINCRBY => {
            metrics.total_incrs.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let amount: u64 = message.value.parse().unwrap_or(1);

            let node_id = "standalone".to_string();
            let new_value = storage.crdt_incr_by(database_id.clone(), key, &node_id, amount).await;

            info!("OK CINCRBY {} {} = {}", key, amount, new_value);
            let response = Message {
                code: OP_CINCRBY,
                key: key.clone(),
                value: new_value.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        // List operations
        OP_LPUSH => {
            metrics.total_list_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let values: Vec<String> = message.value.split('\n').map(|s| s.to_string()).collect();
            let len = storage.lpush(database_id.clone(), key, &values).await;

            info!("OK LPUSH {} {} values = {}", key, values.len(), len);
            let response = Message {
                code: OP_LPUSH,
                key: key.clone(),
                value: len.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_RPUSH => {
            metrics.total_list_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let values: Vec<String> = message.value.split('\n').map(|s| s.to_string()).collect();
            let len = storage.rpush(database_id.clone(), key, &values).await;

            info!("OK RPUSH {} {} values = {}", key, values.len(), len);
            let response = Message {
                code: OP_RPUSH,
                key: key.clone(),
                value: len.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_LPOP => {
            metrics.total_list_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let value = storage.lpop(database_id.clone(), key).await;

            let response = match value {
                Some(v) => {
                    info!("OK LPOP {} = {}", key, v);
                    Message {
                        code: OP_LPOP,
                        key: key.clone(),
                        value: v,
                        not_found: false,
                        database_id,
                    }
                }
                None => {
                    info!("OK LPOP {} = (empty)", key);
                    Message::not_found_response()
                }
            };
            response.send_async(stream).await?;
        }
        OP_RPOP => {
            metrics.total_list_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let value = storage.rpop(database_id.clone(), key).await;

            let response = match value {
                Some(v) => {
                    info!("OK RPOP {} = {}", key, v);
                    Message {
                        code: OP_RPOP,
                        key: key.clone(),
                        value: v,
                        not_found: false,
                        database_id,
                    }
                }
                None => {
                    info!("OK RPOP {} = (empty)", key);
                    Message::not_found_response()
                }
            };
            response.send_async(stream).await?;
        }
        OP_LRANGE => {
            metrics.total_list_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            // Parse start:stop from value
            let parts: Vec<&str> = message.value.split(':').collect();
            let start: i64 = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
            let stop: i64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(-1);
            let values = storage.lrange(database_id.clone(), key, start, stop).await;
            let result = values.join("\n");
            info!("OK LRANGE {} {}:{} = {} items", key, start, stop, values.len());
            let response = Message {
                code: OP_LRANGE,
                key: key.clone(),
                value: result,
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_LLEN => {
            metrics.total_list_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let len = storage.llen(database_id.clone(), key).await;
            info!("OK LLEN {} = {}", key, len);
            let response = Message {
                code: OP_LLEN,
                key: key.clone(),
                value: len.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        // Set operations
        OP_SADD => {
            metrics.total_set_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let members: Vec<String> = message.value.split('\n').map(|s| s.to_string()).collect();
            let added = storage.sadd(database_id.clone(), key, &members).await;

            info!("OK SADD {} {} members, {} added", key, members.len(), added);
            let response = Message {
                code: OP_SADD,
                key: key.clone(),
                value: added.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_SREM => {
            metrics.total_set_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let members: Vec<String> = message.value.split('\n').map(|s| s.to_string()).collect();
            let removed = storage.srem(database_id.clone(), key, &members).await;

            info!("OK SREM {} {} members, {} removed", key, members.len(), removed);
            let response = Message {
                code: OP_SREM,
                key: key.clone(),
                value: removed.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_SMEMBERS => {
            metrics.total_set_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let members = storage.smembers(database_id.clone(), key).await;
            let result = members.join("\n");
            info!("OK SMEMBERS {} = {} members", key, members.len());
            let response = Message {
                code: OP_SMEMBERS,
                key: key.clone(),
                value: result,
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_SCARD => {
            metrics.total_set_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let database_id = message.database_id.clone();
            let size = storage.scard(database_id.clone(), key).await;
            info!("OK SCARD {} = {}", key, size);
            let response = Message {
                code: OP_SCARD,
                key: key.clone(),
                value: size.to_string(),
                not_found: false,
                database_id,
            };
            response.send_async(stream).await?;
        }
        OP_SISMEMBER => {
            metrics.total_set_ops.fetch_add(1, Ordering::SeqCst);
            let key = &message.key;
            let member = &message.value;
            let database_id = message.database_id.clone();
            let exists = storage.sismember(database_id.clone(), key, member).await;
            info!("OK SISMEMBER {} {} = {}", key, member, exists);
            let response = Message {
                code: OP_SISMEMBER,
                key: key.clone(),
                value: if exists { "1" } else { "0" }.to_string(),
                not_found: false,
                database_id,
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
        OP_TYPE => {
            let key = &message.key;
            let database_id = message.database_id.clone();
            let key_type = storage.key_type(database_id.clone(), key).await;
            let response = match key_type {
                Some(t) => {
                    info!("OK TYPE {} = {}", key, t);
                    Message {
                        code: OP_TYPE,
                        key: key.clone(),
                        value: t.to_string(),
                        not_found: false,
                        database_id,
                    }
                }
                None => {
                    info!("OK TYPE {} = none", key);
                    Message {
                        code: OP_TYPE,
                        key: key.clone(),
                        value: "none".to_string(),
                        not_found: true,
                        database_id,
                    }
                }
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
        // User management operations
        OP_USER_ADD => {
            if !session.can_manage_users() {
                let response = Message {
                    code: OP_AUTH_FAIL,
                    key: String::new(),
                    value: "Permission denied: admin access required".to_string(),
                    not_found: false,
                    database_id: DatabaseId::Default,
                };
                response.send_async(stream).await?;
                return Ok(());
            }

            // Format: key=username, value=password:role
            let username = &message.key;
            let parts: Vec<&str> = message.value.splitn(2, ':').collect();
            let (password, role_str) = if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                (message.value.as_str(), "readwrite")
            };

            let role = UserRole::from_str(role_str).unwrap_or(UserRole::ReadWrite);

            match user_store.add_user(username, password, role.clone()).await {
                Ok(()) => {
                    info!("User '{}' created with role '{}'", username, role.as_str());
                    let response = Message {
                        code: OP_USER_ADD,
                        key: username.clone(),
                        value: format!("User '{}' created with role '{}'", username, role.as_str()),
                        not_found: false,
                        database_id: DatabaseId::Default,
                    };
                    response.send_async(stream).await?;
                }
                Err(e) => {
                    let response = Message {
                        code: OP_AUTH_FAIL,
                        key: String::new(),
                        value: format!("Failed to create user: {}", e),
                        not_found: false,
                        database_id: DatabaseId::Default,
                    };
                    response.send_async(stream).await?;
                }
            }
        }
        OP_USER_DEL => {
            if !session.can_manage_users() {
                let response = Message {
                    code: OP_AUTH_FAIL,
                    key: String::new(),
                    value: "Permission denied: admin access required".to_string(),
                    not_found: false,
                    database_id: DatabaseId::Default,
                };
                response.send_async(stream).await?;
                return Ok(());
            }

            let username = &message.key;

            // Prevent deleting yourself
            if session.username() == Some(username) {
                let response = Message {
                    code: OP_AUTH_FAIL,
                    key: String::new(),
                    value: "Cannot delete your own user".to_string(),
                    not_found: false,
                    database_id: DatabaseId::Default,
                };
                response.send_async(stream).await?;
                return Ok(());
            }

            match user_store.remove_user(username).await {
                Ok(()) => {
                    info!("User '{}' deleted", username);
                    let response = Message {
                        code: OP_USER_DEL,
                        key: username.clone(),
                        value: format!("User '{}' deleted", username),
                        not_found: false,
                        database_id: DatabaseId::Default,
                    };
                    response.send_async(stream).await?;
                }
                Err(e) => {
                    let response = Message {
                        code: OP_AUTH_FAIL,
                        key: String::new(),
                        value: format!("Failed to delete user: {}", e),
                        not_found: false,
                        database_id: DatabaseId::Default,
                    };
                    response.send_async(stream).await?;
                }
            }
        }
        OP_USER_LIST => {
            if !session.can_manage_users() {
                let response = Message {
                    code: OP_AUTH_FAIL,
                    key: String::new(),
                    value: "Permission denied: admin access required".to_string(),
                    not_found: false,
                    database_id: DatabaseId::Default,
                };
                response.send_async(stream).await?;
                return Ok(());
            }

            let users = user_store.list_users().await;
            let result: Vec<String> = users.iter()
                .map(|(name, role)| format!("{}:{}", name, role.as_str()))
                .collect();
            let response = Message {
                code: OP_USER_LIST,
                key: String::new(),
                value: result.join("\n"),
                not_found: false,
                database_id: DatabaseId::Default,
            };
            response.send_async(stream).await?;
        }
        OP_USER_PASSWD => {
            // Users can change their own password, admins can change anyone's
            let username = &message.key;
            let new_password = &message.value;

            let is_self = session.username() == Some(username);
            let is_admin = session.can_manage_users();

            if !is_self && !is_admin {
                let response = Message {
                    code: OP_AUTH_FAIL,
                    key: String::new(),
                    value: "Permission denied: can only change your own password".to_string(),
                    not_found: false,
                    database_id: DatabaseId::Default,
                };
                response.send_async(stream).await?;
                return Ok(());
            }

            match user_store.change_password(username, new_password).await {
                Ok(()) => {
                    info!("Password changed for user '{}'", username);
                    let response = Message {
                        code: OP_USER_PASSWD,
                        key: username.clone(),
                        value: "Password changed successfully".to_string(),
                        not_found: false,
                        database_id: DatabaseId::Default,
                    };
                    response.send_async(stream).await?;
                }
                Err(e) => {
                    let response = Message {
                        code: OP_AUTH_FAIL,
                        key: String::new(),
                        value: format!("Failed to change password: {}", e),
                        not_found: false,
                        database_id: DatabaseId::Default,
                    };
                    response.send_async(stream).await?;
                }
            }
        }
        OP_USER_ROLE => {
            if !session.can_manage_users() {
                let response = Message {
                    code: OP_AUTH_FAIL,
                    key: String::new(),
                    value: "Permission denied: admin access required".to_string(),
                    not_found: false,
                    database_id: DatabaseId::Default,
                };
                response.send_async(stream).await?;
                return Ok(());
            }

            let username = &message.key;
            let role_str = &message.value;

            // Prevent changing your own role (to avoid locking yourself out)
            if session.username() == Some(username) {
                let response = Message {
                    code: OP_AUTH_FAIL,
                    key: String::new(),
                    value: "Cannot change your own role".to_string(),
                    not_found: false,
                    database_id: DatabaseId::Default,
                };
                response.send_async(stream).await?;
                return Ok(());
            }

            let role = match UserRole::from_str(role_str) {
                Some(r) => r,
                None => {
                    let response = Message {
                        code: OP_AUTH_FAIL,
                        key: String::new(),
                        value: "Invalid role. Use: admin, readwrite, or readonly".to_string(),
                        not_found: false,
                        database_id: DatabaseId::Default,
                    };
                    response.send_async(stream).await?;
                    return Ok(());
                }
            };

            match user_store.change_role(username, role.clone()).await {
                Ok(()) => {
                    info!("Role changed for user '{}' to '{}'", username, role.as_str());
                    let response = Message {
                        code: OP_USER_ROLE,
                        key: username.clone(),
                        value: format!("Role changed to '{}'", role.as_str()),
                        not_found: false,
                        database_id: DatabaseId::Default,
                    };
                    response.send_async(stream).await?;
                }
                Err(e) => {
                    let response = Message {
                        code: OP_AUTH_FAIL,
                        key: String::new(),
                        value: format!("Failed to change role: {}", e),
                        not_found: false,
                        database_id: DatabaseId::Default,
                    };
                    response.send_async(stream).await?;
                }
            }
        }
        OP_WHOAMI => {
            let (username, role) = match &session.user {
                Some(user) => (user.username.clone(), user.role.as_str().to_string()),
                None => ("anonymous".to_string(), "none".to_string()),
            };
            let response = Message {
                code: OP_WHOAMI,
                key: username,
                value: role,
                not_found: false,
                database_id: DatabaseId::Default,
            };
            response.send_async(stream).await?;
        }
        _ => {
            info!("Unknown operation: {}", message.code);
        }
    }
    Ok(())
}
