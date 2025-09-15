# Bolt

**High-performance in-memory key-value database**

[![Docker Pulls](https://img.shields.io/docker/pulls/cloud55io/bolt)](https://hub.docker.com/r/cloud55io/bolt)
[![Docker Image Size](https://img.shields.io/docker/image-size/cloud55io/bolt/latest)](https://hub.docker.com/r/cloud55io/bolt)

```
   ___  ____  __ ______
  / _ )/ __ \/ //_  __/
 / _  / /_/ / /__/ /
/____/\____/____/_/
```

## Features

- **Blazing Fast**: In-memory storage with microsecond latency
- **Multi-Master Replication**: Write to any node, automatic sync
- **Persistence**: WAL (Write-Ahead Log) for data durability
- **LZ4 Compression**: Automatic compression for large values
- **TTL Support**: Key expiration with lazy cleanup
- **CRDT Counters**: Conflict-free distributed counters
- **Memory Management**: LRU eviction, configurable limits
- **High-Performance Mode**: Sharded storage (64 shards) + batched WAL
- **Prometheus Metrics**: Built-in `/metrics` endpoint
- **TLS Support**: Optional encrypted connections
- **Authentication**: Username/password auth

## Quick Start

### Standalone Mode

```bash
docker run -d \
  --name bolt \
  -p 2012:2012 \
  -p 9091:9091 \
  -v bolt-data:/data \
  cloud55io/bolt:0.0.5
```

### Connect with bolt-ctl

```bash
# Using bolt-ctl (included in container)
docker exec -it bolt bolt-ctl put hello world
docker exec -it bolt bolt-ctl get hello
docker exec -it bolt bolt-ctl stats
docker exec -it bolt bolt-ctl keys "*"
```

### Check metrics

```bash
curl http://localhost:9091/metrics
```

## Docker Compose

### Standalone

```yaml
version: '3.8'
services:
  bolt:
    image: cloud55io/bolt:0.0.5
    ports:
      - "2012:2012"
      - "9091:9091"
    volumes:
      - bolt-data:/data
    environment:
      - BOLT_PERSIST=true
      - BOLT_HIGH_PERF=true
      - BOLT_MAX_MEMORY=75%

volumes:
  bolt-data:
```

### 3-Node Cluster (Multi-Master)

```yaml
version: '3.8'

services:
  bolt-node1:
    image: cloud55io/bolt:0.0.5
    hostname: bolt-node1
    ports:
      - "2012:2012"
      - "9091:9091"
    volumes:
      - bolt-node1-data:/data
    environment:
      - BOLT_NODE_ID=node1
      - BOLT_CLUSTER_PORT=2013
      - BOLT_PEERS=node2@bolt-node2:2013,node3@bolt-node3:2013
      - BOLT_PERSIST=true
      - BOLT_HIGH_PERF=true

  bolt-node2:
    image: cloud55io/bolt:0.0.5
    hostname: bolt-node2
    ports:
      - "2022:2012"
      - "9092:9091"
    volumes:
      - bolt-node2-data:/data
    environment:
      - BOLT_NODE_ID=node2
      - BOLT_CLUSTER_PORT=2013
      - BOLT_PEERS=node1@bolt-node1:2013,node3@bolt-node3:2013
      - BOLT_PERSIST=true
      - BOLT_HIGH_PERF=true

  bolt-node3:
    image: cloud55io/bolt:0.0.5
    hostname: bolt-node3
    ports:
      - "2032:2012"
      - "9093:9091"
    volumes:
      - bolt-node3-data:/data
    environment:
      - BOLT_NODE_ID=node3
      - BOLT_CLUSTER_PORT=2013
      - BOLT_PEERS=node1@bolt-node1:2013,node2@bolt-node2:2013
      - BOLT_PERSIST=true
      - BOLT_HIGH_PERF=true

volumes:
  bolt-node1-data:
  bolt-node2-data:
  bolt-node3-data:
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BOLT_HOST` | `0.0.0.0` | Listen address |
| `BOLT_PORT` | `2012` | Client port |
| `BOLT_DATA_DIR` | `/data` | Data directory for WAL |
| `BOLT_PERSIST` | `true` | Enable persistence |
| `BOLT_HIGH_PERF` | `false` | Enable high-performance mode |
| `BOLT_MAX_MEMORY` | `75%` | Memory limit (e.g., `8gb`, `75%`) |
| `BOLT_EVICTION_POLICY` | `allkeys-lru` | Eviction policy |
| `BOLT_AUTH_ENABLED` | `true` | Enable authentication |
| `BOLT_ADMIN_PASSWORD` | `admin` | Admin password |

### Cluster Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BOLT_NODE_ID` | - | Node identifier (enables cluster mode) |
| `BOLT_CLUSTER_PORT` | `2013` | Cluster communication port |
| `BOLT_PEERS` | - | Comma-separated peers (`id@host:port`) |

### TLS Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BOLT_TLS_CERT` | - | Path to TLS certificate |
| `BOLT_TLS_KEY` | - | Path to TLS private key |

## Supported Commands (bolt-ctl)

### Strings
- `put key value` / `get key` / `del key`
- `setex key seconds value` - Set with TTL
- `mset key1 val1 key2 val2` / `mget key1 key2`
- `incr key` / `decr key` / `incrby key delta`

### Lists
- `lpush key value` / `rpush key value`
- `lpop key` / `rpop key`
- `lrange key start stop` / `llen key`

### Sets
- `sadd key member` / `srem key member`
- `smembers key` / `sismember key member`
- `scard key`

### CRDT Counters (Multi-Master Safe)
- `cincr key` / `cdecr key` / `cincrby key delta`
- `cget key`

### Utility
- `stats` / `keys pattern` / `exists key` / `type key`
- `ttl key` / `cluster`

### User Management
- `useradd` / `userdel` / `users` / `passwd` / `whoami`

## Eviction Policies

| Policy | Description |
|--------|-------------|
| `noeviction` | Return error when memory full |
| `allkeys-lru` | Evict least recently used (default) |
| `allkeys-random` | Evict random keys |
| `volatile-lru` | Evict LRU keys with TTL only |
| `volatile-ttl` | Evict keys closest to expiration |

## Prometheus Metrics

Available at `http://localhost:9091/metrics`:

- `bolt_keys_total` - Total keys in storage
- `bolt_memory_used_bytes` - Memory usage
- `bolt_connections_total` - Active connections
- `bolt_commands_total` - Commands processed
- `bolt_replication_*` - Replication metrics (cluster mode)

## Health Checks

```bash
# Health endpoint
curl http://localhost:9091/health

# Readiness endpoint
curl http://localhost:9091/ready
```

## Performance

- **Throughput**: 100k+ ops/sec (high-perf mode)
- **Latency**: < 100μs p99
- **Memory**: ~100 bytes overhead per key

## License

MIT License - Cloud55
engineering@cloud55.com.br