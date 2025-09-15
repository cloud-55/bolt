# Bolt

> High-performance in-memory key-value database with clustering support.

Bolt is a in-memory database designed for high throughput and low latency. It supports multi-master clustering, multiple data structures and persistence.

## Features

| Category | Features |
|----------|----------|
| **Data Structures** | Strings, Lists, Sets, Counters, CRDT Counters |
| **Operations** | GET/SET/DEL, MGET/MSET/MDEL, TTL, INCR/DECR |
| **Performance** | ~500k ops/sec, sharded storage, batched WAL |
| **Clustering** | Multi-master replication, peer-to-peer sync, no SPOF |
| **Persistence** | Write-Ahead Log (WAL), automatic recovery |
| **Memory** | Configurable limits, LRU eviction, 5 eviction policies |
| **Security** | Authentication, role-based access (admin/readwrite/readonly), TLS |
| **Observability** | Prometheus metrics, health checks |

## Table of Contents

- [Quick Start](#quick-start)
- [Installation](#installation)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Python SDK](#python-sdk)
- [CLI Reference](#cli-reference)
- [Clustering](#clustering)
- [Memory Management](#memory-management)
- [Docker](#docker)
- [Benchmarks](#benchmarks)

---

## Quick Start

### Using Docker

```bash
# Single node
docker run -d -p 2012:2012 -e BOLT_ADMIN_PASSWORD=secret bolt:latest

# Connect
BOLT_USER=admin BOLT_PASSWORD=secret bolt-ctl put mykey "Hello, Bolt!"
BOLT_USER=admin BOLT_PASSWORD=secret bolt-ctl get mykey
```

### Using Python SDK

```python
from bolt_client import BoltClient

client = BoltClient(
    host="127.0.0.1",
    port=2012,
    username="admin",
    password="secret"
)

with client:
    # Basic operations
    client.put("user:1", "Alice")
    name = client.get("user:1")  # "Alice"

    # With TTL (60 seconds)
    client.setex("session:abc", "data", 60)

    # Batch operations
    client.mset({"k1": "v1", "k2": "v2", "k3": "v3"})
    values = client.mget(["k1", "k2", "k3"])  # ["v1", "v2", "v3"]
```

---

## Installation

### From Source

```bash
# Clone repository
git clone https://github.com/cloud55/bolt.git
cd bolt

# Build release
cargo build --release

# Binaries will be in target/release/
# - bolt-server: The database server
# - bolt-ctl: Command-line client
```

### Python SDK

```bash
cd clients/python
pip install -e .
```

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Bolt Server                                │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  │
│  │   TCP/TLS   │  │    Auth     │  │   Metrics   │  │  Cluster   │  │
│  │  Listener   │  │   Layer     │  │  Collector  │  │  Manager   │  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬─────┘  │
│         │                │                │                │        │
│         └────────────────┴────────────────┴────────────────┘        │
│                                  │                                  │
│                          ┌───────▼───────┐                          │
│                          │    Storage    │                          │
│                          │    Engine     │                          │
│                          └───────┬───────┘                          │
│                                  │                                  │
│         ┌────────────────────────┼────────────────────────┐         │
│         │                        │                        │         │
│  ┌──────▼──────┐         ┌───────▼───────┐       ┌───────▼───────┐  │
│  │   Sharded   │         │    Memory     │       │     WAL       │  │
│  │   Storage   │         │   Tracker     │       │               │  │
│  │             |         │   + LRU       │       │               │  │
│  └─────────────┘         └───────────────┘       └───────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Client Request
      │
      ▼
┌─────────────┐
│  TCP/TLS    │
│  Accept     │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Auth      │──── Fail ──▶ Reject
│  Validate   │
└──────┬──────┘
       │ Pass
       ▼
┌─────────────┐
│   Parse     │
│  Message    │
└──────┬──────┘
       │
       ▼
┌─────────────┐         ┌─────────────┐
│   Check     │────────▶│   Memory    │
│  Memory     │         │   Eviction  │
└──────┬──────┘         └─────────────┘
       │
       ▼
┌─────────────┐         ┌─────────────┐
│  Execute    │────────▶│   WAL       │
│  Operation  │         │   Write     │
└──────┬──────┘         └─────────────┘
       │
       ▼
┌─────────────┐         ┌─────────────┐
│  Update     │────────▶│  Replicate  │
│  Metrics    │         │  to Peers   │
└──────┬──────┘         └─────────────┘
       │
       ▼
   Response
```

---

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| **Server** | | |
| `BOLT_HOST` | Listen address | `0.0.0.0` |
| `BOLT_PORT` | Client port | `2012` |
| `BOLT_DATA_DIR` | Data directory | `/data` |
| `BOLT_PERSIST` | Enable persistence | `true` |
| `BOLT_MAX_CONNECTIONS` | Max concurrent connections | `1000` |
| **Performance** | | |
| `BOLT_HIGH_PERF` | Enable high-performance mode | `false` |
| `BOLT_SHARD_COUNT` | Number of shards | `64` |
| `BOLT_WAL_BATCH_SIZE` | WAL batch size | `1000` |
| `BOLT_WAL_FLUSH_INTERVAL_MS` | WAL flush interval | `10` |
| **Memory** | | |
| `BOLT_MAX_MEMORY` | Memory limit (`1gb`, `512mb`, `75%`) | `75%` of RAM |
| `BOLT_EVICTION_POLICY` | Eviction policy | `allkeys-lru` |
| `BOLT_MEMORY_WARNING_THRESHOLD` | Warning at % | `90` |
| **Cluster** | | |
| `BOLT_NODE_ID` | Unique node identifier | - |
| `BOLT_CLUSTER_PORT` | Cluster communication port | `2013` |
| `BOLT_CLUSTER_PEERS` | Peer list `id:host:port,...` | - |
| `BOLT_PENDING_DRAIN_THRESHOLD` | Max pending entries before Full Sync | `10000` |
| `BOLT_PENDING_DRAIN_BATCH_SIZE` | Entries per drain batch | `200` |
| `BOLT_PENDING_DRAIN_BATCH_DELAY_MS` | Delay between drain batches (ms) | `20` |
| `BOLT_PENDING_DRAIN_MAX_ERRORS` | Max errors before aborting drain | `3` |
| **Security** | | |
| `BOLT_ADMIN_PASSWORD` | Admin user password | - |
| `BOLT_TLS_CERT` | TLS certificate path | - |
| `BOLT_TLS_KEY` | TLS private key path | - |
| **Logging** | | |
| `RUST_LOG` | Log level | `info` |
| **Metrics** | | |
| `BOLT_METRICS_PORT` | HTTP metrics port | `9091` |
| `BOLT_METRICS_ENABLED` | Enable HTTP metrics endpoint | `true` |

### Eviction Policies

| Policy | Description |
|--------|-------------|
| `noeviction` | Return error when memory full |
| `allkeys-lru` | Evict least recently used keys (default) |
| `allkeys-random` | Evict random keys (faster) |
| `volatile-lru` | Evict LRU keys with TTL only |
| `volatile-ttl` | Evict keys closest to expiration |

---

## Python SDK

### Installation

```bash
pip install bolt-client
# or from source
cd clients/python && pip install -e .
```

### Basic Usage

```python
from bolt_client import BoltClient

# Connect to server
client = BoltClient(
    host="127.0.0.1",
    port=2012,
    username="admin",
    password="secret"
)
client.connect()

# --- String Operations ---
client.put("user:1:name", "Alice")
name = client.get("user:1:name")          # "Alice"
client.delete("user:1:name")

# With TTL (expires in 300 seconds)
client.setex("session:token123", "user_data", 300)
ttl = client.ttl("session:token123")      # 299

# Check existence
exists = client.exists("user:1:name")     # False

# --- Batch Operations ---
# Set multiple keys at once
client.mset({
    "config:theme": "dark",
    "config:lang": "en",
    "config:tz": "UTC"
})

# Get multiple keys at once
values = client.mget(["config:theme", "config:lang", "config:tz"])
# ["dark", "en", "UTC"]

# Delete multiple keys
deleted = client.mdel(["config:theme", "config:lang", "config:tz"])
# 3

# --- Counter Operations ---
client.put("stats:views", "0")
client.incr("stats:views")                # 1
client.incr("stats:views")                # 2
client.incrby("stats:views", 10)          # 12
client.decr("stats:views")                # 11

# --- List Operations ---
# Queue/Stack operations
client.rpush("queue:tasks", "task1", "task2", "task3")
length = client.llen("queue:tasks")       # 3
items = client.lrange("queue:tasks", 0, -1)  # ["task1", "task2", "task3"]
task = client.lpop("queue:tasks")         # "task1"

# --- Set Operations ---
client.sadd("tags:article:1", "python", "database", "cache")
members = client.smembers("tags:article:1")  # ["python", "database", "cache"]
is_tagged = client.sismember("tags:article:1", "python")  # True
count = client.scard("tags:article:1")    # 3
client.srem("tags:article:1", "cache")

# --- Server Info ---
stats = client.stats()
# {"active_connections": 5, "total_puts": 1000, ...}

cluster = client.cluster_status()
# {"node_id": "node1", "role": "master", "peers": [...]}

# Close connection
client.close()
```

### Context Manager

```python
from bolt_client import BoltClient

with BoltClient(host="127.0.0.1", port=2012, username="admin", password="secret") as client:
    client.put("key", "value")
    value = client.get("key")
# Connection automatically closed
```

### Cluster Mode

```python
from bolt_client import BoltClient

# Connect to cluster (connects to any available node)
client = BoltClient.cluster(
    nodes=["192.168.1.10:2012", "192.168.1.11:2012", "192.168.1.12:2012"],
    username="admin",
    password="secret"
)

with client:
    # All nodes accept reads and writes (multi-master)
    client.put("distributed_key", "value")
    value = client.get("distributed_key")

    # Check cluster status
    status = client.cluster_status()
    print(f"Connected to: {status['node_id']} (role: {status['role']})")
```

### Real-World Examples

#### Session Store

```python
import json
import uuid
from bolt_client import BoltClient

class SessionStore:
    def __init__(self, bolt_client: BoltClient, ttl_seconds: int = 3600):
        self.client = bolt_client
        self.ttl = ttl_seconds

    def create_session(self, user_id: str, data: dict) -> str:
        session_id = str(uuid.uuid4())
        session_data = json.dumps({
            "user_id": user_id,
            "data": data
        })
        self.client.setex(f"session:{session_id}", session_data, self.ttl)
        return session_id

    def get_session(self, session_id: str) -> dict | None:
        data = self.client.get(f"session:{session_id}")
        return json.loads(data) if data else None

    def delete_session(self, session_id: str):
        self.client.delete(f"session:{session_id}")

    def extend_session(self, session_id: str):
        data = self.client.get(f"session:{session_id}")
        if data:
            self.client.setex(f"session:{session_id}", data, self.ttl)

# Usage
with BoltClient(host="localhost", port=2012, username="admin", password="secret") as client:
    sessions = SessionStore(client, ttl_seconds=1800)  # 30 min sessions

    # Create session
    sid = sessions.create_session("user123", {"cart": ["item1", "item2"]})

    # Get session
    session = sessions.get_session(sid)
    print(session)  # {"user_id": "user123", "data": {"cart": ["item1", "item2"]}}
```

#### Rate Limiter

```python
import time
from bolt_client import BoltClient

class RateLimiter:
    def __init__(self, client: BoltClient, requests: int, window_seconds: int):
        self.client = client
        self.max_requests = requests
        self.window = window_seconds

    def is_allowed(self, identifier: str) -> bool:
        key = f"ratelimit:{identifier}"

        # Get current count
        count_str = self.client.get(key)

        if count_str is None:
            # First request in window
            self.client.setex(key, "1", self.window)
            return True

        count = int(count_str)
        if count >= self.max_requests:
            return False

        # Increment counter
        self.client.incr(key)
        return True

    def get_remaining(self, identifier: str) -> int:
        key = f"ratelimit:{identifier}"
        count_str = self.client.get(key)
        if count_str is None:
            return self.max_requests
        return max(0, self.max_requests - int(count_str))

# Usage - 100 requests per minute
with BoltClient(host="localhost", port=2012, username="admin", password="secret") as client:
    limiter = RateLimiter(client, requests=100, window_seconds=60)

    user_ip = "192.168.1.100"

    if limiter.is_allowed(user_ip):
        print(f"Request allowed. Remaining: {limiter.get_remaining(user_ip)}")
    else:
        print("Rate limit exceeded. Try again later.")
```

#### Leaderboard

```python
from bolt_client import BoltClient

class Leaderboard:
    def __init__(self, client: BoltClient, name: str):
        self.client = client
        self.name = name
        self.scores_key = f"leaderboard:{name}:scores"
        self.players_key = f"leaderboard:{name}:players"

    def add_score(self, player: str, score: int):
        # Store score
        self.client.put(f"{self.scores_key}:{player}", str(score))
        # Add to players set
        self.client.sadd(self.players_key, player)

    def get_score(self, player: str) -> int | None:
        score = self.client.get(f"{self.scores_key}:{player}")
        return int(score) if score else None

    def increment_score(self, player: str, delta: int = 1) -> int:
        key = f"{self.scores_key}:{player}"
        if not self.client.exists(key):
            self.client.put(key, "0")
            self.client.sadd(self.players_key, player)
        return self.client.incrby(key, delta)

    def get_all_scores(self) -> dict[str, int]:
        players = self.client.smembers(self.players_key)
        scores = {}
        for player in players:
            score = self.get_score(player)
            if score is not None:
                scores[player] = score
        return dict(sorted(scores.items(), key=lambda x: x[1], reverse=True))

# Usage
with BoltClient(host="localhost", port=2012, username="admin", password="secret") as client:
    lb = Leaderboard(client, "game1")

    lb.add_score("alice", 100)
    lb.add_score("bob", 85)
    lb.increment_score("alice", 25)

    scores = lb.get_all_scores()
    # {"alice": 125, "bob": 85}
```

#### Cache Decorator

```python
import json
import hashlib
from functools import wraps
from bolt_client import BoltClient

def cached(client: BoltClient, ttl: int = 300, prefix: str = "cache"):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            key_data = f"{func.__name__}:{args}:{sorted(kwargs.items())}"
            cache_key = f"{prefix}:{hashlib.md5(key_data.encode()).hexdigest()}"

            # Try to get from cache
            cached_value = client.get(cache_key)
            if cached_value is not None:
                return json.loads(cached_value)

            # Execute function and cache result
            result = func(*args, **kwargs)
            client.setex(cache_key, json.dumps(result), ttl)
            return result
        return wrapper
    return decorator

# Usage
client = BoltClient(host="localhost", port=2012, username="admin", password="secret")
client.connect()

@cached(client, ttl=60)
def get_user_profile(user_id: int) -> dict:
    # Expensive database query
    import time
    time.sleep(1)  # Simulate slow query
    return {"id": user_id, "name": f"User {user_id}", "email": f"user{user_id}@example.com"}

# First call - slow (1 second)
profile = get_user_profile(123)

# Second call - instant (from cache)
profile = get_user_profile(123)
```

---

## CLI Reference

### Basic Commands

```bash
# Set environment variables
export BOLT_HOST=127.0.0.1
export BOLT_PORT=2012
export BOLT_USER=admin
export BOLT_PASSWORD=secret

# Key-Value
bolt-ctl put <key> <value> [database]
bolt-ctl get <key> [database]
bolt-ctl del <key> [database]

# TTL
bolt-ctl setex <key> <ttl_seconds> <value> [database]
bolt-ctl ttl <key> [database]

# Batch
bolt-ctl mget <key1> <key2> ... [--db database]
bolt-ctl mset <key1> <val1> <key2> <val2> ... [--db database]
bolt-ctl mdel <key1> <key2> ... [--db database]

# Counters
bolt-ctl incr <key> [database]
bolt-ctl decr <key> [database]
bolt-ctl incrby <key> <delta> [database]

# CRDT Counters (cluster-safe)
bolt-ctl cincr <key> [database]
bolt-ctl cdecr <key> [database]
bolt-ctl cget <key> [database]

# Lists
bolt-ctl lpush <key> <val1> [val2...] [--db database]
bolt-ctl rpush <key> <val1> [val2...] [--db database]
bolt-ctl lpop <key> [database]
bolt-ctl rpop <key> [database]
bolt-ctl lrange <key> <start> <stop> [database]
bolt-ctl llen <key> [database]

# Sets
bolt-ctl sadd <key> <member1> [member2...] [--db database]
bolt-ctl srem <key> <member1> [member2...] [--db database]
bolt-ctl smembers <key> [database]
bolt-ctl scard <key> [database]
bolt-ctl sismember <key> <member> [database]

# Utility
bolt-ctl exists <key> [database]
bolt-ctl type <key> [database]
bolt-ctl keys <pattern> [database]

# User Management (admin only)
bolt-ctl useradd <username> <password> <role>  # role: admin|readwrite|readonly
bolt-ctl userdel <username>
bolt-ctl users
bolt-ctl passwd <username> <new_password>
bolt-ctl whoami

# Server
bolt-ctl stats
bolt-ctl metrics
bolt-ctl cluster
```

### Examples

```bash
# Store JSON data
bolt-ctl put user:1 '{"name":"Alice","age":30}'

# Get value
bolt-ctl get user:1

# Store with 1 hour TTL
bolt-ctl setex session:abc 3600 "session_data"

# Check TTL
bolt-ctl ttl session:abc

# Batch operations
bolt-ctl mset k1 v1 k2 v2 k3 v3
bolt-ctl mget k1 k2 k3

# Counter
bolt-ctl put counter 0
bolt-ctl incr counter
bolt-ctl incrby counter 100

# List as queue
bolt-ctl rpush queue task1 task2 task3
bolt-ctl lpop queue

# Get cluster status
bolt-ctl cluster
```

---

## Clustering

### Overview

Bolt supports multi-master clustering where all nodes accept reads and writes.

### Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Node 1    │◄───►│   Node 2    │◄───►│   Node 3    │
│   MASTER    │     │   MASTER    │     │   MASTER    │
│   R/W       │     │   R/W       │     │   R/W       │
│  port 2012  │     │  port 2022  │     │  port 2032  │
│  sync 2013  │     │  sync 2023  │     │  sync 2033  │
└─────────────┘     └─────────────┘     └─────────────┘
       │                  │                   │
       └──────────────────┴───────────────────┘
              Peer-to-Peer Sync (eventual consistency)
```

**Key Benefits:**
- All nodes accept reads AND writes
- No single point of failure
- Horizontal write scalability
- Clients connect to any node

### Resilient Replication

Bolt handles node failures gracefully with a two-tier replication strategy:

1. **Pending Queue**: When a peer is offline, replication entries are queued
2. **Drain**: When peer comes back online, queued entries are replayed (rate-limited)
3. **Full Sync**: For large queues (>10K entries), Full Sync is used instead

```
Node Offline → Queue entries → Node Online → Check queue size
                                                    │
                              ┌─────────────────────┴─────────────────────┐
                              │                                           │
                         ≤ 10K entries                              > 10K entries
                              │                                           │
                         Rate-limited                                Full Sync
                         Drain (batches)                           (complete state)
```

**Drain Configuration:**
- Threshold: 10,000 entries (configurable via `BOLT_PENDING_DRAIN_THRESHOLD`)
- Batch size: 200 entries per batch
- Batch delay: 20ms between batches
- Circuit breaker: Aborts after 3 consecutive errors

### Docker Compose Cluster

```yaml
# docker-compose.cluster.yml
version: '3.8'

services:
  bolt-node1:
    image: bolt:latest
    ports:
      - "2012:2012"
      - "2013:2013"
    environment:
      - BOLT_NODE_ID=node1
      - BOLT_CLUSTER_PORT=2013
      - BOLT_CLUSTER_PEERS=node2:bolt-node2:2013,node3:bolt-node3:2013
      - BOLT_ADMIN_PASSWORD=admin

  bolt-node2:
    image: bolt:latest
    ports:
      - "2022:2012"
      - "2023:2013"
    environment:
      - BOLT_NODE_ID=node2
      - BOLT_CLUSTER_PORT=2013
      - BOLT_CLUSTER_PEERS=node1:bolt-node1:2013,node3:bolt-node3:2013
      - BOLT_ADMIN_PASSWORD=admin

  bolt-node3:
    image: bolt:latest
    ports:
      - "2032:2012"
      - "2033:2013"
    environment:
      - BOLT_NODE_ID=node3
      - BOLT_CLUSTER_PORT=2013
      - BOLT_CLUSTER_PEERS=node1:bolt-node1:2013,node2:bolt-node2:2013
      - BOLT_ADMIN_PASSWORD=admin
```

```bash
# Start cluster
docker compose -f docker-compose.cluster.yml up -d

# Check cluster status
BOLT_PORT=2012 bolt-ctl cluster
```

---

## Memory Management

### Overview

Bolt includes built-in memory management to prevent OOM conditions:

- **Default limit**: 75% of system RAM
- **Automatic eviction**: Removes least-used keys when limit reached
- **O(1) LRU tracking**: Efficient doubly-linked list implementation

### Configuration

```bash
# Set memory limit
BOLT_MAX_MEMORY=2gb ./bolt-server

# Or as percentage
BOLT_MAX_MEMORY=50% ./bolt-server

# Choose eviction policy
BOLT_EVICTION_POLICY=allkeys-lru ./bolt-server

# Set warning threshold
BOLT_MEMORY_WARNING_THRESHOLD=80 ./bolt-server
```

### Eviction Policies

| Policy | Best For |
|--------|----------|
| `allkeys-lru` | General caching (default) |
| `volatile-lru` | Cache with some persistent keys |
| `volatile-ttl` | Time-sensitive data |
| `allkeys-random` | Highest performance |
| `noeviction` | When data loss is unacceptable |

### Monitoring

Bolt exposes an HTTP endpoint for Prometheus scraping on port 9091 by default.

```bash
# Prometheus metrics (HTTP endpoint)
curl http://localhost:9091/metrics

# Health check
curl http://localhost:9091/health

# Readiness check
curl http://localhost:9091/ready

# Or via CLI
bolt-ctl stats
bolt-ctl metrics
```

**Available Metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `bolt_active_connections` | gauge | Current active connections |
| `bolt_total_connections` | counter | Total connections since start |
| `bolt_operations_total{type}` | counter | Operations by type (get, put, del, mget, mset, etc.) |
| `bolt_keys_total` | gauge | Current number of keys in storage |
| `bolt_uptime_seconds` | counter | Server uptime in seconds |
| `bolt_pending_replication_total` | gauge | Pending replication entries (cluster mode) |
| `bolt_drain_operations_total` | counter | Total drain operations performed |
| `bolt_drain_skipped_total` | counter | Drains skipped (threshold exceeded, Full Sync used) |
| `bolt_drain_entries_total` | counter | Total entries successfully drained |

**Prometheus scrape config:**
```yaml
scrape_configs:
  - job_name: 'bolt'
    static_configs:
      - targets: ['localhost:9091']
```

### Grafana Dashboards

Bolt includes pre-built Grafana dashboards for cluster monitoring.

```bash
# Start monitoring stack (requires cluster to be running)
docker compose -f docker-compose.monitoring.yml up -d

# Access Grafana
open http://localhost:3001
# Login: admin / admin
```

**Available Dashboards:**
- **Bolt Cluster**: Overview of all nodes, operations, keys, and replication status
- **Replication Drain**: Pending replication, drain operations, and Full Sync metrics

**Monitoring Stack:**
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001`

---

## Docker

### Single Node

```bash
docker run -d \
  --name bolt \
  -p 2012:2012 \
  -v bolt_data:/data \
  -e BOLT_ADMIN_PASSWORD=secret \
  -e BOLT_MAX_MEMORY=1gb \
  bolt:latest
```

### Build from Source

```bash
docker build -t bolt:latest .
```

### Docker Compose

```yaml
version: '3.8'

services:
  bolt:
    build: .
    ports:
      - "2012:2012"
    volumes:
      - bolt_data:/data
    environment:
      - BOLT_ADMIN_PASSWORD=secret
      - BOLT_PERSIST=true
      - BOLT_HIGH_PERF=true
      - BOLT_MAX_MEMORY=2gb
    restart: unless-stopped

volumes:
  bolt_data:
```

---

## Benchmarks

### Test Environment
- CPU: Apple M1 Pro
- RAM: 16GB
- OS: macOS 14

### Results

| Operation | Throughput | Latency (p50) | Latency (p99) |
|-----------|------------|---------------|---------------|
| SET | 520,000 ops/s | 0.8ms | 2.1ms |
| GET | 680,000 ops/s | 0.5ms | 1.2ms |
| MSET (100 keys) | 85,000 batch/s | 4.2ms | 8.5ms |
| MGET (100 keys) | 110,000 batch/s | 3.1ms | 6.2ms |
| INCR | 490,000 ops/s | 0.9ms | 2.3ms |

### Running Benchmarks

```bash
# Build benchmark tool
cargo build --release -p bolt-bench

# Run benchmark
./target/release/bolt-bench
```

---

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing`)
5. Open Pull Request

### Development

```bash
# Run tests
cargo test --all

# Run with debug logging
RUST_LOG=debug cargo run -p bolt-server

# Format code
cargo fmt

# Lint
cargo clippy
```

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---