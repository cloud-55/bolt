# Bolt

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Docker Pulls](https://img.shields.io/docker/pulls/cloud55io/bolt)](https://hub.docker.com/r/cloud55io/bolt)
[![Docker Image Size](https://img.shields.io/docker/image-size/cloud55io/bolt/latest)](https://hub.docker.com/r/cloud55io/bolt)
[![GitHub Stars](https://img.shields.io/github/stars/cloud-55/bolt)](https://github.com/cloud-55/bolt)
[![GitHub Contributors](https://img.shields.io/github/contributors/cloud-55/bolt)](https://github.com/cloud-55/bolt/graphs/contributors)
[![GitHub Issues](https://img.shields.io/github/issues/cloud-55/bolt)](https://github.com/cloud-55/bolt/issues)

> The state engine for AI agents: fast, distributed, and built for agentic workloads.

Bolt is a high-performance in-memory database designed for AI agent infrastructure. It provides the stateful backbone that autonomous agents need: ultra-low latency state management, distributed coordination, and real-time synchronization across agent swarms.

<img src="./assets/demoo.gif" width="100%" />

## Why Bolt for AI Agents?

Modern AI agents need persistent state that's **faster than their decision loop**. Traditional databases add latency that breaks the real-time feedback agents require. Bolt solves this with:

| Challenge | Bolt Solution |
|-----------|---------------|
| **Agent State** | Sub-millisecond GET/SET for working memory, goals, and context |
| **Multi-Agent Coordination** | Multi-master clustering with CRDT counters for distributed consensus |
| **Swarm Synchronization** | Peer-to-peer replication with eventual consistency across nodes |
| **Reliability** | Write-Ahead Log ensures state survives agent restarts |

## Features

| Category | Features |
|----------|----------|
| **Performance** | ~500k ops/sec, sub-ms latency, sharded storage, batched WAL |
| **Data Structures** | Strings, Lists, Sets, Counters, CRDT Counters |
| **Operations** | GET/SET/DEL, MGET/MSET/MDEL, TTL, INCR/DECR |
| **Clustering** | Multi-master replication, peer-to-peer sync, no single point of failure |
| **Persistence** | Write-Ahead Log (WAL), automatic recovery |
| **Memory** | Configurable limits, LRU eviction, 5 eviction policies |
| **Security** | Authentication, role-based access (admin/readwrite/readonly), TLS |
| **Observability** | Prometheus metrics, health checks |

---

## Multi-Agent Task Coordination

Coordinate task distribution across agent swarms with automatic conflict resolution. The `TaskCoordinator` from `bolt-agents` handles the workflow: **orchestrators** create tasks, **agents** subscribe and execute them.

### Examples

```python
# orchestrator_api.py: Creates tasks through REST API for agents pick up
from fastapi import FastAPI
from bolt_client import BoltClient
from bolt_agents import TaskCoordinator

app = FastAPI()
client = BoltClient(host="localhost", port=8518, username="admin", password="secret")
client.connect()
coord = TaskCoordinator(client)

@app.post("/tasks")
def create_task(body: dict):
    """External systems call this API to create tasks."""
    task = coord.create_task(body["type"], body.get("data", {}))
    return {"task_id": task.id, "status": task.status}

@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    """Get task status and result."""
    task = coord.get_task(task_id)
    if not task:
        return {"error": "not found"}, 404
    return {"status": task.status, "result": task.result}

# curl -X POST http://localhost:8000/tasks -H "Content-Type: application/json" -d '{"type": "research", "data": {"query": "AI safety"}}'
```

```python
# meta_agent.py: LLM decides which tasks to create
from openai import OpenAI
from bolt_client import BoltClient
from bolt_agents import TaskCoordinator, Task
import json

# Define available task types with descriptions for the LLM
TASK_TYPES = {
    "research": "Search and gather information on a topic. Input: {query: string}",
    "summarize": "Create a concise summary of content. Input: {content: string}",
    "analyze": "Perform data analysis. Input: {data: object, question: string}",
}

SYSTEM_PROMPT = f"""You are a task planner. Given a user request, decide which tasks to create.

Available task types:
{json.dumps(TASK_TYPES, indent=2)}

Respond with JSON: {{"tasks": [{{"type": "<type>", ...input_fields}}]}}

Examples:
- "What's happening with AI?" -> {{"tasks": [{{"type": "research", "query": "recent AI developments"}}]}}
- "Explain this data" -> {{"tasks": [{{"type": "analyze", "data": ..., "question": "..."}}]}}
"""

openai_client = OpenAI()
client = BoltClient(host="localhost", port=8518, username="admin", password="secret")
client.connect()
coord = TaskCoordinator(client)

def process_user_request(user_input: str) -> list[Task]:
    response = openai_client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_input}
        ],
        response_format={"type": "json_object"}
    )

    plan = json.loads(response.choices[0].message.content)
    tasks = []
    for item in plan["tasks"]:
        if item["type"] in TASK_TYPES:  # Validate task type
            task = coord.create_task(item["type"], item)
            tasks.append(task)
    return tasks

# Usage: tasks = process_user_request("I need to understand the impact of AI on healthcare")
# LLM will create: [Task(type="research", query="impact of AI on healthcare")]
```

```python
# workflow.py: Static pipeline where tasks chain together
from bolt_client import BoltClient
from bolt_agents import TaskCoordinator

client = BoltClient(host="localhost", port=8518, username="admin", password="secret")
client.connect()
coord = TaskCoordinator(client)

def run_pipeline(query: str):
    """Pipeline: research -> summarize (output of one feeds into next)."""
    # Step 1: Research
    research_task = coord.create_task("research", {"query": query})
    research_task = coord.wait_for_task(research_task.id)

    if research_task.status == "failed":
        raise Exception(f"Research failed: {research_task.result}")

    # Step 2: Summarize (uses output from step 1)
    summarize_task = coord.create_task("summarize", {
        "content": research_task.result["findings"]
    })
    summarize_task = coord.wait_for_task(summarize_task.id)

    return summarize_task.result

# Usage: result = run_pipeline("impact of AI on healthcare")
```

**Specialized agent workers**: subscribe to task types and receive push notifications:

```python
# research_agent.py
from bolt_client import BoltClient
from bolt_agents import TaskCoordinator, Task

client = BoltClient(host="localhost", port=8518, username="admin", password="secret")
client.connect()
coord = TaskCoordinator(client)

# Called when a research task is available
def handle_research(task: Task):
    print(f"Researching: {task.data['query']}")

    # ... Implementation here
    result = {"findings": f"Research findings for: {task.data['query']}", "sources": ["..."]}

    coord.complete_task(task.id, result)

# Subscribe and wait for tasks
coord.subscribe("research", agent_id="research_agent_01", callback=handle_research)
```

```python
# summarize_agent.py
from bolt_client import BoltClient
from bolt_agents import TaskCoordinator, Task

client = BoltClient(host="localhost", port=8518, username="admin", password="secret")
client.connect()
coord = TaskCoordinator(client)

# Called when a summarize task is available
def handle_summarize(task: Task):
    content = task.data.get("content") or task.data.get("doc_id")

    # ... Implementation here
    result = {"summary": f"Summary of: {content[:50]}...", "word_count": 150}

    coord.complete_task(task.id, result)

coord.subscribe("summarize", agent_id="summarize_agent_01", callback=handle_summarize)
```

Run a swarm of specialized agents:

```bash
# Terminal 1: API orchestrator
uvicorn orchestrator_api:app --port 8000

# Terminal 2-3: Research agents (subscribe and wait for tasks)
python research_agent.py
python research_agent.py

# Terminal 4: Summarize agent
python summarize_agent.py

# Create tasks via API: agents are notified instantly
curl -X POST http://localhost:8000/tasks -H "Content-Type: application/json" \
  -d '{"type": "research", "data": {"query": "AI safety"}}'

curl -X POST http://localhost:8000/tasks -H "Content-Type: application/json" \
  -d '{"type": "summarize", "data": {"doc_id": "doc_123"}}'
```

---

## Table of Contents

- [Architecture](#architecture)
- [Python SDK](#python-sdk)
- [Configuration](#configuration)
- [Clustering](#clustering)
- [CLI Reference](#cli-reference)
- [Memory Management](#memory-management)
- [Docker](#docker)
- [Benchmarks](#benchmarks)

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

### Multi-Agent Cluster Architecture

```
                              ┌─────────────────────┐
                              │    Orchestrator     │
                              └──────────┬──────────┘
                                         │ create_task()
                                         ▼
┌─────────┐                 ┌─────────────────────────────────────┐
│  Agent  │────subscribe───►│                                     │
└─────────┘                 │         ┌───────────┐               │
                            │         │ Bolt Node │               │
┌─────────┐                 │         └─────┬─────┘               │
│  Agent  │────subscribe───►│               │ replication         │
└─────────┘                 │         ┌─────┴─────┐               │
                            │         │ Bolt Node │               │
┌─────────┐                 │         └─────┬─────┘               │
│  Agent  │────subscribe───►│               │ replication         │
└─────────┘                 │         ┌─────┴─────┐               │
                            │         │ Bolt Node │               │
┌─────────┐                 │         └───────────┘               │
│  Agent  │────subscribe───►│                                     │
└─────────┘                 └─────────────────────────────────────┘
                                        Bolt Cluster
```

---

## Installation

### Quick Install (Linux/macOS)

```bash
curl -fsSL https://raw.githubusercontent.com/cloud-55/bolt/main/install.sh | bash
```

This installs `bolt` (server) and `boltctl` (CLI) to `/usr/local/bin`.

### Docker

```bash
docker run -p 8518:8518 cloud55io/bolt
```

### From Source

```bash
git clone https://github.com/cloud-55/bolt.git
cd bolt
cargo build --release

# Binaries in target/release/
# - bolt: The database server
# - boltctl: Command-line client
```

### Python SDKs

```bash
# Core client (KV operations)
pip install bolt-client

# Agent coordination (requires bolt-client)
pip install bolt-agents

# Or from source
cd clients/python && pip install -e .
```

---

## Python SDK

### Basic Usage

```python
from bolt_client import BoltClient

client = BoltClient(
    host="127.0.0.1",
    port=8518,
    username="admin",
    password="secret"
)
client.connect()

# String operations
client.put("key", "value")
value = client.get("key")
client.delete("key")

# TTL (expires in 300 seconds)
client.setex("session:token", "data", 300)
ttl = client.ttl("session:token")

# Batch operations
client.mset({"k1": "v1", "k2": "v2", "k3": "v3"})
values = client.mget(["k1", "k2", "k3"])
client.mdel(["k1", "k2", "k3"])

# Counters
client.put("counter", "0")
client.incr("counter")
client.incrby("counter", 10)

# Lists (queues)
client.rpush("queue", "task1", "task2", "task3")
task = client.lpop("queue")
items = client.lrange("queue", 0, -1)

# Sets
client.sadd("tags", "python", "ai", "agents")
members = client.smembers("tags")
is_member = client.sismember("tags", "python")

# CRDT Counters (cluster-safe)
client.cincr("distributed_counter")
client.cdecr("distributed_counter")
count = client.cget("distributed_counter")

client.close()
```

### Context Manager

```python
with BoltClient(host="127.0.0.1", port=8518, username="admin", password="secret") as client:
    client.put("key", "value")
# Connection automatically closed
```

### Cluster Mode

```python
client = BoltClient.cluster(
    nodes=["192.168.1.10:8518", "192.168.1.11:8518", "192.168.1.12:8518"],
    username="admin",
    password="secret"
)

with client:
    # All nodes accept reads and writes (multi-master)
    client.put("key", "value")
    status = client.cluster_status()
```

---

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| **Server** | | |
| `BOLT_HOST` | Listen address | `0.0.0.0` |
| `BOLT_PORT` | Client port | `8518` |
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
| `BOLT_CLUSTER_PORT` | Cluster communication port | `8519` |
| `BOLT_CLUSTER_PEERS` | Peer list `id:host:port,...` | - |
| **Security** | | |
| `BOLT_ADMIN_PASSWORD` | Admin user password | - |
| `BOLT_TLS_CERT` | TLS certificate path | - |
| `BOLT_TLS_KEY` | TLS private key path | - |
| **Observability** | | |
| `RUST_LOG` | Log level | `info` |
| `BOLT_METRICS_PORT` | HTTP metrics port | `9091` |
| `BOLT_METRICS_ENABLED` | Enable metrics endpoint | `true` |

### Eviction Policies

| Policy | Best For |
|--------|----------|
| `allkeys-lru` | General agent state caching (default) |
| `volatile-lru` | Mix of persistent config + ephemeral state |
| `volatile-ttl` | Time-sensitive data (sessions, tool cache) |
| `allkeys-random` | Maximum throughput |
| `noeviction` | Critical state that must not be lost |

---

## Clustering

### Multi-Master Architecture

True multi-master: all nodes are equal, all accept reads and writes.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Node 1    │◄───►│   Node 2    │◄───►│   Node 3    │
│   MASTER    │     │   MASTER    │     │   MASTER    │
│   R/W       │     │   R/W       │     │   R/W       │
└─────────────┘     └─────────────┘     └─────────────┘
       │                  │                   │
       └──────────────────┴───────────────────┘
              Peer-to-Peer Sync (eventual consistency)
```

### Docker Compose Cluster

```yaml
version: '3.8'

services:
  bolt-node1:
    image: bolt:latest
    ports:
      - "8518:8518"
    environment:
      - BOLT_NODE_ID=node1
      - BOLT_CLUSTER_PORT=8519
      - BOLT_CLUSTER_PEERS=node2:bolt-node2:8519,node3:bolt-node3:8519
      - BOLT_ADMIN_PASSWORD=admin

  bolt-node2:
    image: bolt:latest
    ports:
      - "8528:8518"
    environment:
      - BOLT_NODE_ID=node2
      - BOLT_CLUSTER_PORT=8519
      - BOLT_CLUSTER_PEERS=node1:bolt-node1:8519,node3:bolt-node3:8519
      - BOLT_ADMIN_PASSWORD=admin

  bolt-node3:
    image: bolt:latest
    ports:
      - "8538:8518"
    environment:
      - BOLT_NODE_ID=node3
      - BOLT_CLUSTER_PORT=8519
      - BOLT_CLUSTER_PEERS=node1:bolt-node1:8519,node2:bolt-node2:8519
      - BOLT_ADMIN_PASSWORD=admin
```

### Resilient Replication

Bolt handles node failures gracefully:

1. **Pending Queue**: When a peer is offline, entries are queued
2. **Drain**: When peer returns, queued entries are replayed
3. **Full Sync**: For large queues (>10K entries), full state sync is used

---

## CLI Reference

```bash
# Environment
export BOLT_HOST=127.0.0.1
export BOLT_PORT=8518
export BOLT_USER=admin
export BOLT_PASSWORD=secret

# Key-Value
boltctl put <key> <value>
boltctl get <key>
boltctl del <key>

# TTL
boltctl setex <key> <ttl_seconds> <value>
boltctl ttl <key>

# Batch
boltctl mset <k1> <v1> <k2> <v2> ...
boltctl mget <k1> <k2> ...
boltctl mdel <k1> <k2> ...

# Counters
boltctl incr <key>
boltctl decr <key>
boltctl incrby <key> <delta>

# CRDT Counters (cluster-safe)
boltctl cincr <key>
boltctl cdecr <key>
boltctl cget <key>

# Lists
boltctl rpush <key> <val1> [val2...]
boltctl lpush <key> <val1> [val2...]
boltctl lpop <key>
boltctl rpop <key>
boltctl lrange <key> <start> <stop>

# Sets
boltctl sadd <key> <member1> [member2...]
boltctl srem <key> <member1> [member2...]
boltctl smembers <key>
boltctl sismember <key> <member>

# Server
boltctl stats
boltctl metrics
boltctl cluster
```

---

## Memory Management

### Configuration

```bash
# Absolute limit
BOLT_MAX_MEMORY=2gb ./bolt-server

# Percentage of system RAM
BOLT_MAX_MEMORY=50% ./bolt-server

# Eviction policy
BOLT_EVICTION_POLICY=allkeys-lru ./bolt-server
```

### Monitoring

```bash
# Prometheus metrics
curl http://localhost:9091/metrics

# Health check
curl http://localhost:9091/health

# CLI
boltctl stats
```

---

## Docker

### Single Node

```bash
docker run -d \
  --name bolt \
  -p 8518:8518 \
  -v bolt_data:/data \
  -e BOLT_ADMIN_PASSWORD=secret \
  -e BOLT_MAX_MEMORY=1gb \
  bolt:latest
```

### Build from Source

```bash
docker build -t bolt:latest .
```

---

## Benchmarks

### Test Environment
- CPU: Apple M1 Pro
- RAM: 16GB

### Results

| Operation | Throughput | Latency (p50) | Latency (p99) |
|-----------|------------|---------------|---------------|
| SET | 520,000 ops/s | 0.8ms | 2.1ms |
| GET | 680,000 ops/s | 0.5ms | 1.2ms |
| MSET (100 keys) | 85,000 batch/s | 4.2ms | 8.5ms |
| MGET (100 keys) | 110,000 batch/s | 3.1ms | 6.2ms |
| INCR | 490,000 ops/s | 0.9ms | 2.3ms |

```bash
# Run benchmarks
cargo build --release -p bolt-bench
./target/release/bolt-bench
```

---

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing`)
5. Open Pull Request

```bash
# Development
cargo test --all
cargo fmt
cargo clippy
```

---

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
