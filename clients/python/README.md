# Bolt Python Client

Python client library for [Bolt](https://github.com/cloud55/bolt) key-value database.

## Installation

```bash
# From source
cd clients/python
pip install -e .

# Or directly
pip install bolt-client
```

## Quick Start

### Single Node Connection

```python
from bolt_client import BoltClient

# Connect to a single node
client = BoltClient(
    host="127.0.0.1",
    port=2012,
    username="admin",
    password="admin"
)

# Basic operations
client.put("name", "Alice")
name = client.get("name")  # "Alice"
client.delete("name")
```

### Cluster Connection

```python
from bolt_client import BoltClient

# Connect to a cluster (multi-master)
client = BoltClient.cluster(
    nodes=["127.0.0.1:2012", "127.0.0.1:2022", "127.0.0.1:2032"],
    username="admin",
    password="admin"
)

# All nodes accept reads and writes
client.put("key", "value")
value = client.get("key")
```

### Context Manager

```python
from bolt_client import BoltClient

with BoltClient(host="127.0.0.1", port=2012, username="admin", password="admin") as client:
    client.put("key", "value")
    value = client.get("key")
# Connection automatically closed
```

## API Reference

### Key-Value Operations

```python
# Set a key
client.put("key", "value")

# Get a key (returns None if not found)
value = client.get("key")

# Delete a key (returns deleted value or None)
deleted = client.delete("key")

# Set with TTL (expires after 60 seconds)
client.setex("session", "data", ttl_seconds=60)

# Get TTL (-1 = no expiration, None = key not found)
ttl = client.ttl("session")

# Check if key exists
exists = client.exists("key")

# Get key type
key_type = client.key_type("key")  # "string", "list", "set", or None

# Get keys matching pattern
keys = client.keys("user:*")
```

### Batch Operations

```python
# Get multiple keys
values = client.mget(["key1", "key2", "key3"])

# Set multiple keys
client.mset({"key1": "val1", "key2": "val2"})

# Delete multiple keys
deleted_count = client.mdel(["key1", "key2"])
```

### Counter Operations

```python
# Increment by 1
new_value = client.incr("counter")

# Decrement by 1
new_value = client.decr("counter")

# Increment by specific amount
new_value = client.incrby("counter", 10)
new_value = client.incrby("counter", -5)  # decrement
```

### List Operations

```python
# Push to head
length = client.lpush("mylist", "a", "b", "c")

# Push to tail
length = client.rpush("mylist", "x", "y", "z")

# Pop from head
value = client.lpop("mylist")

# Pop from tail
value = client.rpop("mylist")

# Get range (0 to -1 = all)
items = client.lrange("mylist", 0, -1)

# Get length
length = client.llen("mylist")
```

### Set Operations

```python
# Add members
added = client.sadd("myset", "a", "b", "c")

# Remove members
removed = client.srem("myset", "a")

# Get all members
members = client.smembers("myset")

# Get set size
size = client.scard("myset")

# Check membership
is_member = client.sismember("myset", "b")
```

### Server Operations

```python
# Get server stats
stats = client.stats()

# Get Prometheus metrics
metrics = client.metrics()

# Get cluster status
cluster = client.cluster_status()
```

### Using Databases

```python
# Set default database
client = BoltClient(host="127.0.0.1", port=2012, database="mydb")

# Or specify per-operation
client.put("key", "value", database="other_db")
value = client.get("key", database="other_db")
```

## Exception Handling

```python
from bolt_client import (
    BoltClient,
    BoltError,
    ConnectionError,
    AuthenticationError,
    KeyNotFoundError,
    PermissionError,
    ClusterError,
)

try:
    client = BoltClient(host="127.0.0.1", port=2012)
    client.connect()
except ConnectionError as e:
    print(f"Failed to connect: {e}")
except AuthenticationError as e:
    print(f"Auth failed: {e}")
except PermissionError as e:
    print(f"Permission denied: {e}")
except ClusterError as e:
    print(f"Cluster error: {e}")
except BoltError as e:
    print(f"Bolt error: {e}")
```

## Configuration Options

```python
client = BoltClient(
    host="127.0.0.1",      # Server host
    port=2012,             # Server port
    username="admin",      # Auth username
    password="admin",      # Auth password
    database="default",    # Default database
    timeout=30.0,          # Connection timeout (seconds)
    auto_reconnect=True,   # Auto-reconnect on connection loss
)
```

## Thread Safety

The `BoltClient` class is **not** thread-safe. For multi-threaded applications, create a separate client instance per thread or use a connection pool.

## License

Apache 2.0
