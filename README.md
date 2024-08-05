# Bolt

High-performance, in-memory key-value database with multi-master replication.

## Features

- **Storage**: Strings, Counters, Lists, Sets with LZ4 compression
- **Persistence**: Write-Ahead Log (WAL) with CRC32 checksums
- **Clustering**: Multi-master replication with LWW conflict resolution
- **CRDT**: Distributed counters that converge across nodes
- **TTL**: Key expiration with lazy + active cleanup
- **Performance**: Async I/O with Tokio

## Quick Start

```bash
cargo run --bin bolt
```

## Clustering

Bolt supports multi-master replication:

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Node 1  │◄───►│  Node 2  │◄───►│  Node 3  │
│ (master) │     │ (master) │     │ (master) │
└──────────┘     └──────────┘     └──────────┘
```

- All nodes accept reads and writes
- Async replication to peers
- LWW conflict resolution
- CRDT counters for distributed counting
- Pending queue with drain for offline peers
- Full sync for large divergence

## Data Types

| Type | Commands |
|------|----------|
| String | GET, PUT, DEL, MGET, MSET |
| Counter | INCR, DECR |
| List | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN |
| Set | SADD, SREM, SMEMBERS, SISMEMBER, SCARD |
| CRDT Counter | CINCR, CDECR, CGET |

## License

MIT
