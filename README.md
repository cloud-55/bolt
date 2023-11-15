# Bolt

A high-performance, in-memory key-value database written in Rust.

## Features

- In-memory key-value storage with TTL support
- Binary wire protocol over TCP
- Async I/O with Tokio
- Write-Ahead Log (WAL) for persistence
- Batch operations (MGET/MSET)
- Atomic counters (INCR/DECR)
- Pattern matching (KEYS)
- Multiple databases (SELECT)
- CLI client (boltctl)

## Quick Start

```bash
cargo run --bin bolt
```

## Commands

| Command | Description |
|---------|-------------|
| PUT key value [TTL] | Store a value with optional TTL |
| GET key | Retrieve a value |
| DEL key | Delete a value |
| MGET key1 key2 ... | Get multiple values |
| MSET key1 val1 key2 val2 ... | Set multiple values |
| INCR key | Increment counter |
| DECR key | Decrement counter |
| KEYS pattern | Find keys matching pattern |
| EXISTS key | Check if key exists |
| DBSIZE | Get number of keys |
| SELECT db | Switch database |
| PING | Health check |

## Architecture

```
Client → TCP → Server → Storage (in-memory)
                           ↓
                         WAL (disk)
```

## License

MIT
