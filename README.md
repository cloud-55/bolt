# Bolt

A high-performance, in-memory key-value database written in Rust.

## Features

- **Data Types**: Strings, Counters, Lists, Sets
- **Persistence**: Write-Ahead Log (WAL) with CRC32 checksums
- **Performance**: Async I/O, LZ4 compression for large values
- **Operations**: GET/PUT/DEL, MGET/MSET, INCR/DECR, LPUSH/RPUSH, SADD/SREM
- **TTL**: Key expiration with lazy + active cleanup
- **CLI**: Interactive client (boltctl)

## Quick Start

```bash
cargo run --bin bolt     # Start server
cargo run --bin boltctl  # Start CLI
```

## Data Types

| Type | Commands |
|------|----------|
| String | GET, PUT, DEL, MGET, MSET |
| Counter | INCR, DECR |
| List | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN |
| Set | SADD, SREM, SMEMBERS, SISMEMBER, SCARD |

## Architecture

```
Client → TCP → Server → Storage Engine
                            │
                  ┌─────────┼──────────┐
                  │         │          │
               Strings  Counters    Lists/Sets
                  │
              Compression (LZ4)
                  │
                 WAL
```

## License

MIT
