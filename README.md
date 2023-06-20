# Bolt

A high-performance, in-memory key-value database written in Rust.

## Features

- In-memory key-value storage
- Binary wire protocol over TCP
- Async I/O with Tokio
- CLI client (boltctl)

## Quick Start

```bash
cargo run --bin bolt
```

In another terminal:
```bash
cargo run --bin boltctl
```

## Commands

- `PUT key value` - Store a value
- `GET key` - Retrieve a value
- `DEL key` - Delete a value
- `PING` - Health check

## License

MIT
