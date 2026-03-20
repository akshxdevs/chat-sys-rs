# chat-sys-rs

Minimal Rust chat backend with Axum WebSockets, Kafka-backed message fan-out, and Redis-based offline replay.

[![CI](https://github.com/akshxdevs/chat-sys-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/akshxdevs/chat-sys-rs/actions/workflows/ci.yml)
[![Rust 2024](https://img.shields.io/badge/Rust-2024-000000?logo=rust)](message-system/Cargo.toml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

This repository is a Rust workspace centered on the `message-system` service:

- `message-system`: binary crate that starts the Axum server
- `message-system/services`: WebSocket routing, config loading, Kafka setup, Redis helpers, and shared state
- `message-system/api`: request and event payload types
- `message-system/store`: small store-related helpers

The service exposes:

- `GET /health`: health response with the active Kafka topic and active connection count
- `GET /ws?user_id=<id>`: WebSocket endpoint for chat clients

If `user_id` is omitted, the server generates one. If a recipient is offline, the message is cached in Redis and replayed on reconnect.

## Features

- Axum-based WebSocket server with Tokio async runtime
- Kafka producer and consumer wiring for message fan-out
- Redis-backed offline message cache and replay
- Config validation for `KAFKA_BROKERS`, `KAFKA_TOPIC`, `REDIS_URL`, and `PORT`
- Unit tests plus a live end-to-end integration test for connect, send, disconnect, and reconnect flow
- CI for `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --workspace`

## Requirements

- Rust and Cargo
- Kafka reachable at `KAFKA_BROKERS` or `localhost:9092`
- Redis reachable at `REDIS_URL` or `redis://127.0.0.1:6379`

## Configuration

The service reads these environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC` | `rust-topic` | Topic used for produced and consumed chat events |
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL |
| `PORT` | `3000` | HTTP server port |

A sample file is available at [`message-system/.env.example`](message-system/.env.example).

## Running Locally

```bash
cd message-system
cargo run
```

The server listens on `0.0.0.0:$PORT`.

## WebSocket Usage

Connect:

```text
ws://127.0.0.1:3000/ws?user_id=alice
```

First server message after connect:

```json
{
  "type": "connected",
  "user_id": "alice"
}
```

Client send payload:

```json
{
  "to": "bob",
  "body": "hello"
}
```

Delivered chat payload:

```json
{
  "to": "bob",
  "from": "alice",
  "body": "hello"
}
```

## Development

From `message-system/`:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

The live integration test is:

```bash
cargo test --test live_chat_flow -- --nocapture
```

That test expects Kafka on `127.0.0.1:9092` and Redis on `127.0.0.1:6379`.

## Architecture

At startup, the binary loads config, creates Kafka producer and consumer clients, builds a Redis pool, and mounts the WebSocket router.

For each incoming WebSocket message:

1. The payload is parsed as `{ "to": "...", "body": "..." }`.
2. The server fills in `from` with the connected user ID.
3. The message is produced to Kafka.
4. If the recipient is online, it is pushed to that active socket immediately.
5. If the recipient is offline, it is stored in Redis and replayed on the next reconnect for that user ID.

## Verification

Current repository checks:

- workspace builds and tests with Cargo
- CI workflow defined in [`.github/workflows/ci.yml`](.github/workflows/ci.yml)
- live integration test in [`message-system/tests/live_chat_flow.rs`](message-system/tests/live_chat_flow.rs)

## License

Licensed under the [MIT License](LICENSE).
