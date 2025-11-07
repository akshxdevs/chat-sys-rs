# Chat-System
A Rust-based real-time chat system implementing private messaging with WebSockets, Kafka for persistence, and Redis for caching.

## Overview
Chat-Sys-Rs is a backend server (built in Rust) that enables minimal, secure private chat functionality. Users connect via WebSockets to join unique UUID-based rooms, send messages (with simple "to + body" format), and receive durable delivery. Kafka ensures message persistence, while Redis handles offline caching—fetching and displaying missed messages on reconnect. Ideal for high-concurrency microservices or real-time apps, showcasing Rust's strengths in performance and safety.

## Features
- **WebSocket Connections**: Real-time bidirectional communication for instant messaging.
- **Private Rooms**: UUID-generated rooms for secure 1:1 or group chats.
- **Durable Delivery**: Kafka queues messages for reliable storage and ordering.
- **Offline Handling**: Redis caches undelivered messages; auto-sync on reconnect.
- **Simple API**: Send messages with minimal payload (recipient + body); server auto-fills sender.
- **High Concurrency**: Leverages Rust's async runtime (Tokio) for scalable handling.

## How It Works
1. **Connect**: Client initiates WebSocket handshake to server endpoint (e.g., ws://localhost:8080/ws). Server assigns/generates room UUID.

2. **Send Message**: Client sends JSON: `{ "to": "user_id", "body": "Hello!" }`. Server authenticates, publishes to Kafka topic (e.g., "chat-{room_uuid}"), and broadcasts via WebSocket.

3. **Receive/Deliver**: Subscribers pull from Kafka; if user offline, cache in Redis (key: "offline-{user_id}"). On reconnect, query Redis and flush cache.

4. **Offline Sync**: Reconnect triggers fetch: Load from Redis, mark as delivered, clear cache.

### Main Data Structure
```rust
// src/services/state.rs (inferred structure)
#[derive(Clone)]
pub struct AppState {
    pub broadcaster: broadcast::Sender<String>,
    pub active_users: Arc<Mutex<HashMap<String,UnboundedSender<String>>>>,
    pub redis_pool: deadpool_redis::Pool,
    pub kafka_topic: String,
    pub kafka_producer: Arc<rdkafka::producer::FutureProducer>,
}

#[derive(Debug, Serialize,Deserialize)]
pub struct ChatMessage{
    pub to:String,
    pub from:String,
    pub body:String
}

```

## Usage
### Clone the Repo
```bash
git clone https://github.com/akshxdevs/chat-sys-rs.git
cd chat-sys-rs
```
### Install Dependencies
```bash
cargo build
```
### Build the Project
```bash
cargo build --release
```
### Test the Project
```bash
cargo test
```

## Example Flow
- **Setup Services**: Run Kafka (e.g., via Docker) and Redis locally.
- Client A connects WS, creates room UUID, sends to Client B: `{ "to": "b_user", "body": "Hi!" }`.
- Message queued in Kafka; if B offline, cached in Redis.
- B connects/reconnects: Fetches from Redis/Kafka, displays "Hi!", marks delivered.
- **Alternate**: High-load test—multiple clients send 1000+ msgs/sec; Rust handles without drops.

## Key Files
- `src/main.rs`: Entry point with Tokio runtime, WS server setup (using tungstenite or axum).
- `src/handlers/ws_handler.rs`: WebSocket logic for connect/send/reconnect.
- `src/services/kafka_producer.rs`: Publishes messages to Kafka topics.
- `src/services/redis_cache.rs`: Caches/fetches offline messages.
- `Cargo.toml`: Dependencies (tokio, uuid, rdkafka, redis, serde).

## Events
- `MessageSent`: Emitted on successful publish to Kafka with msg ID and room.
- `MessageDelivered`: Emitted on WebSocket broadcast or cache flush.
- `ReconnectSync`: Emitted on offline message fetch with count synced.

## Requirements
- Rust 1.75+, Cargo
- Kafka (for persistence)
- Redis (for caching)
- Docker (optional, for services)

## License
MIT
---
For more details, see the program code and the test suite.
