use axum::{Router};
use futures_util::{StreamExt, lock::Mutex};
use tokio::{sync::broadcast};
use rdkafka::{consumer::{CommitMode, Consumer, StreamConsumer}};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use rdkafka::Message;
use log::{info};

use services::{AppState, ChatMessage, init_consumer, init_producer, init_redis_pool, ws};

#[tokio::main]
pub async fn main() ->Result<(),std::io::Error> {
    tracing_subscriber::fmt::init();
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or("rust-topic".into());
    let producer = Arc::new(init_producer(&brokers));
    let consumer: StreamConsumer = init_consumer(&brokers,"rust-ws-service",&topic);
    let (tx,_rx) = broadcast::channel(100);
    let redis_pool = init_redis_pool().await;   

    let state = AppState{
        kafka_producer:producer.clone(),
        active_users: Arc::new(Mutex::new(HashMap::new())),
        kafka_topic:topic.clone(),
        redis_pool,
        broadcaster:tx.clone(),
    };

    let bg_state = state.clone();
    let bg_consumer = consumer;
    tokio::spawn(async move {
        let mut stream = bg_consumer.stream();
        while let Some(Ok(msg)) = stream.next().await {
            if let Some(Ok(payload)) = msg.payload_view::<str>() {
                if let Ok(chat_msg) = serde_json::from_str::<ChatMessage>(payload) {
                    let json_str = serde_json::to_string(&chat_msg).unwrap_or(payload.to_owned());
                    let _ = bg_state
                    .broadcast_to_all(&json_str);
                }
                let _ = bg_state.broadcaster.send(payload.to_string());
            }
            let _ = bg_consumer.commit_message(&msg, CommitMode::Async);
        }
    });
    let ws_router = ws::ws_router().with_state(state.clone());

    let app = Router::new()
    .merge(ws_router);

    let addr = SocketAddr::from(([0,0,0,0],3000));
    info!("Server Listening on {}",addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}