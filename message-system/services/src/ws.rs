// src/ws.rs
use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use serde_json::json;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::state::{AppState, ChatMessage};

pub fn ws_router() -> Router<AppState> {
    Router::new().route("/ws", get(ws_handler))
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

// ---------------------------------------------------------------------------
// Core socket handling (private chat)
// ---------------------------------------------------------------------------
async fn handle_socket(socket: axum::extract::ws::WebSocket, state: AppState) {
    // 1. Identify the user (replace with real auth later)
    let user_id = Uuid::new_v4().to_string();
    info!("WS connected: {}", user_id);

    // 2. Channel that pushes messages *to this client*
    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<String>();

    // 3. Register the client – **exact field name**
    {
        let mut active = state.active_users.lock().await;
        active.insert(user_id.clone(), client_tx);
    }

    // 4. Split the socket
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // 5. Send task: client_rx → WebSocket
    let send_task = tokio::spawn(async move {
        while let Some(msg) = client_rx.recv().await {
            // Convert String → Utf8Bytes (required by axum 0.8+)
            if ws_sender
                .send(axum::extract::ws::Message::Text(msg.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // 6. Receive task: WebSocket → handle_incoming
    let recv_task = tokio::spawn({
        let state = state.clone();
        let user_id = user_id.clone();
        async move {
            while let Some(Ok(frame)) = ws_receiver.next().await {
                if let axum::extract::ws::Message::Text(text) = frame {
                    if let Err(e) = handle_incoming(&state, &user_id, &text).await {
                        error!("handle_incoming error: {}", e);
                        let _ = state
                            .send_to_user(&user_id, json!({"error": e.to_string()}).to_string())
                            .await;
                    }
                }
            }
        }
    });

    // 7. Wait for either side to close
    tokio::select! {
        _ = send_task => {}
        _ = recv_task => {}
    }

    // 8. Cleanup
    {
        let mut active = state.active_users.lock().await;
        active.remove(&user_id);
    }
    info!("WS disconnected: {}", user_id);
}

// ---------------------------------------------------------------------------
// Incoming message processing (private routing + persistence)
// ---------------------------------------------------------------------------
async fn handle_incoming(
    state: &AppState,
    from: &str,
    text: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Parse JSON
    let msg: ChatMessage = serde_json::from_str(text)?;
    if msg.from != from {
        return Err("Sender mismatch".into());
    }

    // 2. Persist to Kafka (fire-and-forget)
    let key = Uuid::new_v4().to_string();
    let payload = serde_json::to_string(&msg)?;
    let payload_kafka = payload.clone();
    let producer = state.kafka_producer.clone();
    let topic = state.kafka_topic.clone();

    tokio::spawn(async move {
        let record = rdkafka::producer::FutureRecord::to(&topic)
            .key(&key)
            .payload(&payload_kafka);

        if let Err(e) = producer.send(record, std::time::Duration::from_secs(1)).await {
            error!("Kafka send failed: {:?}", e);
        }
    });

    // 3. Optional Redis cache
if let Ok(mut conn) = state.redis_pool.get().await {
        let _: () = redis::cmd("SET")
            .arg(format!("last_msg:{}", msg.to))
            .arg(&payload) 
            .query_async(&mut conn)
            .await
            .unwrap_or_default();
    }

    state.send_to_user(&msg.to, payload).await?;  
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper on AppState
// ---------------------------------------------------------------------------
impl AppState {
    /// Send a raw string message to a specific online user.
    pub async fn send_to_user(&self, user_id: &str, msg: String) -> Result<(), &'static str> {
        let active = self.active_users.lock().await;
        if let Some(tx) = active.get(user_id) {
            let _ = tx.send(msg);
            Ok(())
        } else {
            Err("User not online")
        }
    }
}