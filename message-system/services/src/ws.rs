use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::state::{AppState, ChatMessage};

pub fn ws_router() -> Router<AppState> {
    Router::new().route("/ws", get(ws_handler))
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}


async fn handle_socket(socket: axum::extract::ws::WebSocket, state: AppState) {
    let user_id = Uuid::new_v4().to_string();
    info!("WS connected: {}", user_id);

    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<String>();

    {
        let mut active = state.active_users.lock().await;
        active.insert(user_id.clone(), client_tx);
    }

    let (mut ws_sender, mut ws_receiver) = socket.split();

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

    tokio::select! {
        _ = send_task => {}
        _ = recv_task => {}
    }

    {
        let mut active = state.active_users.lock().await;
        active.remove(&user_id);
    }
    info!("WS disconnected: {}", user_id);
}


async fn handle_incoming(
    state: &AppState,
    from_user_id: &str,
    text: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    #[derive(Deserialize)]
    struct IncomingMsg{
        to:String,
        body:String
    }

    let incoming: IncomingMsg = serde_json::from_str(text)?;

    let msg = ChatMessage{
        to:incoming.to,
        from:from_user_id.to_string(),
        body:incoming.body
    };

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

    if let Ok(mut conn) = state.redis_pool.get().await {
            let list_key = format!("chat history:{}",msg.to);
            let _:() = redis::pipe()
            .cmd("LPUSH").arg(&list_key).arg(&payload)
            .cmd("LTRIM").arg(&list_key).arg(0).arg(19)
            .query_async(&mut conn)
            .await
            .unwrap_or_default();
    }
    state.send_to_user(&msg.to, payload).await?;  
    Ok(())
}

impl AppState {
    pub async fn send_to_user(&self, user_id: &str, msg: String) -> Result<(), &'static str> {
        let active = self.active_users.lock().await;
        if let Some(tx) = active.get(user_id) {
            let _ = tx.send(msg);
            debug!("Sent to {}", user_id);         
            Ok(())
        } else {
            Err("User not online")
        }
    }
}