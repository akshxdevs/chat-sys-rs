use axum::{
    Json, Router,
    extract::{Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{AppState, ChatMessage, cache_message, take_cached_messages};

pub fn ws_router() -> Router<AppState> {
    Router::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(health_handler))
}

#[derive(Debug, Deserialize, Default)]
struct ConnectParams {
    user_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    kafka_topic: String,
    active_users: usize,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(params): Query<ConnectParams>,
) -> Response {
    let user_id = match resolve_user_id(params.user_id) {
        Ok(user_id) => user_id,
        Err(message) => {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response();
        }
    };

    ws.on_upgrade(move |socket| handle_socket(socket, state, user_id))
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok",
        kafka_topic: state.kafka_topic.clone(),
        active_users: state.active_user_count().await,
    })
}

async fn handle_socket(socket: axum::extract::ws::WebSocket, state: AppState, user_id: String) {
    info!("WS connected: {}", user_id);

    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<String>();

    {
        let mut active = state.active_users.lock().await;
        active.insert(user_id.clone(), client_tx);
    }

    let (mut ws_sender, mut ws_receiver) = socket.split();

    let _ = state
        .send_to_user(
            &user_id,
            json!({
                "type": "connected",
                "user_id": user_id,
            })
            .to_string(),
        )
        .await;

    match take_cached_messages(&state.redis_pool, &user_id).await {
        Ok(cached_messages) => {
            for message in cached_messages {
                let _ = state.send_to_user(&user_id, message).await;
            }
        }
        Err(err) => error!("Failed to replay offline messages for {}: {}", user_id, err),
    }

    let send_task = tokio::spawn(async move {
        while let Some(msg) = client_rx.recv().await {
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
                if let axum::extract::ws::Message::Text(text) = frame
                    && let Err(e) = handle_incoming(&state, &user_id, &text).await
                {
                    error!("handle_incoming error: {}", e);
                    let _ = state
                        .send_to_user(&user_id, json!({"error": e.to_string()}).to_string())
                        .await;
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
    struct IncomingMsg {
        to: String,
        body: String,
    }

    let incoming: IncomingMsg = serde_json::from_str(text)?;

    let msg = ChatMessage {
        to: incoming.to,
        from: from_user_id.to_string(),
        body: incoming.body,
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

        if let Err(e) = producer
            .send(record, std::time::Duration::from_secs(1))
            .await
        {
            error!("Kafka send failed: {:?}", e);
        }
    });

    match state.send_to_user(&msg.to, payload.clone()).await {
        Ok(()) => {
            debug!("Delivered live message to {}", msg.to);
        }
        Err(_) => {
            cache_message(&state.redis_pool, &msg.to, &payload).await?;
            debug!("Cached offline message for {}", msg.to);
        }
    }

    Ok(())
}

fn resolve_user_id(user_id: Option<String>) -> Result<String, &'static str> {
    match user_id {
        Some(user_id) => {
            let trimmed = user_id.trim();
            if trimmed.len() < 3 || trimmed.len() > 64 {
                return Err("user_id must be between 3 and 64 characters");
            }

            if !trimmed
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
            {
                return Err("user_id may only contain letters, numbers, underscores, and hyphens");
            }

            Ok(trimmed.to_owned())
        }
        None => Ok(Uuid::new_v4().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_user_id;

    #[test]
    fn accepts_valid_user_id() {
        let user_id = resolve_user_id(Some("alice_123".into())).unwrap();
        assert_eq!(user_id, "alice_123");
    }

    #[test]
    fn rejects_short_user_id() {
        let err = resolve_user_id(Some("ab".into())).unwrap_err();
        assert_eq!(err, "user_id must be between 3 and 64 characters");
    }

    #[test]
    fn rejects_invalid_characters() {
        let err = resolve_user_id(Some("alice@example".into())).unwrap_err();
        assert_eq!(
            err,
            "user_id may only contain letters, numbers, underscores, and hyphens"
        );
    }

    #[test]
    fn generates_user_id_when_missing() {
        let user_id = resolve_user_id(None).unwrap();
        assert!(!user_id.is_empty());
    }
}
