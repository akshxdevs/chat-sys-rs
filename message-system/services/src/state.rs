use futures_util::lock::Mutex;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, mpsc::UnboundedSender};

pub type ActiveUsers = Arc<Mutex<HashMap<String, UnboundedSender<String>>>>;

#[derive(Clone)]
pub struct AppState {
    pub broadcaster: broadcast::Sender<String>,
    pub active_users: ActiveUsers,
    pub redis_pool: deadpool_redis::Pool,
    pub kafka_topic: String,
    pub kafka_producer: Arc<rdkafka::producer::FutureProducer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    pub to: String,
    pub from: String,
    pub body: String,
}

impl AppState {
    pub async fn broadcast_to_all(&self, msg: &str) -> Result<(), &'static str> {
        broadcast_to_active_users(&self.active_users, msg).await;
        Ok(())
    }

    pub async fn send_to_user(&self, user_id: &str, msg: String) -> Result<(), &'static str> {
        if send_to_active_user(&self.active_users, user_id, msg).await {
            Ok(())
        } else {
            Err("User not online")
        }
    }

    pub async fn active_user_count(&self) -> usize {
        self.active_users.lock().await.len()
    }
}

pub async fn broadcast_to_active_users(active_users: &ActiveUsers, msg: &str) {
    let active = active_users.lock().await;
    for (_, sender) in active.iter() {
        let _ = sender.send(msg.to_owned());
    }
}

pub async fn send_to_active_user(active_users: &ActiveUsers, user_id: &str, msg: String) -> bool {
    let active = active_users.lock().await;
    if let Some(tx) = active.get(user_id) {
        let _ = tx.send(msg);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{ActiveUsers, broadcast_to_active_users, send_to_active_user};
    use futures_util::lock::Mutex;
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::mpsc;

    fn active_users() -> ActiveUsers {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[tokio::test]
    async fn sends_message_to_specific_user() {
        let active_users = active_users();
        let (tx, mut rx) = mpsc::unbounded_channel();
        active_users.lock().await.insert("alice".into(), tx);

        let sent = send_to_active_user(&active_users, "alice", "hello".into()).await;

        assert!(sent);
        assert_eq!(rx.recv().await.as_deref(), Some("hello"));
    }

    #[tokio::test]
    async fn broadcasts_message_to_all_users() {
        let active_users = active_users();
        let (tx_a, mut rx_a) = mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel();
        {
            let mut active = active_users.lock().await;
            active.insert("alice".into(), tx_a);
            active.insert("bob".into(), tx_b);
        }

        broadcast_to_active_users(&active_users, "broadcast").await;

        assert_eq!(rx_a.recv().await.as_deref(), Some("broadcast"));
        assert_eq!(rx_b.recv().await.as_deref(), Some("broadcast"));
    }

    #[tokio::test]
    async fn returns_false_for_unknown_user() {
        let active_users = active_users();

        let sent = send_to_active_user(&active_users, "ghost", "hello".into()).await;

        assert!(!sent);
    }
}
