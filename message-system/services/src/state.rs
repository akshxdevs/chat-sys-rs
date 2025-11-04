use std::{collections::HashMap, sync::{Arc}};
use futures_util::lock::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc::UnboundedSender};

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

impl AppState {
    pub async fn broadcast_to_all(&self, msg:&str) ->Result<(),&'static str> {
        let activie = self.active_users.lock().await;
        for (_,sender) in activie.iter() {
            let _ = sender.send(msg.to_owned());
        }
        Ok(())
    }
}