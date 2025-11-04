use deadpool_redis::{Config as RedisConfig, Pool};

pub async fn init_redis_pool() -> Pool {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let cfg = RedisConfig::from_url(redis_url);
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1)).unwrap()
}