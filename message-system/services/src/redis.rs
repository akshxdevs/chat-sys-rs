use deadpool_redis::{Config as RedisConfig, CreatePoolError, Pool, PoolError};
use redis::{AsyncCommands, ErrorKind, RedisError, RedisResult};

const OFFLINE_QUEUE_LIMIT: isize = 100;

pub fn init_redis_pool(redis_url: &str) -> Result<Pool, CreatePoolError> {
    let cfg = RedisConfig::from_url(redis_url);
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))
}

pub fn offline_messages_key(user_id: &str) -> String {
    store::offline_messages_key(user_id)
}

pub async fn cache_message(pool: &Pool, user_id: &str, payload: &str) -> RedisResult<()> {
    let mut conn = pool.get().await.map_err(pool_error)?;
    let key = offline_messages_key(user_id);

    let _: () = redis::pipe()
        .cmd("RPUSH")
        .arg(&key)
        .arg(payload)
        .cmd("LTRIM")
        .arg(&key)
        .arg(-OFFLINE_QUEUE_LIMIT)
        .arg(-1)
        .query_async(&mut conn)
        .await?;

    Ok(())
}

pub async fn take_cached_messages(pool: &Pool, user_id: &str) -> RedisResult<Vec<String>> {
    let mut conn = pool.get().await.map_err(pool_error)?;
    let key = offline_messages_key(user_id);
    let messages: Vec<String> = conn.lrange(&key, 0, -1).await?;

    if !messages.is_empty() {
        let _: usize = conn.del(&key).await?;
    }

    Ok(messages)
}

fn pool_error(err: PoolError) -> RedisError {
    RedisError::from((ErrorKind::IoError, "deadpool redis error", err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::offline_messages_key;

    #[test]
    fn builds_offline_message_key() {
        assert_eq!(offline_messages_key("user-1"), "offline:user-1");
    }
}
