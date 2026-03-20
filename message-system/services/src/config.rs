use std::{env, error::Error, fmt};

const DEFAULT_KAFKA_BROKERS: &str = "localhost:9092";
const DEFAULT_KAFKA_TOPIC: &str = "rust-topic";
const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
const DEFAULT_PORT: u16 = 3000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppConfig {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub redis_url: String,
    pub port: u16,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConfigError {
    Empty(&'static str),
    InvalidPort(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty(name) => write!(f, "{name} cannot be empty"),
            Self::InvalidPort(value) => write!(f, "invalid PORT value: {value}"),
        }
    }
}

impl Error for ConfigError {}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup<F>(mut lookup: F) -> Result<Self, ConfigError>
    where
        F: FnMut(&'static str) -> Option<String>,
    {
        let kafka_brokers = read_var(&mut lookup, "KAFKA_BROKERS", DEFAULT_KAFKA_BROKERS)?;
        let kafka_topic = read_var(&mut lookup, "KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC)?;
        let redis_url = read_var(&mut lookup, "REDIS_URL", DEFAULT_REDIS_URL)?;
        let port = match lookup("PORT") {
            Some(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return Err(ConfigError::Empty("PORT"));
                }
                trimmed
                    .parse::<u16>()
                    .map_err(|_| ConfigError::InvalidPort(trimmed.to_owned()))?
            }
            None => DEFAULT_PORT,
        };

        Ok(Self {
            kafka_brokers,
            kafka_topic,
            redis_url,
            port,
        })
    }
}

fn read_var<F>(lookup: &mut F, name: &'static str, default: &str) -> Result<String, ConfigError>
where
    F: FnMut(&'static str) -> Option<String>,
{
    match lookup(name) {
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Err(ConfigError::Empty(name))
            } else {
                Ok(trimmed.to_owned())
            }
        }
        None => Ok(default.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::{AppConfig, ConfigError, DEFAULT_PORT};
    use std::collections::HashMap;

    #[test]
    fn uses_defaults_when_env_is_missing() {
        let env: HashMap<String, String> = HashMap::new();
        let config = AppConfig::from_lookup(|name| env.get(name).cloned()).unwrap();

        assert_eq!(config.port, DEFAULT_PORT);
        assert_eq!(config.kafka_brokers, "localhost:9092");
        assert_eq!(config.kafka_topic, "rust-topic");
        assert_eq!(config.redis_url, "redis://127.0.0.1:6379");
    }

    #[test]
    fn rejects_invalid_port() {
        let env = HashMap::from([(String::from("PORT"), String::from("abc"))]);
        let err = AppConfig::from_lookup(|name| env.get(name).cloned()).unwrap_err();

        assert_eq!(err, ConfigError::InvalidPort("abc".into()));
    }

    #[test]
    fn rejects_empty_required_values() {
        let env = HashMap::from([(String::from("KAFKA_TOPIC"), String::from("   "))]);
        let err = AppConfig::from_lookup(|name| env.get(name).cloned()).unwrap_err();
        assert_eq!(err, ConfigError::Empty("KAFKA_TOPIC"));
    }
}
