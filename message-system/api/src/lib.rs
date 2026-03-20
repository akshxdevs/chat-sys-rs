use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SendMessageRequest {
    pub to: String,
    pub body: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ServerEvent {
    pub event: String,
    pub user_id: Option<String>,
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_payload_is_structured() {
        let payload = SendMessageRequest {
            to: "bob".into(),
            body: "hello".into(),
        };

        assert_eq!(payload.to, "bob");
        assert_eq!(payload.body, "hello");
    }
}
