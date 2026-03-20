pub const OFFLINE_KEY_PREFIX: &str = "offline:";

pub fn offline_messages_key(user_id: &str) -> String {
    format!("{OFFLINE_KEY_PREFIX}{user_id}")
}

#[cfg(test)]
mod tests {
    use super::offline_messages_key;

    #[test]
    fn builds_offline_key() {
        assert_eq!(offline_messages_key("alice"), "offline:alice");
    }
}
