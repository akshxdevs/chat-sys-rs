use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::{
    net::TcpListener,
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::{
    process::{Child, Command},
    time::sleep,
};
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message};

type WsStream = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::test]
async fn delivers_live_and_offline_messages_across_reconnects()
-> Result<(), Box<dyn std::error::Error>> {
    let port = reserve_port()?;
    let topic = format!("chat-e2e-{}", std::process::id());
    let mut server = spawn_server(port, &topic)?;

    wait_for_health(port).await?;

    let (mut alice, _) = connect_async(format!("ws://127.0.0.1:{port}/ws?user_id=alice")).await?;
    let (mut bob, _) = connect_async(format!("ws://127.0.0.1:{port}/ws?user_id=bob")).await?;

    assert_connected(&mut alice, "alice").await?;
    assert_connected(&mut bob, "bob").await?;

    alice
        .send(Message::text(r#"{"to":"bob","body":"hello-live"}"#))
        .await?;

    let live_message = recv_until(&mut bob, |json| json["body"] == "hello-live").await?;
    assert_eq!(live_message["from"], "alice");
    assert_eq!(live_message["to"], "bob");

    bob.close(None).await?;
    sleep(Duration::from_millis(200)).await;

    alice
        .send(Message::text(r#"{"to":"bob","body":"hello-offline"}"#))
        .await?;

    sleep(Duration::from_millis(500)).await;

    let (mut bob_reconnected, _) =
        connect_async(format!("ws://127.0.0.1:{port}/ws?user_id=bob")).await?;

    assert_connected(&mut bob_reconnected, "bob").await?;

    let offline_message =
        recv_until(&mut bob_reconnected, |json| json["body"] == "hello-offline").await?;
    assert_eq!(offline_message["from"], "alice");
    assert_eq!(offline_message["to"], "bob");

    shutdown_server(&mut server).await;
    Ok(())
}

fn reserve_port() -> Result<u16, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn spawn_server(port: u16, topic: &str) -> Result<Child, Box<dyn std::error::Error>> {
    let binary = env!("CARGO_BIN_EXE_message-system");
    let child = Command::new(binary)
        .env("PORT", port.to_string())
        .env("KAFKA_BROKERS", "127.0.0.1:9092")
        .env("KAFKA_TOPIC", topic)
        .env("REDIS_URL", "redis://127.0.0.1:6379")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(child)
}

async fn wait_for_health(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    let url = format!("http://127.0.0.1:{port}/health");

    loop {
        if let Ok(response) = client.get(&url).send().await
            && response.status().is_success()
        {
            let json: Value = response.json().await?;
            if json["status"] == "ok" {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            return Err("server did not become healthy in time".into());
        }

        sleep(Duration::from_millis(200)).await;
    }
}

async fn assert_connected(
    stream: &mut WsStream,
    user_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let connected = recv_until(stream, |json| {
        json["type"] == "connected" && json["user_id"] == user_id
    })
    .await?;
    assert_eq!(connected["user_id"], user_id);
    Ok(())
}

async fn recv_until<F>(
    stream: &mut WsStream,
    predicate: F,
) -> Result<Value, Box<dyn std::error::Error>>
where
    F: Fn(&Value) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("timed out waiting for websocket message".into());
        }

        let next = tokio::time::timeout(remaining, stream.next()).await?;
        match next {
            Some(Ok(Message::Text(text))) => {
                let json: Value = serde_json::from_str(&text)?;
                if predicate(&json) {
                    return Ok(json);
                }
            }
            Some(Ok(Message::Binary(_))) => {}
            Some(Ok(Message::Ping(_))) => {}
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Frame(_))) => {}
            Some(Ok(Message::Close(_))) => {
                return Err("websocket closed before expected message".into());
            }
            Some(Err(err)) => return Err(err.into()),
            None => return Err("websocket stream ended".into()),
        }
    }
}

async fn shutdown_server(child: &mut Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}
