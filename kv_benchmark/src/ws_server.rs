use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use futures::{sink::SinkExt, stream::StreamExt};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;

#[derive(Debug, Clone)]
enum WsClientMessage {
    Text(String),
    Binary(Vec<u8>),
    Pong(Vec<u8>),
}

struct WsAppState {
    client_txs: DashMap<Uuid, mpsc::Sender<WsClientMessage>>,
    global_broadcast_tx: broadcast::Sender<WsClientMessage>,
}

async fn handle_websocket_connection(socket: WebSocket, app_state: Arc<WsAppState>) {
    let client_id = Uuid::new_v4();
    println!("WebSocket Client {} connected.", client_id);

    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (client_tx, mut client_rx) = mpsc::channel::<WsClientMessage>(64);

    app_state.client_txs.insert(client_id, client_tx.clone());
    let mut global_broadcast_rx = app_state.global_broadcast_tx.subscribe();

    let client_ws_tx_task = tokio::spawn(async move {
        while let Some(msg) = client_rx.recv().await {
            let tungstenite_msg = match msg {
                WsClientMessage::Text(s) => Message::Text(s.into()),
                WsClientMessage::Binary(b) => Message::Binary(b.into()),
                WsClientMessage::Pong(p) => Message::Pong(p.into()),
            };
            if ws_sender.send(tungstenite_msg).await.is_err() { break; }
        }
    });

    let app_state_clone = Arc::clone(&app_state);
    let client_broadcast_listener_task = tokio::spawn(async move {
        loop {
            match global_broadcast_rx.recv().await {
                Ok(msg) => {
                    if let Some(sender) = app_state_clone.client_txs.get(&client_id) {
                        if sender.try_send(msg).is_err() { }
                    } else { break; }
                }
                Err(RecvError::Lagged(_)) => eprintln!("Client {} lagged!", client_id),
                Err(RecvError::Closed) => break,
            }
        }
    });

    while let Some(result) = ws_receiver.next().await {
        match result {
            Ok(msg) => match msg {
                Message::Text(t) => {
                    println!("Client {} sent text: {}", client_id, t);
                    let _ = app_state.global_broadcast_tx.send(WsClientMessage::Text(format!("Client {}: {}", client_id, t)));
                }
                Message::Binary(b) => {
                    println!("Client {} sent binary ({} bytes)", client_id, b.len());
                    let _ = app_state.global_broadcast_tx.send(WsClientMessage::Binary(b.to_vec()));
                }
                Message::Ping(ping_data) => {
                    if client_tx.send(WsClientMessage::Pong(ping_data.to_vec())).await.is_err() { break; }
                }
                Message::Pong(_) => {},
                Message::Close(_) => break,
            },
            Err(err) => {
                eprintln!("WebSocket error for client {}: {:?}", client_id, err);
                break;
            }
        }
    }

    app_state.client_txs.remove(&client_id);
    println!("WebSocket Client {} disconnected.", client_id);
    client_ws_tx_task.abort();
    client_broadcast_listener_task.abort();
}

async fn axum_ws_handler(ws: WebSocketUpgrade, State(app_state): State<Arc<WsAppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_websocket_connection(socket, app_state))
}

async fn axum_publish_handler(State(app_state): State<Arc<WsAppState>>, Json(payload): Json<serde_json::Value>) -> impl IntoResponse {
    let message_text = payload["message"].as_str().unwrap_or("No message provided").to_string();
    if app_state.global_broadcast_tx.send(WsClientMessage::Text(format!("Server says: {}", message_text))).is_err() {
        eprintln!("Failed to send message to global broadcast (no receivers).");
        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "Failed to publish message")
    } else {
        (axum::http::StatusCode::OK, "Message published")
    }
}

pub async fn run_axum_ws_server() -> Result<()> {
    let (global_broadcast_tx, _) = broadcast::channel::<WsClientMessage>(1024);

    let app_state = Arc::new(WsAppState {
        client_txs: DashMap::new(),
        global_broadcast_tx: global_broadcast_tx.clone(),
    });

    let app = Router::new()
        .route("/ws", get(axum_ws_handler))
        .route("/publish", post(axum_publish_handler))
        .with_state(app_state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Axum WebSocket server listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();

    Ok(())
}