use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade}, // Kept WebSocketUpgrade for `axum_websocket_upgrade_handler`
        State,
    },
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use futures::{sink::SinkExt, stream::StreamExt};
use std::sync::Arc;
use tokio::sync::{mpsc}; // Removed `broadcast` as InternalPubSub manages it
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;
use bytes::Bytes;
use rkyv::{access, Archived, rancor::Error as RkyvError};
use crate::benchmark::pubsub::InternalPubSub;
use crate::benchmark::data::BenchmarkPayload;
use crate::cli::DataFormat; // Keep DataFormat, it's used in AppState.

#[derive(Debug, Clone)]
enum WsClientMessage {
    Text(String),
    Binary(Bytes),
    Pong(Vec<u8>),
}

struct WsAppState {
    client_txs: DashMap<Uuid, mpsc::Sender<WsClientMessage>>,
    internal_pubsub: Arc<InternalPubSub>,
    cli_data_format: DataFormat,
    cli_compress_zstd: bool,
}

async fn handle_websocket_connection(socket: WebSocket, app_state: Arc<WsAppState>) {
    let client_id = Uuid::new_v4();
    println!("WebSocket Client {} connected.", client_id);

    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (client_tx, mut client_rx) = mpsc::channel::<WsClientMessage>(64);

    app_state.client_txs.insert(client_id, client_tx.clone());

    let mut internal_pubsub_rx = app_state.internal_pubsub.subscribe("global_chat".to_string());


    let client_ws_tx_task = tokio::spawn(async move {
        while let Some(msg) = client_rx.recv().await {
            let tungstenite_msg = match msg {
                WsClientMessage::Text(s) => Message::Text(s.into()),
                WsClientMessage::Binary(b) => Message::Binary(b),
                WsClientMessage::Pong(p) => Message::Pong(p.into()),
            };
            if ws_sender.send(tungstenite_msg).await.is_err() { break; }
        }
    });

    let ws_client_tx_clone = client_tx.clone();
    let internal_pubsub_listener_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                result = internal_pubsub_rx.recv() => {
                    match result {
                        Ok(message_arc_bytes) => {
                            let bytes_to_send = (*message_arc_bytes).clone();
                            if ws_client_tx_clone.send(WsClientMessage::Binary(bytes_to_send)).await.is_err() {
                                break;
                            }
                        }
                        Err(RecvError::Lagged(_)) => eprintln!("WebSocket Client {} lagged on internal PubSub!", client_id),
                        Err(RecvError::Closed) => break,
                    }
                }
                _ = tokio::signal::ctrl_c() => break,
            }
        }
    });

    while let Some(result) = ws_receiver.next().await {
        match result {
            Ok(msg) => match msg {
                Message::Text(t) => {
                    eprintln!("Client {} sent unexpected text: {}", client_id, t);
                }
                Message::Binary(b) => {
                    // Corrected: Use rkyv::access for zero-copy deserialization
                    let payload_ref = access::<Archived<BenchmarkPayload>, RkyvError>(&b)
                                        .map(|archived| archived)
                                        .ok();
                    
                    if let Some(_payload) = payload_ref {
                        app_state.internal_pubsub.publish("global_chat".to_string(), Arc::new(b.into()));
                    } else {
                        eprintln!("Client {} sent invalid Rkyv binary message ({} bytes)", client_id, b.len());
                    }
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
    internal_pubsub_listener_task.abort();
}

async fn axum_publish_handler(State(app_state): State<Arc<WsAppState>>, Json(payload): Json<serde_json::Value>) -> impl IntoResponse {
    let message_text = payload["message"].as_str().unwrap_or("No message provided").to_string();
    
    app_state.internal_pubsub.publish("global_chat".to_string(), Arc::new(Bytes::from(message_text.into_bytes())));

    if app_state.client_txs.is_empty() {
        eprintln!("Failed to send message to WebSocket clients (no clients connected to server).");
        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "Failed to publish message")
    } else {
        (axum::http::StatusCode::OK, "Message published")
    }
}

// Handler for the /ws route, wrapping handle_websocket_connection
async fn axum_websocket_upgrade_handler(ws: WebSocketUpgrade, State(app_state): State<Arc<WsAppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_websocket_connection(socket, app_state))
}


pub async fn run_axum_ws_server(internal_pubsub: Arc<InternalPubSub>, cli_data_format: DataFormat, cli_compress_zstd: bool) -> Result<()> {
    let app_state = Arc::new(WsAppState {
        client_txs: DashMap::new(),
        internal_pubsub: internal_pubsub.clone(),
        cli_data_format,
        cli_compress_zstd,
    });

    let app = Router::new()
        .route("/ws", get(axum_websocket_upgrade_handler))
        .route("/publish", post(axum_publish_handler))
        .with_state(app_state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Axum WebSocket server listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();

    Ok(())
}