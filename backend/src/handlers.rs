use std::sync::Arc;
use axum::extract::{State, WebSocketUpgrade};
use axum::extract::ws::{Message, WebSocket};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use shared::{ChatEvent, ClientMessage};
use crate::app_state::AppState;

pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut username = String::new();

    let (private_tx, mut private_rx) = mpsc::unbounded_channel::<ChatEvent>();

    if let Some(Ok(Message::Text(name))) = receiver.next().await {
        username = name.to_string();

        {
            let mut users = state.users.lock().unwrap();
            users.insert(username.clone());
        }
        {
            let mut conns = state.connections.lock().unwrap();
            conns.insert(username.clone(), private_tx);
        }

        let users: Vec<String> = state.users.lock().unwrap().iter().cloned().collect();
        let conns = state.connections.lock().unwrap();
        for (name, tx) in conns.iter() {
            let _ = tx.send(ChatEvent::SyncUsers(users.clone()));
            if *name != username {
                let _ = tx.send(ChatEvent::UserJoined(username.clone()));
            }
        }
    }

    let history_snapshot: Vec<ChatEvent> = {
        let history = state.history.lock().unwrap();
        history
            .iter()
            .filter(|event| match event {
                ChatEvent::Message {
                    user,
                    text: _,
                    recipient,
                } => {
                    recipient.is_none()
                        || recipient.as_deref() == Some(&username)
                        || user == &username
                }
                _ => true,
            })
            .cloned()
            .collect()
    };

    let mut send_task = tokio::spawn(async move {
        for event in history_snapshot {
            let msg = serde_json::to_string(&event).unwrap();
            if sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
        while let Some(event) = private_rx.recv().await {
            let msg = serde_json::to_string(&event).unwrap();
            if sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    let state_copy = state.clone();
    let name_copy = username.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = receiver.next().await {
            if let Ok(incoming) = serde_json::from_str::<ClientMessage>(&text) {
                let chat_event = ChatEvent::Message {
                    user: name_copy.clone(),
                    text: incoming.text,
                    recipient: incoming.recipient.clone(),
                };

                {
                    let mut history = state_copy.history.lock().unwrap();
                    history.push_back(chat_event.clone());
                    if history.len() > 50 {
                        history.pop_front();
                    }
                }

                let conns = state_copy.connections.lock().unwrap();
                if let Some(recipient) = &incoming.recipient {
                    if let Some(tx) = conns.get(recipient) {
                        let _ = tx.send(chat_event.clone());
                    }
                    if let Some(tx) = conns.get(&name_copy) {
                        let _ = tx.send(chat_event.clone());
                    }
                } else {
                    for tx in conns.values() {
                        let _ = tx.send(chat_event.clone());
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    }

    if !username.is_empty() {
        state.connections.lock().unwrap().remove(&username);
        state.users.lock().unwrap().remove(&username);

        let users: Vec<String> = state.users.lock().unwrap().iter().cloned().collect();
        let conns = state.connections.lock().unwrap();
        for tx in conns.values() {
            let _ = tx.send(ChatEvent::UserLeft(username.clone()));
            let _ = tx.send(ChatEvent::SyncUsers(users.clone()));
        }
    }
}
