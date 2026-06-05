use axum::{
    Router,
    extract::State,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "payload")]
enum ChatEvent {
    UserJoined(String),
    UserLeft(String),
    Message { user: String, text: String },
    SyncUsers(Vec<String>),
}

struct AppState {
    users: Mutex<HashSet<String>>,
    history: Mutex<VecDeque<ChatEvent>>, // Хранилище истории сообщений
    tx: broadcast::Sender<ChatEvent>,
}

#[tokio::main]
async fn main() {
    let (tx, _rx) = broadcast::channel(100);
    let app_state = Arc::new(AppState {
        users: Mutex::new(HashSet::new()),
        history: Mutex::new(VecDeque::new()), // Инициализируем пустую историю
        tx,
    });

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    println!("Сервер запущен на 0.0.0.0:8080");
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();
    let mut username = String::new();

    // 1. Ожидаем первое сообщение с именем пользователя
    if let Some(Ok(Message::Text(name))) = receiver.next().await {
        username = name.to_string();
        state.users.lock().unwrap().insert(username.clone());

        let users = state.users.lock().unwrap().iter().cloned().collect();
        let _ = state.tx.send(ChatEvent::SyncUsers(users));
        let _ = state.tx.send(ChatEvent::UserJoined(username.clone()));
    }

    // 2. Собираем историю сообщений для отправки новому пользователю
    let history_snapshot: Vec<ChatEvent> = {
        let history = state.history.lock().unwrap();
        history.iter().cloned().collect()
    };

    // 3. Рассылка сообщений (Incoming от клиента -> Broadcast) и (Broadcast -> Outgoing клиенту)
    let mut send_task = tokio::spawn(async move {
        // Сначала отправляем историю новому пользователю
        for event in history_snapshot {
            let msg = serde_json::to_string(&event).unwrap();
            if sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }

        // Потом начинаем слушать broadcast для новых сообщений
        while let Ok(event) = rx.recv().await {
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
            if let Ok(msg_content) = serde_json::from_str::<String>(&text) {
                let chat_event = ChatEvent::Message {
                    user: name_copy.clone(),
                    text: msg_content,
                };

                // Добавляем сообщение в историю
                {
                    let mut history = state_copy.history.lock().unwrap();
                    history.push_back(chat_event.clone());

                    // Ограничиваем историю 15 сообщениями
                    if history.len() > 15 {
                        history.pop_front();
                    }
                }

                // Рассылаем сообщение всем подключённым клиентам
                let _ = state_copy.tx.send(chat_event);
            }
        }
    });

    // Очистка при отключении
    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    };

    if !username.is_empty() {
        state.users.lock().unwrap().remove(&username);
        let _ = state.tx.send(ChatEvent::UserLeft(username));
        let users = state.users.lock().unwrap().iter().cloned().collect();
        let _ = state.tx.send(ChatEvent::SyncUsers(users));
    }
}
