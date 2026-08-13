mod app_state;
mod handlers;

use crate::app_state::AppState;
use crate::handlers::ws_handler;
use axum::{
    Router,
    routing::get,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    let app_state = Arc::new(AppState {
        users: Mutex::new(HashSet::new()),
        connections: Mutex::new(HashMap::new()),
        history: Mutex::new(VecDeque::new()),
    });

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(shared::configs::HOST).await.unwrap();
    println!("Сервер запущен на {}", shared::configs::HOST);
    axum::serve(listener, app).await.unwrap();
}