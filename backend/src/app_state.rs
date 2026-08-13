use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;
use tokio::sync::mpsc;
use shared::ChatEvent;

pub struct AppState {
    pub(crate) users: Mutex<HashSet<String>>,
    pub(crate) connections: Mutex<HashMap<String, mpsc::UnboundedSender<ChatEvent>>>,
    pub(crate) history: Mutex<VecDeque<ChatEvent>>,
}