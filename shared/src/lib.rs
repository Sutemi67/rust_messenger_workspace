pub mod configs;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "payload")]
pub enum ChatEvent {
    UserJoined(String),
    UserLeft(String),
    Message {
        user: String,
        text: String,
        recipient: Option<String>,
    },
    SyncUsers(Vec<String>),
}

#[derive(Deserialize, Serialize)]
pub struct ClientMessage {
    pub text: String,
    pub recipient: Option<String>,
}
