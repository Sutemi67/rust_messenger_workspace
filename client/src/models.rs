use shared::ChatEvent;

#[derive(PartialEq)]
pub enum SelectedChat {
    Global,
    Private(String),
}
pub enum NetEvent {
    Chat(ChatEvent),
    Connected,
    Disconnected,
}

pub enum AppState {
    Login,
    Chat,
}
