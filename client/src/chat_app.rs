#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
#[cfg(target_os = "windows")]
unsafe extern "system" {
    #[link_name = "MessageBeep"]
    fn message_beep(u_type: u32) -> i32;
}

use eframe::egui;
use futures_util::{SinkExt, StreamExt};
use shared::{ChatEvent, ClientMessage};
use std::sync::mpsc::{Receiver, channel};
use std::time::Duration;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use shared::configs::{HOST, HOST_WS};
use crate::models::{AppState, NetEvent, SelectedChat};

pub struct ChatApp {
    state: AppState,
    username_input: String,
    username: String,
    is_connected: bool,
    current_msg: String,
    history: Vec<ChatEvent>,
    users: Vec<String>,
    selected_chat: SelectedChat,

    tx_to_net: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    rx_from_net: Option<Receiver<NetEvent>>,
}

impl ChatApp {
    pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        _cc.egui_ctx.set_visuals(egui::Visuals::dark());
        let mut fonts = egui::FontDefinitions::default();
        fonts.font_data.insert(
            "my_custom_font".to_owned(),
            egui::FontData::from_static(include_bytes!("../assets/font.ttf")),
        );
        fonts
            .families
            .entry(egui::FontFamily::Proportional)
            .or_default()
            .insert(0, "my_custom_font".to_owned());
        fonts
            .families
            .entry(egui::FontFamily::Monospace)
            .or_default()
            .push("my_custom_font".to_owned());
        _cc.egui_ctx.set_fonts(fonts);
        Self {
            state: AppState::Login,
            username_input: String::new(),
            username: String::new(),
            is_connected: false,
            current_msg: String::new(),
            history: Vec::new(),
            users: Vec::new(),
            selected_chat: SelectedChat::Global,
            tx_to_net: None,
            rx_from_net: None,
        }
    }

    fn should_show(event: &ChatEvent, selected: &SelectedChat, my_username: &str) -> bool {
        match event {
            ChatEvent::Message {
                user,
                text: _,
                recipient,
            } => match selected {
                SelectedChat::Global => recipient.is_none(),
                SelectedChat::Private(other) => {
                    (user == my_username && recipient.as_deref() == Some(other))
                        || (user == other && recipient.as_deref() == Some(my_username))
                }
            },
            ChatEvent::UserJoined(_) | ChatEvent::UserLeft(_) => {
                matches!(selected, SelectedChat::Global)
            }
            ChatEvent::SyncUsers(_) => false,
        }
    }

    fn format_message(event: &ChatEvent, _selected: &SelectedChat, _my_username: &str) -> String {
        match event {
            ChatEvent::Message {
                user,
                text,
                recipient: _,
            } => {
                format!("{}: {}", user, text)
            }
            ChatEvent::UserJoined(name) => format!("{} присоединился к чату", name),
            ChatEvent::UserLeft(name) => format!("{} покинул чат", name),
            ChatEvent::SyncUsers(_) => String::new(),
        }
    }

    fn connect(&mut self, nickname: String) {
        let (tx_to_ui, rx_from_net) = channel::<NetEvent>();
        let (tx_to_net, mut rx_from_ui) = tokio::sync::mpsc::unbounded_channel::<String>();

        self.tx_to_net = Some(tx_to_net);
        self.rx_from_net = Some(rx_from_net);
        self.username = nickname.clone();
        self.selected_chat = SelectedChat::Global;
        self.state = AppState::Chat;
        self.history.push(ChatEvent::Message {
            user: "⚙ Система".to_string(),
            text: "Подключение к серверу...".to_string(),
            recipient: None,
        });

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {

                loop {
                    match connect_async(HOST_WS).await {
                        Ok((ws_stream, _)) => {
                            let _ = tx_to_ui.send(NetEvent::Connected);
                            let (mut write, mut read) = ws_stream.split();

                            let _ = write.send(WsMessage::Text(nickname.clone())).await;

                            let tx_clone = tx_to_ui.clone();
                            let mut read_handle = tokio::spawn(async move {
                                while let Some(Ok(WsMessage::Text(text))) = read.next().await {
                                    if let Ok(event) = serde_json::from_str::<ChatEvent>(&text) {
                                        let _ = tx_clone.send(NetEvent::Chat(event));
                                    }
                                }
                            });

                            loop {
                                tokio::select! {
                                    msg = rx_from_ui.recv() => {
                                        match msg {
                                            Some(text) => {
                                                if write.send(WsMessage::Text(text)).await.is_err() {
                                                    break;
                                                }
                                            }
                                            None => break,
                                        }
                                    }
                                    _ = &mut read_handle => break,
                                }
                            }

                            read_handle.abort();
                            let _ = tx_to_ui.send(NetEvent::Disconnected);
                        }
                        Err(e) => {
                            eprintln!("Ошибка подключения: {}", e);
                            let _ = tx_to_ui.send(NetEvent::Disconnected);
                        }
                    }

                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });
        });
    }
}

impl eframe::App for ChatApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let events: Vec<NetEvent> = self
            .rx_from_net
            .as_ref()
            .map(|rx| {
                let mut evts = Vec::new();
                while let Ok(event) = rx.try_recv() {
                    evts.push(event);
                }
                evts
            })
            .unwrap_or_default();

        for event in events {
            match event {
                NetEvent::Chat(chat_event) => {
                    let is_from_other = match &chat_event {
                        ChatEvent::Message { user, .. } => *user != self.username,
                        _ => false,
                    };
                    match &chat_event {
                        ChatEvent::SyncUsers(users) => {
                            self.users = users.clone();
                        }
                        _ => {}
                    }
                    self.history.push(chat_event);
                    if is_from_other {
                        let has_focus = ctx.input(|i| i.viewport().focused.unwrap_or(true));
                        if !has_focus {
                            ctx.send_viewport_cmd(egui::ViewportCommand::RequestUserAttention(
                                egui::UserAttentionType::Informational,
                            ));
                            #[cfg(target_os = "windows")]
                            unsafe {
                                message_beep(0);
                            }
                        }
                    }
                }
                NetEvent::Connected => {
                    self.is_connected = true;
                    self.history.push(ChatEvent::Message {
                        user: "⚙ Система".to_string(),
                        text: "Подключение установлено".to_string(),
                        recipient: None,
                    });
                }
                NetEvent::Disconnected => {
                    self.is_connected = false;
                    self.users.clear();
                    self.history.push(ChatEvent::Message {
                        user: "⚙ Система".to_string(),
                        text: "Соединение разорвано. Переподключение...".to_string(),
                        recipient: None,
                    });
                }
            }
        }

        match self.state {
            AppState::Login => {
                egui::CentralPanel::default().show(ctx, |ui| {
                    ui.heading("Вход в чат");
                    ui.separator();
                    ui.label("Введите ваш никнейм для подключения:");

                    let text_input = ui.add(
                        egui::TextEdit::singleline(&mut self.username_input)
                            .hint_text("Никнейм...")
                            .desired_width(200.0),
                    );

                    let connect_btn = ui.button("Подключиться");

                    if connect_btn.clicked()
                        || (text_input.lost_focus()
                            && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                    {
                        let trimmed = self.username_input.trim().to_string();
                        if !trimmed.is_empty() {
                            self.connect(trimmed);
                        }
                    }
                });
            }
            AppState::Chat => {
                egui::SidePanel::left("users_panel")
                    .resizable(false)
                    .default_width(180.0)
                    .show(ctx, |ui| {
                        ui.heading("Молодые и успешные");
                        ui.separator();

                        let is_global = self.selected_chat == SelectedChat::Global;
                        if ui
                            .add(egui::SelectableLabel::new(is_global, "🌐 Общий чат"))
                            .clicked()
                        {
                            self.selected_chat = SelectedChat::Global;
                        }

                        ui.separator();
                        ui.label("В сети:");
                        egui::ScrollArea::vertical().show(ui, |ui| {
                            for user in &self.users {
                                if user != &self.username {
                                    let is_selected =
                                        matches!(&self.selected_chat, SelectedChat::Private(u) if u == user);
                                    if ui
                                        .add(egui::SelectableLabel::new(
                                            is_selected,
                                            format!("👤 {}", user),
                                        ))
                                        .clicked()
                                    {
                                        self.selected_chat = SelectedChat::Private(user.clone());
                                    }
                                }
                            }
                        });
                    });

                egui::CentralPanel::default().show(ctx, |ui| {
                    if !self.is_connected {
                        ui.colored_label(
                            egui::Color32::RED,
                            "⚠ Отключено от сервера. Переподключение...",
                        );
                        ui.separator();
                    }

                    match &self.selected_chat {
                        SelectedChat::Global => {
                            ui.heading("Общий чат");
                        }
                        SelectedChat::Private(user) => {
                            ui.heading(format!("Чат с {}", user));
                        }
                    }
                    ui.separator();

                    let text_edit_height = ui.spacing().interact_size.y + 10.0;

                    egui::ScrollArea::vertical()
                        .max_height(ui.available_height() - text_edit_height)
                        .stick_to_bottom(true)
                        .show(ui, |ui| {
                            for event in &self.history {
                                if Self::should_show(event, &self.selected_chat, &self.username) {
                                    ui.label(Self::format_message(
                                        event,
                                        &self.selected_chat,
                                        &self.username,
                                    ));
                                }
                            }
                        });

                    ui.separator();

                    ui.horizontal(|ui| {
                        let text_input = ui.add_enabled(
                            self.is_connected,
                            egui::TextEdit::singleline(&mut self.current_msg)
                                .hint_text("Введите сообщение...")
                                .desired_width(ui.available_width() - 80.0),
                        );

                        let send_btn =
                            ui.add_enabled(self.is_connected, egui::Button::new("Послать"));

                        if send_btn.clicked()
                            || (text_input.lost_focus()
                                && self.is_connected
                                && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                        {
                            if !self.current_msg.trim().is_empty() {
                                if let Some(tx) = &self.tx_to_net {
                                    let msg = ClientMessage {
                                        text: self.current_msg.clone(),
                                        recipient: match &self.selected_chat {
                                            SelectedChat::Global => None,
                                            SelectedChat::Private(user) => Some(user.clone()),
                                        },
                                    };
                                    let json = serde_json::to_string(&msg).unwrap();
                                    let _ = tx.send(json);
                                }
                                self.current_msg.clear();
                            }
                            text_input.request_focus();
                        }
                    });
                });
            }
        }

        ctx.request_repaint();
    }
}
