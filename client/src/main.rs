#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // Отключает консоль в релизе на Windows

use eframe::egui;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::mpsc::{Receiver, channel};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;

// Важно: Структура события должна СТРОГО совпадать с серверной
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "payload")]
enum ChatEvent {
    UserJoined(String),
    UserLeft(String),
    Message { user: String, text: String },
    SyncUsers(Vec<String>),
}

enum AppState {
    Login,
    Chat,
}

struct ChatApp {
    state: AppState,
    username_input: String,
    username: String,
    is_connected: bool,
    current_msg: String,
    history: Vec<String>,
    users: Vec<String>,

    tx_to_net: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    rx_from_net: Option<Receiver<ChatEvent>>,
}

impl ChatApp {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        _cc.egui_ctx.set_visuals(egui::Visuals::dark());
        let mut fonts = egui::FontDefinitions::default();
        // Встраиваем байты файла шрифта прямо в бинарник при компиляции
        // Убедись, что путь к файлу верный относительно файла Cargo.toml или main.rs
        fonts.font_data.insert(
            "my_custom_font".to_owned(),
            egui::FontData::from_static(include_bytes!("../assets/font.ttf")),
            // Если файл лежит рядом с main.rs, используй include_bytes!("font.ttf")
        );
        // Добавляем наш шрифт в начало списка пропорциональных шрифтов (он будет использоваться по умолчанию)
        fonts
            .families
            .entry(egui::FontFamily::Proportional)
            .or_default()
            .insert(0, "my_custom_font".to_owned());

        // Также можно добавить его в моноширинный семейство (для кода или логов), если хочешь
        fonts
            .families
            .entry(egui::FontFamily::Monospace)
            .or_default()
            .push("my_custom_font".to_owned());

        // Применяем новые настройки шрифтов к контексту
        _cc.egui_ctx.set_fonts(fonts);
        Self {
            state: AppState::Login,
            username_input: String::new(),
            username: String::new(),
            is_connected: false,
            current_msg: String::new(),
            history: Vec::new(),
            users: Vec::new(),
            tx_to_net: None,
            rx_from_net: None,
        }
    }

    // Метод подключения, который запускает сетевой поток
    fn connect(&mut self, nickname: String) {
        let (tx_to_ui, rx_from_net) = channel::<ChatEvent>();
        let (tx_to_net, mut rx_from_ui) = tokio::sync::mpsc::unbounded_channel::<String>();

        // Сохраняем каналы и меняем состояние на Chat
        self.tx_to_net = Some(tx_to_net);
        self.rx_from_net = Some(rx_from_net);
        self.username = nickname.clone();
        self.state = AppState::Chat;
        self.history
            .push(format!("Попытка подключения к серверу..."));

        // Запускаем фоновый поток для асинхронного сетевого рантайма Tokio
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let url = "ws://82.21.114.110:8080/ws";

                if let Ok((ws_stream, _)) = connect_async(url).await {
                    let (mut write, mut read) = ws_stream.split();

                    // Отправляем никнейм, который ввел пользователь
                    let _ = write.send(WsMessage::Text(nickname)).await;

                    let tx_to_ui_clone = tx_to_ui.clone();
                    let mut read_task = tokio::spawn(async move {
                        while let Some(Ok(WsMessage::Text(text))) = read.next().await {
                            if let Ok(event) = serde_json::from_str::<ChatEvent>(&text) {
                                let _ = tx_to_ui_clone.send(event);
                            }
                        }
                    });

                    let mut write_task = tokio::spawn(async move {
                        while let Some(msg_text) = rx_from_ui.recv().await {
                            let json_msg = serde_json::to_string(&msg_text).unwrap();
                            if write.send(WsMessage::Text(json_msg)).await.is_err() {
                                break;
                            }
                        }
                    });

                    tokio::select! {
                        _ = &mut read_task => write_task.abort(),
                        _ = &mut write_task => read_task.abort(),
                    }
                } else {
                    println!("Ошибка: не удалось подключиться к серверу!");
                }
            });
        });
    }
}

impl eframe::App for ChatApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // 1. Опрашиваем канал на наличие новых сообщений от сервера (если он создан)
        if let Some(rx) = &self.rx_from_net {
            while let Ok(event) = rx.try_recv() {
                self.is_connected = true;
                match event {
                    ChatEvent::Message { user, text } => {
                        self.history.push(format!("{}: {}", user, text));
                    }
                    ChatEvent::SyncUsers(active_users) => {
                        self.users = active_users;
                    }
                    ChatEvent::UserJoined(name) => {
                        self.history.push(format!("{} присоединился к чату", name));
                    }
                    ChatEvent::UserLeft(name) => {
                        self.history.push(format!("{} покинул чат", name));
                    }
                }
            }
        }

        // 2. Рендеринг интерфейса в зависимости от текущего состояния
        match self.state {
            AppState::Login => {
                // Экран входа (занимает всю центральную область)
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

                    // Подключаемся по клику или при нажатии Enter в поле ввода
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
                // Левая панель: Список пользователей
                egui::SidePanel::left("users_panel")
                    .resizable(false)
                    .default_width(150.0)
                    .show(ctx, |ui| {
                        ui.heading("В сети:");
                        ui.separator();
                        egui::ScrollArea::vertical().show(ui, |ui| {
                            for user in &self.users {
                                ui.label(format!("👤 {}", user));
                            }
                        });
                    });

                // Центральная панель: Чат и ввод текста
                egui::CentralPanel::default().show(ctx, |ui| {
                    ui.heading("Молодые и успешные");
                    ui.separator();

                    let text_edit_height = ui.spacing().interact_size.y + 10.0;

                    egui::ScrollArea::vertical()
                        .max_height(ui.available_height() - text_edit_height)
                        .stick_to_bottom(true)
                        .show(ui, |ui| {
                            for line in &self.history {
                                ui.label(line);
                            }
                        });

                    ui.separator();

                    ui.horizontal(|ui| {
                        let text_input = ui.add(
                            egui::TextEdit::singleline(&mut self.current_msg)
                                .hint_text("Введите сообщение...")
                                .desired_width(ui.available_width() - 80.0),
                        );

                        let send_btn = ui.button("Послать");

                        if send_btn.clicked()
                            || (text_input.lost_focus()
                                && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                        {
                            if !self.current_msg.trim().is_empty() {
                                // Отправляем текст в фоновый сетевой поток (если канал создан)
                                if let Some(tx) = &self.tx_to_net {
                                    let _ = tx.send(self.current_msg.clone());
                                }
                                self.current_msg.clear();
                            }
                            text_input.request_focus();
                        }
                    });
                });
            }
        }

        // Заставляем UI перерисоваться, если пришли новые сетевые данные
        ctx.request_repaint();
    }
}

fn main() -> eframe::Result<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size(egui::vec2(600.0, 400.0)),
        ..Default::default()
    };

    eframe::run_native(
        "Young & successful chat",
        native_options,
        Box::new(|cc| Box::new(ChatApp::new(cc))),
    )
}
