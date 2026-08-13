use crate::chat_app::ChatApp;
use eframe::egui;
mod chat_app;
mod models;

fn main() -> eframe::Result<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size(egui::vec2(800.0, 500.0)),
        ..Default::default()
    };

    eframe::run_native(
        "Young & successful chat",
        native_options,
        Box::new(|cc| Box::new(ChatApp::new(cc))),
    )
}
