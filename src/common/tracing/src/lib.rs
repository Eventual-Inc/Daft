#![feature(let_chains)]

use tracing_subscriber::{layer::SubscriberExt, Layer};
mod chrome;
mod otel;

pub use otel::should_enable_opentelemetry;
#[cfg(feature = "python")]
pub mod python;

pub fn init_tracing() {
    let mut layers = Vec::new();
    if let Some(layer) = chrome::init_tracing() {
        layers.push(layer.boxed());
    }
    if let Some(layer) = otel::init_opentelemetry_providers() {
        layers.push(layer.boxed());
    }
    let registry = tracing_subscriber::registry().with(layers);
    tracing::subscriber::set_global_default(registry).unwrap();
}

pub fn start_tracing() {
    chrome::start_new_chrome_trace();
}

pub fn flush_tracing() {
    chrome::finish_chrome_trace();
    otel::flush_opentelemetry_providers();
}
