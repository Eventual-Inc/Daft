use log::{Metadata, Record};

pub struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let now = chrono::offset::Utc::now();
            println!("{} - {} - {}", now, record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

pub static LOGGER: SimpleLogger = SimpleLogger;
