use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use log::Log;

/// A logger that can be internally modified at runtime.
/// Usually, loggers can only be initialized once, but this container can
/// swap out the internal logger at runtime atomically.
pub struct SwappableLogger {
    base: ArcSwap<Box<dyn Log + Send + Sync + 'static>>,
    temp: ArcSwap<Option<Box<dyn Log + Send + Sync + 'static>>>,
}

impl SwappableLogger {
    pub fn new(logger: Box<dyn Log + Send + Sync + 'static>) -> Self {
        Self {
            base: ArcSwap::new(Arc::new(logger)),
            temp: ArcSwap::new(Arc::new(None)),
        }
    }

    pub fn set_base_logger(&self, logger: Box<dyn Log + Send + Sync + 'static>) {
        self.base.store(Arc::new(logger));
    }

    pub fn get_base_logger(&self) -> Arc<Box<dyn Log + Send + Sync + 'static>> {
        self.base.load().to_owned()
    }

    pub fn set_temp_logger(&self, logger: Box<dyn Log + Send + Sync + 'static>) {
        self.temp.store(Arc::new(Some(logger)));
    }

    pub fn reset_temp_logger(&self) {
        self.temp.store(Arc::new(None));
    }
}

impl Log for SwappableLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        if let Some(temp) = self.temp.load().as_ref() {
            temp.enabled(metadata)
        } else {
            self.base.load().enabled(metadata)
        }
    }

    fn log(&self, record: &log::Record) {
        if let Some(temp) = self.temp.load().as_ref() {
            temp.log(record);
        } else {
            self.base.load().log(record);
        }
    }

    fn flush(&self) {
        if let Some(temp) = self.temp.load().as_ref() {
            temp.flush();
        } else {
            self.base.load().flush();
        }
    }
}

/// A Noop logger that does nothing.
/// Used for initialization purposes only, should never actually be used.
struct NoopLogger;

impl Log for NoopLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        false
    }

    fn log(&self, _record: &log::Record) {}

    fn flush(&self) {}
}

/// The global logger that can be swapped out at runtime.
/// This is initialized to a NoopLogger to avoid any logging during initialization.
/// It can be swapped out with a real logger using `set_inner_logger`.
pub static GLOBAL_LOGGER: LazyLock<Arc<SwappableLogger>> =
    LazyLock::new(|| Arc::new(SwappableLogger::new(Box::new(NoopLogger))));
