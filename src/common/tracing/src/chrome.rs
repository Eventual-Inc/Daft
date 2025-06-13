use std::sync::{atomic::AtomicBool, LazyLock, Mutex};

use tracing_chrome::{ChromeLayerBuilder, FlushGuard};
use tracing_subscriber::{Layer, Registry};

static CHROME_TRACING_INIT: AtomicBool = AtomicBool::new(false);

static CHROME_GUARD_HANDLE: LazyLock<Mutex<Option<FlushGuard>>> =
    LazyLock::new(|| Mutex::new(None));

const CHROME_TRACE_VAR_NAME: &str = "DAFT_DEV_ENABLE_CHROME_TRACE";

pub fn should_enable_chrome_trace() -> bool {
    if let Ok(val) = std::env::var(CHROME_TRACE_VAR_NAME)
        && matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    {
        true
    } else {
        false
    }
}

pub fn init_tracing() -> Option<impl Layer<Registry>> {
    use std::sync::atomic::Ordering;

    if !should_enable_chrome_trace() {
        return None;
    }

    assert!(
        !CHROME_TRACING_INIT.swap(true, Ordering::Relaxed),
        "Cannot init tracing, already initialized!"
    );

    let mut mg = CHROME_GUARD_HANDLE.lock().unwrap();
    assert!(
        mg.is_none(),
        "Expected chrome flush guard to be None on init"
    );

    let (chrome_layer, guard) = ChromeLayerBuilder::new()
        .writer(std::io::sink())
        .trace_style(tracing_chrome::TraceStyle::Threaded)
        .name_fn(Box::new(|event_or_span| {
            match event_or_span {
                tracing_chrome::EventOrSpan::Event(ev) => ev.metadata().name().into(),
                tracing_chrome::EventOrSpan::Span(s) => {
                    // TODO: this is where we should extract out fields (such as node id to show the different pipelines)
                    s.name().into()
                }
            }
        }))
        .build();

    *mg = Some(guard);

    Some(chrome_layer)
}

pub fn start_new_chrome_trace() -> bool {
    let mut mg = CHROME_GUARD_HANDLE.lock().unwrap();
    if let Some(fg) = mg.as_mut() {
        // start_new(None) will let tracing-chrome choose the file and file name.
        fg.start_new(None);
        true
    } else {
        false
    }
}

pub fn finish_chrome_trace() -> bool {
    let mut mg = CHROME_GUARD_HANDLE.lock().unwrap();
    if let Some(fg) = mg.as_mut() {
        // start_new(Some(Box::new(std::io::sink()))) will flush the current trace, and start a new one with a dummy writer.
        // The flush method doesn't actually close the file. The only way to do it is to drop the guard or call 'start_new'.
        // But we can't drop the guard because it's a static and we may have multiple traces per process, so we need to call start_new.
        fg.start_new(Some(Box::new(std::io::sink())));
        true
    } else {
        false
    }
}
