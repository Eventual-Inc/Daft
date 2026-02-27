use std::{
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use dashmap::DashMap;
use serde::Serialize;
use tracing::{
    Subscriber,
    span::{Attributes, Id},
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

/// Chrome Trace Event in the Trace Event Format.
#[derive(Clone, Serialize)]
pub struct ChromeTraceEvent {
    pub ph: &'static str,
    pub name: String,
    pub tid: u64,
    pub pid: u64,
    pub ts: f64,
}

/// Per-query trace buffer.
struct TraceBuffer {
    events: Mutex<Vec<ChromeTraceEvent>>,
    start: Instant,
}

/// Data stored in span extensions to cache resolved query_id.
struct SpanQueryId {
    query_id: Option<String>,
    resolved: bool,
}

/// Extension type to store span creation time.
struct SpanStartTime(Instant);

/// Global state for the chrome trace layer.
static TRACE_STATE: LazyLock<TraceState> = LazyLock::new(TraceState::new);

struct TraceState {
    captures: DashMap<String, TraceBuffer>,
    has_captures: AtomicBool,
    finished: DashMap<String, Vec<ChromeTraceEvent>>,
}

impl TraceState {
    fn new() -> Self {
        Self {
            captures: DashMap::new(),
            has_captures: AtomicBool::new(false),
            finished: DashMap::new(),
        }
    }
}

/// Start capturing trace events for a query.
pub fn start_trace(query_id: &str) {
    let state = &*TRACE_STATE;
    state.captures.insert(
        query_id.to_string(),
        TraceBuffer {
            events: Mutex::new(Vec::new()),
            start: Instant::now(),
        },
    );
    state.has_captures.store(true, Ordering::Release);
}

/// Finish capturing and move the trace to the finished store.
pub fn finish_trace(query_id: &str) {
    let state = &*TRACE_STATE;
    if let Some((_, buffer)) = state.captures.remove(query_id) {
        let events = buffer.events.into_inner().unwrap();
        state.finished.insert(query_id.to_string(), events);
    }
    if state.captures.is_empty() {
        state.has_captures.store(false, Ordering::Release);
    }
}

/// Get the Chrome Trace Event Format JSON for a finished query.
pub fn get_trace_json(query_id: &str) -> Option<String> {
    let state = &*TRACE_STATE;
    state
        .finished
        .get(query_id)
        .map(|events| serde_json::to_string(events.value()).unwrap())
}

/// Post trace data to the dashboard server, if `DAFT_DASHBOARD_URL` is set.
/// This is fire-and-forget — errors are logged but not propagated.
pub async fn post_trace_to_dashboard(query_id: &str) {
    let dashboard_url = match std::env::var("DAFT_DASHBOARD_URL") {
        Ok(url) => url,
        Err(_) => return,
    };
    let trace_json = match get_trace_json(query_id) {
        Some(json) => json,
        None => return,
    };
    let url = format!("{}/engine/query/{}/trace", dashboard_url, query_id);
    let result = reqwest::Client::new()
        .post(&url)
        .header("content-type", "application/json")
        .body(trace_json)
        .send()
        .await;
    if let Err(e) = result {
        log::warn!("Failed to post trace data to dashboard: {}", e);
    }
}

/// Check if chrome tracing is enabled via env var.
pub fn is_chrome_trace_enabled() -> bool {
    std::env::var("DAFT_DEV_ENABLE_CHROME_TRACE")
        .ok()
        .is_some_and(|v| matches!(v.trim().to_lowercase().as_str(), "1" | "true"))
}

fn current_thread_id() -> u64 {
    use std::sync::atomic::AtomicU64;
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    thread_local! {
        static TID: u64 = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    }
    TID.with(|id| *id)
}

/// A `tracing::Layer` that captures span timing into per-query Chrome Trace buffers.
///
/// When no queries are being traced (`has_captures` is false), all hooks return
/// immediately after a single atomic load — effectively zero overhead.
pub struct ChromeTraceLayer;

impl<S> Layer<S> for ChromeTraceLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if !TRACE_STATE.has_captures.load(Ordering::Acquire) {
            return;
        }

        let span = ctx.span(id).expect("span not found");
        let mut extensions = span.extensions_mut();

        // Store creation timestamp
        extensions.insert(SpanStartTime(Instant::now()));

        // Extract query_id field if present on this span
        let mut query_id_value = None;
        struct QueryIdVisitor<'a>(&'a mut Option<String>);
        impl tracing::field::Visit for QueryIdVisitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "query_id" {
                    *self.0 = Some(format!("{:?}", value));
                }
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "query_id" {
                    *self.0 = Some(value.to_string());
                }
            }
        }
        attrs.record(&mut QueryIdVisitor(&mut query_id_value));

        let resolved = query_id_value.is_some();
        extensions.insert(SpanQueryId {
            query_id: query_id_value,
            resolved,
        });
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if !TRACE_STATE.has_captures.load(Ordering::Acquire) {
            return;
        }

        let Some(span) = ctx.span(&id) else {
            return;
        };

        // Resolve query_id by walking parent chain
        let query_id = resolve_query_id(&span);
        let Some(query_id) = query_id else {
            return;
        };

        let state = &*TRACE_STATE;
        let Some(buffer) = state.captures.get(&query_id) else {
            return;
        };

        let extensions = span.extensions();
        let Some(start_time) = extensions.get::<SpanStartTime>() else {
            return;
        };

        let name = span.name().to_string();
        let tid = current_thread_id();
        let now = Instant::now();
        let start_us = start_time.0.duration_since(buffer.start).as_micros() as f64;
        let end_us = now.duration_since(buffer.start).as_micros() as f64;
        drop(extensions);

        let mut events = buffer.events.lock().unwrap();
        events.push(ChromeTraceEvent {
            ph: "B",
            name: name.clone(),
            tid,
            pid: 1,
            ts: start_us,
        });
        events.push(ChromeTraceEvent {
            ph: "E",
            name,
            tid,
            pid: 1,
            ts: end_us,
        });
    }
}

fn resolve_query_id<S>(span: &tracing_subscriber::registry::SpanRef<'_, S>) -> Option<String>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    // Check this span
    {
        let extensions = span.extensions();
        if let Some(data) = extensions.get::<SpanQueryId>()
            && data.resolved
        {
            return data.query_id.clone();
        }
    }

    // Walk parent chain
    let mut current = span.parent();
    while let Some(parent) = current {
        let extensions = parent.extensions();
        if let Some(data) = extensions.get::<SpanQueryId>()
            && data.resolved
        {
            return data.query_id.clone();
        }
        drop(extensions);
        current = parent.parent();
    }

    None
}
