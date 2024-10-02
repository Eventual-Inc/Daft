use std::sync::{atomic::AtomicBool, Mutex};

use lazy_static::lazy_static;
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::{layer::SubscriberExt, Registry};
use tracing_tree::HierarchicalLayer;

static TRACING_INIT: AtomicBool = AtomicBool::new(false);

lazy_static! {
    static ref CHROME_GUARD_HANDLE: Mutex<Option<tracing_chrome::FlushGuard>> = Mutex::new(None);
}

pub fn init_tracing(enable_chrome_trace: bool) {
    use std::sync::atomic::Ordering;

    if TRACING_INIT.swap(true, Ordering::Relaxed) {
        panic!("Cannot init tracing, already initialized!");
    }

    // if !enable_chrome_trace {
    //     return; // Do nothing for now
    // }
    println!("init tracing");

    let layer = HierarchicalLayer::default()
        .with_writer(std::io::stdout)
        .with_indent_lines(true)
        .with_indent_amount(2)
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_targets(true);

    let subscriber = Registry::default().with(layer);
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // let mut mg = CHROME_GUARD_HANDLE.lock().unwrap();
    // assert!(
    //     mg.is_none(),
    //     "Expected chrome flush guard to be None on init"
    // );

    // let (chrome_layer, guard) = ChromeLayerBuilder::new()
    //     .trace_style(tracing_chrome::TraceStyle::Threaded)
    //     .name_fn(Box::new(|event_or_span| {
    //         match event_or_span {
    //             tracing_chrome::EventOrSpan::Event(ev) => ev.metadata().name().into(),
    //             tracing_chrome::EventOrSpan::Span(s) => {
    //                 // TODO: this is where we should extract out fields (such as node id to show the different pipelines)
    //                 s.name().into()
    //             }
    //         }
    //     }))
    //     .build();

    // tracing::subscriber::set_global_default(
    //     tracing_subscriber::registry().with(chrome_layer),
    // )
    // .unwrap();

    // *mg = Some(guard);
}

pub fn refresh_chrome_trace() -> bool {
    let mut mg = CHROME_GUARD_HANDLE.lock().unwrap();
    if let Some(fg) = mg.as_mut() {
        fg.start_new(None);
        true
    } else {
        false
    }
}
