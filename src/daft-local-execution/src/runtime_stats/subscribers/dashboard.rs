use std::{
    collections::HashMap,
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use common_error::DaftResult;
use common_runtime::get_io_runtime;
use parking_lot::Mutex;
use reqwest::{header, Client};
use tokio::sync::{
    mpsc::{self},
    watch,
};

use crate::runtime_stats::{subscribers::RuntimeStatsSubscriber, RuntimeStatsEvent};

/// Similar to `RuntimeStatsEventHandler`, the `DashboardSubscriber` also has it's own internal mechanism to throttle events.
/// Since there could be many queries broadcasting to the dashboard at the same time, we want to be conscientious about how often we send updates.
#[derive(Debug)]
pub struct DashboardSubscriber {
    senders: Arc<Mutex<HashMap<String, watch::Sender<Option<RuntimeStatsEvent>>>>>,
    new_receiver_tx: mpsc::UnboundedSender<(String, watch::Receiver<Option<RuntimeStatsEvent>>)>,
}

impl DashboardSubscriber {
    fn new_with_throttle_interval(interval: Duration) -> Arc<Self> {
        let Ok(url) = env::var("DAFT_DASHBOARD_METRICS_URL") else {
            panic!("DashboardSubscriber::new must only be called after checking if it's enabled via `DashboardSubscriber::is_enabled`")
        };

        static USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

        let client = if url.contains("localhost") || url.contains("127.0.0.1") {
            reqwest::Client::builder()
                // if it's a localhost uri we can skip ssl verification
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .timeout(std::time::Duration::from_secs(2))
                .user_agent(USER_AGENT)
                .build()
                .unwrap()
        } else {
            let default_headers = if let Ok(auth_token) = env::var("DAFT_DASHBOARD_AUTH_TOKEN") {
                let mut headers = header::HeaderMap::new();
                let mut auth_value = header::HeaderValue::from_str(&auth_token)
                    .expect("Failed to create auth header value");
                auth_value.set_sensitive(true);
                headers.insert(header::AUTHORIZATION, auth_value);
                headers
            } else {
                header::HeaderMap::new()
            };
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .user_agent(USER_AGENT)
                .default_headers(default_headers)
                .build()
                .unwrap()
        };
        let client = Arc::new(client);
        let senders = Arc::new(Mutex::new(HashMap::new()));
        let (new_receiver_tx, mut new_receiver_rx) = mpsc::unbounded_channel();

        let rt = get_io_runtime(true);
        rt.runtime.spawn(async move {
            let mut last_sent: HashMap<String, Instant> = HashMap::new();
            let mut receivers: HashMap<String, watch::Receiver<Option<RuntimeStatsEvent>>> =
                HashMap::new();
            let mut tick_interval = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = tick_interval.tick() => {


                        for (key, rx) in &mut receivers {
                            if rx.has_changed().unwrap_or(false) {
                                let event = rx.borrow_and_update().clone();
                                if let Some(event) = event {
                                    let now = Instant::now();
                                    let should_process = last_sent.get(key).is_none_or(|last| now.duration_since(*last) >= interval);

                                    if should_process {
                                        send_metric(&url, &client, &event).await;
                                        last_sent.insert(key.clone(), now);
                                    }
                                }
                            }
                        }

                        // Cleanup
                        // Remove old events to free up memory
                        let cutoff = Instant::now().checked_sub(Duration::from_secs(300)).unwrap();
                        last_sent.retain(|_, &mut last_time| last_time > cutoff);
                    }
                    Some((key, rx)) = new_receiver_rx.recv() => {
                        receivers.insert(key, rx);
                    }
                }
            }
        });

        Arc::new(Self {
            senders,
            new_receiver_tx,
        })
    }
    pub fn new() -> Arc<Self> {
        Self::new_with_throttle_interval(Duration::from_secs(2))
    }

    pub fn is_enabled() -> bool {
        env::var("DAFT_DASHBOARD_ENABLED")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false)
            && env::var("DAFT_DASHBOARD_METRICS_URL").is_ok()
    }
    fn get_or_create_sender(&self, key: String) -> watch::Sender<Option<RuntimeStatsEvent>> {
        let mut senders = self.senders.lock();
        if let Some(sender) = senders.get(&key) {
            sender.clone()
        } else {
            let (tx, rx) = watch::channel(None);
            senders.insert(key.clone(), tx.clone());
            let _ = self.new_receiver_tx.send((key, rx));
            tx
        }
    }
}

async fn send_metric(url: &str, client: &Arc<Client>, event: &RuntimeStatsEvent) {
    let context = &event.node_info;
    let mut payload = event.node_info.context.clone();

    payload.insert("name".to_string(), context.name.to_string());
    payload.insert("id".to_string(), context.id.to_string());
    payload.insert("rows_received".to_string(), event.rows_received.to_string());
    payload.insert("rows_emitted".to_string(), event.rows_emitted.to_string());
    payload.insert("cpu_usage".to_string(), event.cpu_us.to_string());

    if let Ok(run_id) = env::var("DAFT_DASHBOARD_RUN_ID") {
        payload.insert("run_id".to_string(), run_id);
    }

    let req = client.post(url);
    let res = req.json(&payload).send().await;

    if let Err(e) = res {
        #[cfg(debug_assertions)]
        {
            eprintln!("Failed to send metric to dashboard: {}", e);
        }

        log::error!("Failed to send metric to dashboard: {}", e);
    }
}

impl RuntimeStatsSubscriber for DashboardSubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_event(&self, event: &RuntimeStatsEvent) -> DaftResult<()> {
        let key = format!("{}:{}", event.node_info.name, event.node_info.id);
        let sender = self.get_or_create_sender(key);
        let _ = sender.send(Some(event.clone()));
        Ok(())
    }
}
