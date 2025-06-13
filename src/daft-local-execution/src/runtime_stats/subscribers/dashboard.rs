use std::{
    env,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use common_runtime::get_io_runtime;
use tokio::{sync::watch, time::sleep};

use crate::{pipeline::NodeInfo, runtime_stats::subscribers::RuntimeStatsSubscriber};
#[derive(Debug)]
pub struct DashboardSubscriber {
    client: Arc<reqwest::Client>,
    dashboard_url: String,
}

impl DashboardSubscriber {
    pub fn new() -> Self {
        let Ok(url) = env::var("DAFT_DASHBOARD_METRICS_URL") else {
            panic!("DashboardSubscriber::new must only be called after checking if it's enabled via `DashboardSubscriber::is_enabled`")
        };

        let client = if url.contains("localhost") {
            reqwest::Client::builder()
                // if it's a localhost uri we can skip ssl verification
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap()
        } else {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap()
        };
        let client = Arc::new(client);
        let mut receivers = Vec::new();
        let mut senders = Vec::new();

        for _ in 0..10 {
            let (tx, rx) = watch::channel(Option::<NodeInfo>::None);
            receivers.push(rx);
            senders.push(tx);
        }

        Self {
            client,
            dashboard_url: url,
        }
    }

    pub fn is_enabled() -> bool {
        env::var("DAFT_DASHBOARD_ENABLED")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false)
            && env::var("DAFT_DASHBOARD_METRICS_URL").is_ok()
    }

    async fn send_metric(&self, metric_type: &str, context: &NodeInfo, value: u64) {
        let url = self.dashboard_url.clone();

        let mut payload = context.context.clone();
        payload.insert("metric_type".to_string(), metric_type.to_string());
        payload.insert("name".to_string(), context.name.to_string());
        payload.insert("id".to_string(), context.id.to_string());
        payload.insert("value".to_string(), value.to_string());

        if let Ok(run_id) = env::var("DAFT_DASHBOARD_RUN_ID") {
            payload.insert("run_id".to_string(), run_id);
        }
        println!("Payload: {:?}", payload);

        let client = self.client.clone();

        // sleep(Duration::from_millis(10000)).await;
        let req = client.post(dbg!(url));
        let req = if let Ok(auth_token) = env::var("DAFT_DASHBOARD_AUTH_TOKEN") {
            req.bearer_auth(auth_token)
        } else {
            req
        };
        let res = req.json(&payload).send().await;

        if let Err(e) = res {
            #[cfg(debug_assertions)]
            {
                eprintln!("Failed to send metric to dashboard: {}", e);
            }

            log::error!("Failed to send metric to dashboard: {}", e);
        }
    }
}

#[async_trait]
impl RuntimeStatsSubscriber for DashboardSubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn on_rows_received(&self, context: &Arc<NodeInfo>, count: u64) {
        self.send_metric("rows_received", context, count).await;
    }

    async fn on_rows_emitted(&self, context: &Arc<NodeInfo>, count: u64) {
        self.send_metric("rows_emitted", context, count).await;
    }

    async fn on_cpu_time_elapsed(&self, context: &Arc<NodeInfo>, microseconds: u64) {
        self.send_metric("cpu_time_elapsed", context, microseconds)
            .await;
    }
}
