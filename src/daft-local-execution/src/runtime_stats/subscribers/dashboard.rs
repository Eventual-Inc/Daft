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

        let client = if url.contains("localhost") {
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
    println!("Payload: {payload:?}");

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

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use tokio::time::sleep;

    use super::*;
    use crate::pipeline::NodeInfo;

    struct TestServer {
        port: u16,
        received_data: Arc<Mutex<Vec<String>>>,
    }

    impl TestServer {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            dbg!(&port);
            let received_data = Arc::new(Mutex::new(Vec::new()));
            let received_data_clone = received_data.clone();

            thread::spawn(move || {
                for stream in listener.incoming() {
                    if let Ok(mut stream) = stream {
                        let received_data = received_data_clone.clone();
                        thread::spawn(move || {
                            let mut buffer = vec![0; 4096];
                            if let Ok(n) = stream.read(&mut buffer) {
                                let data = String::from_utf8_lossy(&buffer[..n]);

                                // Extract JSON from POST body (after \r\n\r\n)
                                if let Some(json_start) = data.find('{') {
                                    if let Some(json_end) = data.rfind('}') {
                                        if json_end >= json_start {
                                            let json = &data[json_start..=json_end];
                                            received_data.lock().unwrap().push(json.to_string());
                                        }
                                    }
                                }

                                let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                                let _ = stream.write_all(response);
                            }
                        });
                    }
                }
            });

            thread::sleep(Duration::from_millis(100));

            Self {
                port,
                received_data,
            }
        }

        fn get_received_data(&self) -> Vec<String> {
            self.received_data.lock().unwrap().clone()
        }

        fn url(&self) -> String {
            format!("http://127.0.0.1:{}", self.port)
        }
    }

    fn create_node_info(name: &str, id: usize) -> NodeInfo {
        NodeInfo {
            name: Arc::from(name.to_string()),
            id,
            context: std::collections::HashMap::new(),
        }
    }

    fn assert_json_contains_fields(json: &str, expected_fields: &[(&str, &str)]) {
        for (field, expected_value) in expected_fields {
            let pattern = format!("\"{}\":\"{}\"", field, expected_value);
            assert!(
                json.contains(&pattern),
                "JSON missing field {}:{}, got: {}",
                field,
                expected_value,
                json
            );
        }
    }

    #[tokio::test]
    async fn test_dashboard_subscriber_comprehensive() {
        let server = TestServer::start();

        std::env::set_var("DAFT_DASHBOARD_ENABLED", "true");
        std::env::set_var("DAFT_DASHBOARD_METRICS_URL", &server.url());
        std::env::set_var("DAFT_DASHBOARD_RUN_ID", "test_run_comprehensive");

        let subscriber =
            DashboardSubscriber::new_with_throttle_interval(Duration::from_millis(100));

        // Test 1: Basic single event
        let event1 = RuntimeStatsEvent {
            rows_received: 150,
            rows_emitted: 75,
            cpu_us: 2500,
            node_info: create_node_info("basic_node", 1),
        };
        subscriber.handle_event(&event1).unwrap();
        sleep(Duration::from_millis(250)).await;

        let data = server.get_received_data();
        assert_eq!(data.len(), 1, "Should have received first request");
        assert_json_contains_fields(
            &data[0],
            &[("name", "basic_node"), ("rows_received", "150")],
        );

        // Test 2: Multiple events (throttling - latest wins)
        let node_info = create_node_info("throttle_node", 2);
        for i in 1..=5 {
            let event = RuntimeStatsEvent {
                rows_received: i * 100,
                rows_emitted: i * 50,
                cpu_us: i * 1000,
                node_info: node_info.clone(),
            };
            subscriber.handle_event(&event).unwrap();
        }
        sleep(Duration::from_millis(250)).await;

        let data = server.get_received_data();
        assert_eq!(data.len(), 2, "Should have 2 requests total");
        assert_json_contains_fields(
            &data[1],
            &[("name", "throttle_node"), ("rows_received", "500")],
        );

        // Test 3: Cross throttle threshold
        let threshold_node = create_node_info("threshold_node", 3);
        let event_a = RuntimeStatsEvent {
            rows_received: 100,
            rows_emitted: 50,
            cpu_us: 1000,
            node_info: threshold_node.clone(),
        };
        subscriber.handle_event(&event_a).unwrap();
        sleep(Duration::from_millis(250)).await;

        let event_b = RuntimeStatsEvent {
            rows_received: 200,
            rows_emitted: 100,
            cpu_us: 2000,
            node_info: threshold_node.clone(),
        };
        subscriber.handle_event(&event_b).unwrap();
        sleep(Duration::from_millis(250)).await;

        let data = server.get_received_data();
        assert_eq!(data.len(), 4, "Should have 4 requests total");
        assert_json_contains_fields(
            &data[2],
            &[("name", "threshold_node"), ("rows_received", "100")],
        );
        assert_json_contains_fields(
            &data[3],
            &[("name", "threshold_node"), ("rows_received", "200")],
        );
    }
}
