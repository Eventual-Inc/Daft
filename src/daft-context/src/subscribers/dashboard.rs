use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

use common_error::{DaftError, DaftResult};
use common_metrics::{QueryID, QueryPlan};
use common_runtime::{RuntimeRef, get_io_runtime};
use dashmap::DashMap;
use reqwest::{Client, RequestBuilder};
use tokio::{sync::mpsc, task::JoinHandle};
use uuid::Uuid;

use crate::subscribers::{
    Event, QueryMetadata, QueryResult, Subscriber,
    events::{
        InMemoryScanSource, OperatorEndEvent, OperatorStartEvent, PhysicalScanSource, StatsEvent,
        TaskEndEvent, TaskScheduledEvent, TaskSource, TaskStatsUpdateEvent, TaskSubmitEvent,
    },
};

// `daft-dashboard` does not (and shouldn't) depend on `daft-context`, so these
// `From` impls live here. The orphan rule permits them because the trait's
// input type (`TaskSource` etc.) is local to this crate.

impl From<&PhysicalScanSource> for daft_dashboard::engine::PhysicalScanSourceArgs {
    fn from(p: &PhysicalScanSource) -> Self {
        Self {
            source_id: p.source_id,
            scan_tasks: p.scan_tasks,
            paths: p.paths.clone(),
            storage_bytes: p.storage_bytes.map(|b| b as u64),
            estimated_memory_bytes: p.estimated_memory_bytes.map(|b| b as u64),
        }
    }
}

impl From<&InMemoryScanSource> for daft_dashboard::engine::InMemoryScanSourceArgs {
    fn from(m: &InMemoryScanSource) -> Self {
        Self {
            source_id: m.source_id,
            partitions: m.partitions as u64,
            total_bytes: m.total_bytes.map(|b| b as u64),
        }
    }
}

impl From<&TaskSource> for daft_dashboard::engine::TaskSourceArgs {
    fn from(source: &TaskSource) -> Self {
        match source {
            TaskSource::PhysicalScan(p) => Self::PhysicalScan(p.into()),
            TaskSource::InMemoryScan(m) => Self::InMemoryScan(m.into()),
        }
    }
}

const DASHBOARD_EVENT_LIMIT: usize = 512;
const DASHBOARD_SHUTDOWN_TIMEOUT_MS: u64 = 500;

/// Name of the file `daft dashboard start` publishes its URL to, next to the PID file.
pub const DASHBOARD_URL_FILE_NAME: &str = "daft_dashboard.url";
/// Name of the file holding the PID of a running dashboard.
pub const DASHBOARD_PID_FILE_NAME: &str = "daft_dashboard.pid";
const DASHBOARD_LOCK_FILE_NAME: &str = "daft_dashboard.lock";

const ENV_DAFT_DASHBOARD_URL: &str = "DAFT_DASHBOARD_URL";
const ENV_DAFT_DASHBOARD_PID_DIR: &str = "DAFT_DASHBOARD_PID_DIR";

/// Directory used to exchange dashboard discovery files between the CLI and
/// clients.
///
/// Defaults to a private per-user directory so another local user cannot plant
/// a URL file that redirects query reports to their own server.
pub fn dashboard_state_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os(ENV_DAFT_DASHBOARD_PID_DIR)
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }

    default_dashboard_state_dir()
}

#[cfg(unix)]
fn default_dashboard_state_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(dir).join("daft");
    }

    std::env::temp_dir().join(format!("daft-{}", unsafe { libc::geteuid() }))
}

#[cfg(not(unix))]
fn default_dashboard_state_dir() -> PathBuf {
    std::env::temp_dir().join("daft")
}

/// Create the dashboard state directory if needed.
///
/// When `require_private` is set (the default, user-scoped directory), the
/// directory must be owned by the current user and inaccessible to others.
pub fn prepare_dashboard_state_dir(dir: &Path, require_private: bool) -> std::io::Result<()> {
    match std::fs::symlink_metadata(dir) {
        Ok(meta) => {
            if !meta.file_type().is_dir() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("'{}' exists but is not a directory", dir.display()),
                ));
            }
            if require_private {
                validate_private_dir(dir, &meta)?;
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = dir.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(dir)?;

            if require_private {
                let meta = std::fs::symlink_metadata(dir)?;
                validate_private_dir(dir, &meta)?;
            }
            Ok(())
        }
        Err(err) => Err(err),
    }
}

#[cfg(unix)]
fn validate_private_dir(dir: &Path, meta: &std::fs::Metadata) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if meta.uid() != unsafe { libc::geteuid() } || meta.permissions().mode() & 0o077 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "dashboard state directory '{}' must be owned by the current user with mode 0700",
                dir.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_dir(_dir: &Path, _meta: &std::fs::Metadata) -> std::io::Result<()> {
    Ok(())
}

/// Atomically write a discovery file, refusing to follow pre-existing symlinks.
pub fn write_dashboard_state_file(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;

    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("daft-dashboard");
    let tmp_path = dir.join(format!(".{file_name}.{}.{nonce}.tmp", std::process::id()));

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let write_result = (|| {
        let mut file = options.open(&tmp_path)?;
        file.write_all(contents.as_bytes())
    })();

    match write_result.and_then(|()| std::fs::rename(&tmp_path, path)) {
        Ok(()) => Ok(()),
        Err(err) => {
            let _ = std::fs::remove_file(&tmp_path);
            Err(err)
        }
    }
}

/// Serializes writers and cleanup of a dashboard instance's discovery files.
pub struct DashboardStateLock {
    #[cfg(unix)]
    _file: std::fs::File,
}

impl DashboardStateLock {
    pub fn acquire(dir: &Path) -> std::io::Result<Self> {
        let path = dir.join(DASHBOARD_LOCK_FILE_NAME);

        #[cfg(unix)]
        {
            use std::os::unix::{fs::OpenOptionsExt, io::AsRawFd};

            let file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .mode(0o600)
                .open(path)?;

            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(std::io::Error::last_os_error());
            }

            Ok(Self { _file: file })
        }

        #[cfg(not(unix))]
        {
            let _ = path;
            Ok(Self {})
        }
    }
}

#[cfg(unix)]
impl Drop for DashboardStateLock {
    fn drop(&mut self) {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::flock(self._file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

/// Resolve the dashboard a client should publish to.
///
/// `DAFT_DASHBOARD_URL` takes precedence. Otherwise, fall back to the URL file
/// written by `daft dashboard start`, so that a locally started dashboard is
/// discovered automatically without any environment configuration.
pub fn resolve_dashboard_url() -> Option<String> {
    if let Ok(url) = std::env::var(ENV_DAFT_DASHBOARD_URL) {
        let url = url.trim();
        if !url.is_empty() {
            return Some(url.to_string());
        }
    }
    read_published_dashboard_url()
}

fn read_published_dashboard_url() -> Option<String> {
    read_published_dashboard_url_from(std::slice::from_ref(&dashboard_state_dir()))
}

fn read_published_dashboard_url_from(dirs: &[PathBuf]) -> Option<String> {
    for dir in dirs {
        let path = dir.join(DASHBOARD_URL_FILE_NAME);
        let Some(contents) = read_trusted_state_file(&path) else {
            continue;
        };
        let url = contents.trim();
        if url.starts_with("http://") || url.starts_with("https://") {
            return Some(url.to_string());
        }
    }
    None
}

fn read_trusted_state_file(path: &Path) -> Option<String> {
    let meta = std::fs::symlink_metadata(path).ok()?;
    if !meta.file_type().is_file() {
        return None;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        // Reject files owned or writable by other users; a shared temporary
        // directory would otherwise allow redirecting dashboard reports.
        if meta.uid() != unsafe { libc::geteuid() } || meta.permissions().mode() & 0o022 != 0 {
            return None;
        }
    }

    std::fs::read_to_string(path).ok()
}

/// Get the number of seconds from the current time since the UNIX epoch
fn secs_from_epoch() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

#[derive(Debug)]
struct DashboardEvent {
    path: String,
    context: &'static str,
    body: Option<Vec<u8>>,
}

pub struct DashboardSubscriber {
    url: String,
    client: Client,
    runtime: RuntimeRef,
    execution_ids: DashMap<QueryID, String>,
    worker_id: Option<String>,

    dashboard_tx: Option<mpsc::Sender<DashboardEvent>>,
    dashboard_worker: Option<JoinHandle<()>>,
    dropped_events: AtomicU64,
}

impl std::fmt::Debug for DashboardSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DashboardSubscriber")
            .field("url", &self.url)
            .field("client", &self.client)
            .field("runtime", &self.runtime.runtime)
            .field("execution_ids", &self.execution_ids)
            .field("worker_id", &self.worker_id)
            .field("dashboard_tx", &self.dashboard_tx.is_some())
            .field("dashboard_worker", &self.dashboard_worker.is_some())
            .field(
                "dropped_events",
                &self
                    .dropped_events
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish()
    }
}

impl DashboardSubscriber {
    pub fn try_new() -> DaftResult<Option<Self>> {
        let Some(url) = resolve_dashboard_url() else {
            return Ok(None);
        };

        const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

        let client = if url.contains("localhost") || url.contains("127.0.0.1") {
            Client::builder()
                // if it's a localhost uri we can skip ssl verification
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .connect_timeout(std::time::Duration::from_secs(3))
                .timeout(std::time::Duration::from_secs(5))
                .user_agent(USER_AGENT)
                .build()
                .map_err(|e| DaftError::External(Box::new(e)))?
        } else {
            // TODO: Auth handling?
            Client::builder()
                .connect_timeout(std::time::Duration::from_secs(3))
                .timeout(std::time::Duration::from_secs(5))
                .user_agent(USER_AGENT)
                .build()
                .map_err(|e| DaftError::External(Box::new(e)))?
        };

        let runtime = get_io_runtime(false);

        let (dashboard_tx, dashboard_rx) = mpsc::channel(DASHBOARD_EVENT_LIMIT);
        let worker_url = url.clone();
        let worker_client = client.clone();
        let dashboard_worker = runtime.runtime.handle().spawn(async move {
            Self::dashboard_worker_loop(worker_url, worker_client, dashboard_rx).await;
        });

        // Validate that we can connect to the dashboard
        // We log a warning if we can't connect, but we still return the subscriber
        // because the dashboard might come up later.
        let ping_url = format!("{}/api/ping", url);
        let ping_client = client.clone();
        if let Err(e) = runtime.block_within_async_context(async move {
            ping_client
                .get(ping_url)
                .send()
                .await
                .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
                .error_for_status()
                .map_err(|e| DaftError::External(Box::new(e)))?;
            Ok::<_, DaftError>(())
        }) {
            log::warn!("Failed to connect to dashboard at {}: {}", url, e);
        }

        let worker_id = if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            Some(format!("worker-{}", Uuid::new_v4()))
        } else {
            None
        };

        Ok(Some(Self {
            url,
            client,
            runtime,
            execution_ids: DashMap::new(),
            worker_id,
            dashboard_tx: Some(dashboard_tx),
            dashboard_worker: Some(dashboard_worker),
            dropped_events: AtomicU64::new(0),
        }))
    }

    /// Returns true if this process is a flotilla worker.
    /// Workers should not emit most lifecycle events (they race with the driver).
    fn is_worker(&self) -> bool {
        self.worker_id.is_some()
    }

    async fn handle_request(request: RequestBuilder) -> DaftResult<()> {
        request
            .send()
            .await
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;
        Ok(())
    }

    fn try_enqueue(&self, event: DashboardEvent) {
        let Some(tx) = &self.dashboard_tx else {
            return; // shutdown in progress
        };

        if tx.try_send(event).is_err() {
            let dropped = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped.is_multiple_of(100) {
                log::warn!("Dropped {dropped} dashboard events because queue is full");
            }
        }
    }

    fn enqueue_json<T: serde::Serialize>(&self, path: String, context: &'static str, payload: &T) {
        match serde_json::to_vec(payload) {
            Ok(body) => self.try_enqueue(DashboardEvent {
                path,
                context,
                body: Some(body),
            }),
            Err(e) => log::error!("Failed to serialize dashboard payload for {context}: {e}"),
        }
    }

    fn enqueue_no_body(&self, path: String, context: &'static str) {
        self.try_enqueue(DashboardEvent {
            path,
            context,
            body: None,
        });
    }

    async fn dashboard_worker_loop(
        url: String,
        client: Client,
        mut rx: mpsc::Receiver<DashboardEvent>,
    ) {
        while let Some(event) = rx.recv().await {
            let mut req = client.post(format!("{}/{}", url, event.path));
            if let Some(body) = event.body {
                req = req
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body);
            }

            if let Err(e) = Self::handle_request(req).await {
                log::error!("Failed to notify {}: {}", event.context, e);
            }
        }
    }

    // ---- event handlers ----

    fn on_query_start(&self, query_id: QueryID, metadata: Arc<QueryMetadata>) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        self.enqueue_json(
            format!("engine/query/{}/start", query_id),
            "query_start",
            &daft_dashboard::engine::StartQueryArgs {
                start_sec: secs_from_epoch(),
                unoptimized_plan: metadata.unoptimized_plan.clone(),
                runner: Some(metadata.runner.clone()),
                ray_dashboard_url: metadata.ray_dashboard_url.clone(),
                entrypoint: metadata.entrypoint.clone(),
                python_version: metadata.python_version.clone(),
                daft_version: metadata.daft_version.clone(),
                ray_version: metadata.ray_version.clone(),
            },
        );
        Ok(())
    }

    fn on_query_heartbeat(&self, query_id: QueryID) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        self.enqueue_json(
            format!("engine/query/{}/heartbeat", query_id),
            "query_heartbeat",
            &daft_dashboard::engine::QueryHeartbeatArgs {
                timestamp_sec: secs_from_epoch(),
            },
        );
        Ok(())
    }

    fn on_query_end(&self, query_id: QueryID, end_result: QueryResult) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        self.enqueue_json(
            format!("engine/query/{}/end", query_id),
            "query_end",
            &daft_dashboard::engine::FinalizeArgs {
                end_sec: secs_from_epoch(),
                end_state: end_result.end_state,
                error_message: end_result.error_message,
            },
        );
        Ok(())
    }

    fn on_optimization_start(&self, query_id: QueryID) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        self.enqueue_json(
            format!("engine/query/{}/plan_start", query_id),
            "optimization_start",
            &daft_dashboard::engine::PlanStartArgs {
                plan_start_sec: secs_from_epoch(),
            },
        );
        Ok(())
    }

    fn on_optimization_end(&self, query_id: QueryID, optimized_plan: QueryPlan) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        self.enqueue_json(
            format!("engine/query/{}/plan_end", query_id),
            "optimization_end",
            &daft_dashboard::engine::PlanEndArgs {
                plan_end_sec: secs_from_epoch(),
                optimized_plan,
            },
        );
        Ok(())
    }

    fn on_exec_start_with_id(
        &self,
        query_id: QueryID,
        execution_id: &str,
        physical_plan: QueryPlan,
    ) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        self.execution_ids
            .insert(query_id.clone(), execution_id.to_string());

        self.enqueue_json(
            format!("engine/query/{}/exec/start", query_id),
            "exec_start",
            &daft_dashboard::engine::ExecStartArgs {
                exec_start_sec: secs_from_epoch(),
                physical_plan,
            },
        );
        Ok(())
    }

    fn on_exec_start(&self, query_id: QueryID, physical_plan: QueryPlan) -> DaftResult<()> {
        self.on_exec_start_with_id(query_id.clone(), &query_id, physical_plan)
    }

    fn on_exec_end_with_id(&self, query_id: QueryID, _execution_id: &str) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        self.execution_ids.remove(&query_id);

        self.enqueue_json(
            format!("engine/query/{}/exec/end", query_id),
            "exec_end",
            &daft_dashboard::engine::ExecEndArgs {
                exec_end_sec: secs_from_epoch(),
            },
        );
        Ok(())
    }

    fn on_exec_end(&self, query_id: QueryID) -> DaftResult<()> {
        self.on_exec_end_with_id(query_id, "unknown")
    }

    fn on_operator_start(&self, event: &OperatorStartEvent) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        self.enqueue_no_body(
            format!(
                "engine/query/{}/exec/{}/start",
                event.header.query_id, event.operator.node_id
            ),
            "exec_operator_start",
        );
        Ok(())
    }

    fn on_stats(&self, event: &StatsEvent) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        let query_id = event.header.query_id.clone();
        let source_id = self
            .execution_ids
            .get(&query_id)
            .map(|id| id.clone())
            .unwrap_or_else(|| "unknown".to_string());

        self.enqueue_json(
            format!("engine/query/{}/exec/emit_stats", query_id),
            "exec_emit_stats",
            &daft_dashboard::engine::ExecEmitStatsArgsSend {
                source_id,
                stats: event
                    .stats
                    .iter()
                    .map(|(node_id, snapshot)| {
                        (
                            *node_id,
                            snapshot
                                .0
                                .iter()
                                .map(|(name, stat)| (name.to_string(), stat.clone()))
                                .collect(),
                        )
                    })
                    .collect(),
            },
        );
        Ok(())
    }

    fn on_operator_end(&self, event: &OperatorEndEvent) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        self.enqueue_no_body(
            format!(
                "engine/query/{}/exec/{}/end",
                event.header.query_id, event.operator.node_id
            ),
            "exec_operator_end",
        );
        Ok(())
    }

    fn on_task_stats_update(&self, event: &TaskStatsUpdateEvent) -> DaftResult<()> {
        if event.tasks.is_empty() {
            return Ok(());
        }
        let query_id = event.header.query_id.clone();
        self.enqueue_json(
            format!("engine/query/{}/tasks/stats", query_id),
            "tasks_stats_update",
            &daft_dashboard::engine::TasksStatsUpdateArgs {
                timestamp_sec: event.header.timestamp_epoch_secs,
                tasks: event
                    .tasks
                    .iter()
                    .map(|t| daft_dashboard::engine::TaskStatsEntry {
                        task_id: t.task_id,
                        totals: daft_dashboard::engine::TaskTotals {
                            cpu_us: t.cpu_us,
                            rows_in: t.rows_in,
                            rows_out: t.rows_out,
                            bytes_in: t.bytes_in,
                            bytes_out: t.bytes_out,
                        },
                    })
                    .collect(),
            },
        );
        Ok(())
    }

    fn on_task_submit(&self, event: &TaskSubmitEvent) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        let query_id = event.header.query_id.clone();
        let task = &event.task;
        let sources = event.sources.iter().map(Into::into).collect::<Vec<_>>();
        self.enqueue_json(
            format!("engine/query/{}/task/submit", query_id),
            "task_submit",
            &daft_dashboard::engine::TaskSubmitArgs {
                submit_sec: event.header.timestamp_epoch_secs,
                task_id: task.id,
                last_node_id: task.last_node_id as usize,
                node_ids: task.node_ids.iter().map(|n| *n as usize).collect(),
                plan_fingerprint: task.plan_fingerprint,
                name: task.name.as_deref().map(str::to_string),
                sources,
            },
        );
        Ok(())
    }

    fn on_task_scheduled(&self, event: &TaskScheduledEvent) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        let query_id = event.header.query_id.clone();
        let task = &event.task;
        self.enqueue_json(
            format!("engine/query/{}/task/schedule", query_id),
            "task_schedule",
            &daft_dashboard::engine::TaskScheduledArgs {
                scheduled_sec: event.header.timestamp_epoch_secs,
                task_id: task.id,
                last_node_id: task.last_node_id as usize,
                node_ids: task.node_ids.iter().map(|n| *n as usize).collect(),
                plan_fingerprint: task.plan_fingerprint,
                worker_id: event.worker_id.as_deref().map(str::to_string),
            },
        );
        Ok(())
    }

    fn on_task_end(&self, event: &TaskEndEvent) -> DaftResult<()> {
        if self.is_worker() {
            return Ok(());
        }

        let query_id = event.header.query_id.clone();
        let task = &event.task;

        // Aggregate per-snapshot scalars across the task's local plan nodes.
        // `is_task_root` / `is_task_leaf` filtering (and the source-leaf
        // `rows.out` / `bytes.read` fallback) lives in
        // `common_metrics::task_io::TaskExternalIo` so this site and the
        // mid-flight aggregator in `daft-local-execution` stay in lockstep.
        let mut io = common_metrics::task_io::TaskExternalIo::default();
        for (node_info, snapshot) in &event.stats {
            io.accumulate(node_info, snapshot);
        }
        let totals = daft_dashboard::engine::TaskTotals {
            cpu_us: io.cpu_us,
            rows_in: io.rows_in,
            rows_out: io.rows_out,
            bytes_in: io.bytes_in,
            bytes_out: io.bytes_out,
        };

        let outcome = match &event.outcome {
            crate::subscribers::events::TaskOutcome::Success => {
                daft_dashboard::engine::TaskOutcomeArgs::Success
            }
            crate::subscribers::events::TaskOutcome::Failed { message } => {
                daft_dashboard::engine::TaskOutcomeArgs::Failed {
                    message: message.clone(),
                }
            }
            crate::subscribers::events::TaskOutcome::Cancelled => {
                daft_dashboard::engine::TaskOutcomeArgs::Cancelled
            }
        };

        self.enqueue_json(
            format!("engine/query/{}/task/end", query_id),
            "task_end",
            &daft_dashboard::engine::TaskEndArgs {
                end_sec: event.header.timestamp_epoch_secs,
                task_id: task.id,
                last_node_id: task.last_node_id as usize,
                node_ids: task.node_ids.iter().map(|n| *n as usize).collect(),
                plan_fingerprint: task.plan_fingerprint,
                worker_id: event.worker_id.as_deref().map(str::to_string),
                outcome,
                totals,
            },
        );
        Ok(())
    }
}

impl Drop for DashboardSubscriber {
    fn drop(&mut self) {
        // Close channel so worker drains buffered events and exits.
        let _ = self.dashboard_tx.take();

        // Wait up to timeout for clean drain; then force abort.
        if let Some(mut worker) = self.dashboard_worker.take() {
            let shutdown = self.runtime.block_within_async_context(async move {
                if tokio::time::timeout(
                    std::time::Duration::from_millis(DASHBOARD_SHUTDOWN_TIMEOUT_MS),
                    &mut worker,
                )
                .await
                .is_err()
                {
                    log::warn!("Dashboard worker did not drain in time, aborting");
                    worker.abort();
                }
            });

            if let Err(e) = shutdown {
                log::warn!("Dashboard worker shutdown encountered an error: {e}");
            }
        }
    }
}

impl Subscriber for DashboardSubscriber {
    fn on_event(&self, event: Event) -> DaftResult<()> {
        match event {
            Event::QueryStart(e) => {
                self.on_query_start(e.header.query_id, e.metadata)?;
            }
            Event::QueryHeartbeat(e) => {
                self.on_query_heartbeat(e.header.query_id)?;
            }
            Event::QueryEnd(e) => {
                self.on_query_end(e.header.query_id, e.result)?;
            }
            Event::OptimizationStart(e) => {
                self.on_optimization_start(e.header.query_id)?;
            }
            Event::OptimizationComplete(e) => {
                self.on_optimization_end(e.header.query_id, e.optimized_plan)?;
            }
            Event::ExecStart(e) => {
                self.on_exec_start(e.header.query_id, e.physical_plan)?;
            }
            Event::ExecEnd(e) => {
                self.on_exec_end(e.header.query_id)?;
            }
            Event::OperatorStart(e) => {
                self.on_operator_start(&e)?;
            }
            Event::OperatorEnd(e) => {
                self.on_operator_end(&e)?;
            }
            Event::Stats(e) => {
                self.on_stats(&e)?;
            }
            Event::TaskStatsUpdate(e) => {
                self.on_task_stats_update(&e)?;
            }
            Event::ProcessStats(_e) => {}
            Event::TaskSubmit(e) => {
                self.on_task_submit(&e)?;
            }
            Event::TaskScheduled(e) => {
                self.on_task_scheduled(&e)?;
            }
            // Worker-side TaskStart not yet wired to the dashboard; the
            // driver's TaskScheduled stands in as the "running" signal until
            // worker-side wiring lands.
            Event::TaskStart(_) => {}
            Event::TaskEnd(e) => {
                self.on_task_end(&e)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("daft-dashboard-url-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn reads_published_dashboard_url() {
        let dir = temp_dir();
        let url_path = dir.join(DASHBOARD_URL_FILE_NAME);
        write_dashboard_state_file(&url_path, "http://127.0.0.1:3238\n").unwrap();

        assert_eq!(
            read_published_dashboard_url_from(std::slice::from_ref(&dir)),
            Some("http://127.0.0.1:3238".to_string())
        );

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ignores_missing_or_invalid_dashboard_url_files() {
        let empty_dir = temp_dir();
        assert_eq!(
            read_published_dashboard_url_from(std::slice::from_ref(&empty_dir)),
            None
        );

        let invalid_dir = temp_dir();
        write_dashboard_state_file(&invalid_dir.join(DASHBOARD_URL_FILE_NAME), "not-a-url")
            .unwrap();
        assert_eq!(
            read_published_dashboard_url_from(&[empty_dir.clone(), invalid_dir.clone()]),
            None
        );

        std::fs::remove_dir_all(empty_dir).unwrap();
        std::fs::remove_dir_all(invalid_dir).unwrap();
    }

    #[test]
    fn prefers_first_directory_with_a_valid_url() {
        let missing_dir = temp_dir();
        let valid_dir = temp_dir();
        write_dashboard_state_file(
            &valid_dir.join(DASHBOARD_URL_FILE_NAME),
            "http://localhost:9999",
        )
        .unwrap();

        assert_eq!(
            read_published_dashboard_url_from(&[missing_dir.clone(), valid_dir.clone()]),
            Some("http://localhost:9999".to_string())
        );

        std::fs::remove_dir_all(missing_dir).unwrap();
        std::fs::remove_dir_all(valid_dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn ignores_world_writable_url_files() {
        use std::os::unix::fs::PermissionsExt;

        let dir = temp_dir();
        let url_path = dir.join(DASHBOARD_URL_FILE_NAME);
        std::fs::write(&url_path, "http://127.0.0.1:3238").unwrap();
        std::fs::set_permissions(&url_path, std::fs::Permissions::from_mode(0o666)).unwrap();

        assert_eq!(
            read_published_dashboard_url_from(std::slice::from_ref(&dir)),
            None
        );

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn ignores_symlinked_url_files() {
        let dir = temp_dir();
        let target = dir.join("target");
        std::fs::write(&target, "http://127.0.0.1:3238").unwrap();
        std::os::unix::fs::symlink(&target, dir.join(DASHBOARD_URL_FILE_NAME)).unwrap();

        assert_eq!(
            read_published_dashboard_url_from(std::slice::from_ref(&dir)),
            None
        );

        std::fs::remove_dir_all(dir).unwrap();
    }
}
