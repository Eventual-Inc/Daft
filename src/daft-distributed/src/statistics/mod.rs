use std::sync::{Arc, Mutex};

use common_error::DaftResult;
use common_metrics::StatSnapshot;
use common_runtime::get_io_runtime;
use daft_logical_plan::LogicalPlanRef;

use crate::{
    plan::PlanID,
    scheduling::task::{TaskContext, TaskName, TaskStatus},
};

pub mod http_subscriber;
pub mod rpc_server;
pub use http_subscriber::HttpSubscriber;
pub use rpc_server::RpcServer;

const STATISTICS_LOG_TARGET: &str = "DaftStatisticsManager";

#[cfg(test)]
mod http_subscriber_test;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PlanState {
    pub plan_id: PlanID,
    pub query_id: String,
    pub logical_plan: LogicalPlanRef,
}

#[derive(Debug, Clone)]
pub struct TaskState {
    pub name: TaskName,
    pub status: TaskExecutionStatus,
    pub pending: u32,
    pub completed: u32,
    pub canceled: u32,
    pub failed: u32,
    pub total: u32,
}

#[derive(Debug, Clone)]
pub enum TaskExecutionStatus {
    Created,
    Running,
    Completed,
    Failed,
    Canceled,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum StatisticsEvent {
    TaskSubmitted {
        context: TaskContext,
        name: TaskName,
    },
    #[allow(dead_code)]
    TaskScheduled {
        context: TaskContext,
    },
    TaskCompleted {
        context: TaskContext,
    },
    TaskStarted {
        context: TaskContext,
    },
    LocalPhysicalNodeMetrics {
        // Task context
        plan_id: PlanID,
        stage_id: u16,
        task_id: u32,
        logical_node_id: u32,

        // Required for performing node-type aware metrics aggregation
        local_physical_node_type: String,
        distributed_physical_node_type: String,
        snapshot: StatSnapshot,
    },
    TaskFailed {
        context: TaskContext,
        reason: String,
    },
    TaskCancelled {
        context: TaskContext,
    },
    PlanSubmitted {
        plan_id: PlanID,
        query_id: String,
        logical_plan: LogicalPlanRef,
    },
    PlanStarted {
        plan_id: PlanID,
    },
    PlanFinished {
        plan_id: PlanID,
    },
}

impl StatisticsEvent {
    pub fn plan_id(&self) -> PlanID {
        match self {
            Self::PlanSubmitted { plan_id, .. } => *plan_id,
            Self::PlanStarted { plan_id } => *plan_id,
            Self::PlanFinished { plan_id } => *plan_id,
            Self::TaskSubmitted { context, .. } => context.plan_id,
            Self::TaskScheduled { context } => context.plan_id,
            Self::LocalPhysicalNodeMetrics { plan_id, .. } => *plan_id,
            Self::TaskCompleted { context } => context.plan_id,
            Self::TaskStarted { context } => context.plan_id,
            Self::TaskFailed { context, .. } => context.plan_id,
            Self::TaskCancelled { context } => context.plan_id,
        }
    }
}

impl From<(TaskContext, &DaftResult<TaskStatus>)> for StatisticsEvent {
    fn from((context, task_result): (TaskContext, &DaftResult<TaskStatus>)) -> Self {
        match task_result {
            Ok(task_status) => match task_status {
                TaskStatus::Success { .. } => Self::TaskCompleted { context },
                TaskStatus::Failed { error } => Self::TaskFailed {
                    context,
                    reason: error.to_string(),
                },
                TaskStatus::Cancelled => Self::TaskCancelled { context },
                TaskStatus::WorkerDied => Self::TaskFailed {
                    context,
                    reason: "Worker died".to_string(),
                },
                TaskStatus::WorkerUnavailable => Self::TaskFailed {
                    context,
                    reason: "Worker unavailable".to_string(),
                },
            },
            Err(error) => Self::TaskFailed {
                context,
                reason: error.to_string(),
            },
        }
    }
}

pub trait StatisticsSubscriber: Send + Sync + 'static {
    fn handle_event(&mut self, event: &StatisticsEvent) -> DaftResult<()>;
}

pub type StatisticsManagerRef = Arc<StatisticsManager>;

#[derive(Default)]
pub struct StatisticsManager {
    subscribers: Mutex<Vec<Box<dyn StatisticsSubscriber>>>,
    rpc_server: Mutex<Option<RpcServer>>,
}

impl StatisticsManager {
    pub fn new(subscribers: Vec<Box<dyn StatisticsSubscriber>>) -> StatisticsManagerRef {
        Arc::new(Self {
            subscribers: Mutex::new(subscribers),
            rpc_server: Mutex::new(None),
        })
    }

    pub fn handle_event(&self, event: StatisticsEvent) -> DaftResult<()> {
        let mut subscribers = self.subscribers.lock().unwrap();
        for (i, subscriber) in subscribers.iter_mut().enumerate() {
            tracing::info!(target: STATISTICS_LOG_TARGET, "StatisticsManager calling subscriber {}", i);
            subscriber.handle_event(&event)?;
        }
        Ok(())
    }

    pub async fn start_rpc_server(self: &Arc<Self>, addr: &str) -> DaftResult<()> {
        // Check if server is already running and create new server
        let mut rpc_server = {
            let rpc_server_guard = self.rpc_server.lock().unwrap();
            if rpc_server_guard.is_some() {
                return Err(common_error::DaftError::MiscTransient(Box::new(
                    std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        "RPC server is already running",
                    ),
                )));
            }
            RpcServer::new(self.clone())
        }; // MutexGuard is dropped here

        // Start the server (async operation)
        rpc_server.start(addr).await?;

        // Store the started server
        {
            let mut rpc_server_guard = self.rpc_server.lock().unwrap();
            *rpc_server_guard = Some(rpc_server);
        }

        Ok(())
    }
}

impl Drop for StatisticsManager {
    fn drop(&mut self) {
        tracing::debug!(target: STATISTICS_LOG_TARGET, "Dropping StatisticsManager, shutting down RPC server if running");
        if let Ok(mut rpc_server_guard) = self.rpc_server.lock() {
            if let Some(mut rpc_server) = rpc_server_guard.take() {
                let runtime = get_io_runtime(true);
                runtime.block_on_current_thread(async {
                    let _ = rpc_server.shutdown().await;
                });
                drop(rpc_server);
                tracing::debug!(target: STATISTICS_LOG_TARGET, "StatisticsManager dropped, RPC server shutdown initiated");
            }
        }
    }
}
