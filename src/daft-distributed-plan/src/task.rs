use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;
use pyo3::PyObject;
use serde::{Deserialize, Serialize};

use crate::ray::ray_task_handle::RayTaskHandle;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Task {
    plan: LocalPhysicalPlanRef,
    daft_execution_config: Arc<DaftExecutionConfig>,
}
impl Task {
    pub fn new(
        plan: LocalPhysicalPlanRef,
        daft_execution_config: Arc<DaftExecutionConfig>,
    ) -> Self {
        Self {
            plan,
            daft_execution_config,
        }
    }

    pub fn plan(&self) -> LocalPhysicalPlanRef {
        self.plan.clone()
    }

    pub fn estimated_memory_cost(&self) -> usize {
        16 * 1024 * 1024 * 1024
    }

    pub fn execution_config(&self) -> Arc<DaftExecutionConfig> {
        self.daft_execution_config.clone()
    }
}

pub(crate) enum TaskHandle {
    Ray(RayTaskHandle),
}

impl TaskHandle {
    pub async fn get_result(&self) -> DaftResult<Vec<PartitionRef>> {
        match self {
            TaskHandle::Ray(ray_task_handle) => ray_task_handle.get_result().await,
        }
    }
}
