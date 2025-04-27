use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use crate::ray::task::RayTaskResultHandle;

#[derive(Debug)]
pub struct SwordfishTask {
    plan: LocalPhysicalPlanRef,
    daft_execution_config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
}
impl SwordfishTask {
    pub fn new(
        plan: LocalPhysicalPlanRef,
        daft_execution_config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
    ) -> Self {
        Self {
            plan,
            daft_execution_config,
            psets,
        }
    }

    pub fn plan(&self) -> LocalPhysicalPlanRef {
        self.plan.clone()
    }

    pub fn estimated_memory_cost(&self) -> usize {
        self.plan.estimated_memory_cost()
    }

    pub fn execution_config(&self) -> Arc<DaftExecutionConfig> {
        self.daft_execution_config.clone()
    }

    pub fn psets(&self) -> HashMap<String, Vec<PartitionRef>> {
        self.psets.clone()
    }
}

pub(crate) enum SwordfishTaskResultHandle {
    #[cfg(feature = "python")]
    Ray(RayTaskResultHandle),
}

impl SwordfishTaskResultHandle {
    pub async fn get_result(&self) -> DaftResult<PartitionRef> {
        match self {
            #[cfg(feature = "python")]
            SwordfishTaskResultHandle::Ray(ray_task_handle) => ray_task_handle.get_result().await,
            #[cfg(not(feature = "python"))]
            _ => panic!("No TaskHandle variants available"),
        }
    }
}
