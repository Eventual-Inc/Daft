use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_metrics::QueryID;
use common_partitioning::PartitionRef;
use common_runtime::JoinSet;
use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder};
use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::{
    pipeline_node::MaterializedOutput,
    utils::{
        channel::{Receiver, ReceiverStream},
        stream::JoinableForwardingStream,
    },
};

mod distributed_physical_plan;
mod runner;
pub(crate) use distributed_physical_plan::DistributedPhysicalPlanCollector;
#[cfg(test)]
pub(crate) use runner::RunningPlan;
pub(crate) use runner::{PlanConfig, PlanExecutionContext, PlanRunner, TaskIDCounter};

/// Internal scheduler counter for the # of queries executed so far.
static QUERY_IDX_COUNTER: AtomicU16 = AtomicU16::new(0);
/// Index of a query in the scheduler.
/// Lower indexes (aka earlier queries) should have priority in scheduling.
pub(crate) type QueryIdx = u16;

/// A `DistributedPipeline` describes the tree of distributed pipeline nodes that Flotilla
/// will instantiate for a query. It is *not* the actual per-worker physical execution —
/// each pipeline node produces its own local `LocalPhysicalPlan`s at runtime (one per task
/// via `produce_tasks`). The aggregate of those local plans — the real distributed
/// physical plan — is collected during execution into `DistributedPhysicalPlan`
/// (see `distributed_physical_plan.rs`).
#[derive(Serialize, Deserialize)]
pub(crate) struct DistributedPipeline {
    query_idx: QueryIdx,
    query_id: QueryID,
    logical_plan: Arc<LogicalPlan>,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPipeline {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        query_id: QueryID,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let logical_plan = builder.build();

        Ok(Self {
            query_idx: QUERY_IDX_COUNTER.fetch_add(1, Ordering::Relaxed),
            query_id,
            logical_plan,
            config,
        })
    }

    pub fn idx(&self) -> QueryIdx {
        self.query_idx
    }

    pub fn query_id(&self) -> QueryID {
        self.query_id.clone()
    }

    pub fn logical_plan(&self) -> &daft_logical_plan::LogicalPlanRef {
        &self.logical_plan
    }

    pub fn execution_config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }
}

pub(crate) type PlanResultStream =
    JoinableForwardingStream<Box<dyn Stream<Item = PartitionRef> + Send + Unpin + 'static>>;

pub(crate) struct PlanResult {
    joinset: JoinSet<DaftResult<()>>,
    rx: Receiver<MaterializedOutput>,
    /// Handle to the real-physical-plan collector. Callers keep this alive past
    /// stream consumption so they can call `snapshot()` once the query has
    /// finished to retrieve the aggregated local plans.
    physical_plan_collector: DistributedPhysicalPlanCollector,
}

impl PlanResult {
    fn new(
        joinset: JoinSet<DaftResult<()>>,
        rx: Receiver<MaterializedOutput>,
        physical_plan_collector: DistributedPhysicalPlanCollector,
    ) -> Self {
        Self {
            joinset,
            rx,
            physical_plan_collector,
        }
    }

    pub fn physical_plan_collector(&self) -> DistributedPhysicalPlanCollector {
        self.physical_plan_collector.clone()
    }

    pub fn into_stream(self) -> PlanResultStream {
        JoinableForwardingStream::new(
            Box::new(ReceiverStream::new(self.rx).flat_map(|mat| stream::iter(mat.into_inner().0))),
            self.joinset,
        )
    }
}
