use std::sync::{
    Arc, atomic::{AtomicU16, Ordering}
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_metrics::QueryID;
use common_partitioning::PartitionRef;
use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder};
use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::{
    pipeline_node::MaterializedOutput,
    utils::{
        channel::{Receiver, ReceiverStream},
        joinset::JoinSet,
        stream::JoinableForwardingStream,
    },
};

mod runner;
pub(crate) use runner::{QueryConfig, PlanExecutionContext, PlanRunner, TaskIDCounter};

static QUERY_COUNTER: AtomicU16 = AtomicU16::new(0);
pub(crate) type QueryIdx = u16;

#[derive(Serialize, Deserialize)]
pub(crate) struct DistributedPhysicalPlan {
    query_id: QueryID,
    query_idx: QueryIdx,
    logical_plan: Arc<LogicalPlan>,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        query_id: QueryID,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let logical_plan = builder.build();

        Ok(Self {
            query_id,
            query_idx: QUERY_COUNTER.fetch_add(1, Ordering::SeqCst),
            logical_plan,
            config,
        })
    }

    /// Unique query id
    pub fn query_id(&self) -> QueryID {
        self.query_id.clone()
    }

    /// Local query index based on # of queries run before
    pub fn query_idx(&self) -> QueryIdx {
        self.query_idx
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
}

impl PlanResult {
    fn new(joinset: JoinSet<DaftResult<()>>, rx: Receiver<MaterializedOutput>) -> Self {
        Self { joinset, rx }
    }

    pub fn into_stream(self) -> PlanResultStream {
        JoinableForwardingStream::new(
            Box::new(ReceiverStream::new(self.rx).flat_map(|mat| stream::iter(mat.into_inner().0))),
            self.joinset,
        )
    }
}
