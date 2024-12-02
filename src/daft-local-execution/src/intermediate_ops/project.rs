use std::{cmp::max, sync::Arc};

use common_resource_request::ResourceRequest;
use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_functions::uri::get_max_connections;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::{
    dispatcher::{RoundRobinDispatcher, UnorderedDispatcher},
    NUM_CPUS,
};

pub struct ProjectOperator {
    projection: Arc<Vec<ExprRef>>,
    resource_request: Option<ResourceRequest>,
}

impl ProjectOperator {
    pub fn new(projection: Vec<ExprRef>, resource_request: Option<ResourceRequest>) -> Self {
        Self {
            projection: Arc::new(projection),
            resource_request,
        }
    }
}

impl IntermediateOperator for ProjectOperator {
    #[instrument(skip_all, name = "ProjectOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        runtime: &RuntimeRef,
    ) -> IntermediateOpExecuteResult {
        let projection = self.projection.clone();
        runtime
            .spawn(async move {
                let out = input.eval_expression_list(&projection)?;
                Ok((
                    state,
                    IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "ProjectOperator"
    }

    fn max_concurrency(&self) -> usize {
        self.resource_request
            .as_ref()
            .and_then(|r| r.num_cpus())
            .map(|requested_cpus| {
                let safe_cpu_count = max(requested_cpus as usize, 1);
                max(*NUM_CPUS / safe_cpu_count, 1)
            })
            .unwrap_or(*NUM_CPUS)
    }

    fn dispatch_spawner(
        &self,
        runtime_handle: &crate::ExecutionRuntimeContext,
        maintain_order: bool,
    ) -> Arc<dyn crate::dispatcher::DispatchSpawner> {
        let morsel_size = get_max_connections(&self.projection)
            .unwrap_or_else(|| runtime_handle.default_morsel_size());
        if maintain_order {
            Arc::new(RoundRobinDispatcher::new(Some(morsel_size)))
        } else {
            Arc::new(UnorderedDispatcher::new(Some(morsel_size)))
        }
    }
}
