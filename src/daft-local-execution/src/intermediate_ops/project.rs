use std::sync::Arc;

use common_error::{DaftError, DaftResult};
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

    fn max_concurrency(&self) -> DaftResult<usize> {
        match &self.resource_request {
            Some(resource_request) => {
                if let Some(requested_num_cpus) = resource_request.num_cpus() {
                    if requested_num_cpus > *NUM_CPUS as f64 {
                        Err(DaftError::ValueError(format!(
                            "Requested {} CPUs but found only {} available",
                            requested_num_cpus, *NUM_CPUS
                        )))
                    } else {
                        Ok((*NUM_CPUS as f64 / requested_num_cpus).ceil() as usize)
                    }
                } else {
                    Ok(*NUM_CPUS)
                }
            }
            None => Ok(*NUM_CPUS),
        }
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
