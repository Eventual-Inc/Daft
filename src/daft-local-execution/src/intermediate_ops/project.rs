use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_runtime::RuntimeRef;
use daft_dsl::{functions::python::get_resource_request, ExprRef};
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::NUM_CPUS;

pub struct ProjectOperator {
    projection: Arc<Vec<ExprRef>>,
}

impl ProjectOperator {
    pub fn new(projection: Vec<ExprRef>) -> Self {
        Self {
            projection: Arc::new(projection),
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
        let resource_request = get_resource_request(&self.projection);
        match resource_request {
            // If the resource request specifies a number of CPUs, the max concurrency is the number of CPUs
            // divided by the requested number of CPUs, clamped to (1, NUM_CPUS).
            // E.g. if the resource request specifies 2 CPUs and NUM_CPUS is 4, the max concurrency is 2.
            Some(resource_request) if resource_request.num_cpus().is_some() => {
                let requested_num_cpus = resource_request.num_cpus().unwrap();
                if requested_num_cpus > *NUM_CPUS as f64 {
                    Err(DaftError::ValueError(format!(
                        "Requested {} CPUs but found only {} available",
                        requested_num_cpus, *NUM_CPUS
                    )))
                } else {
                    Ok(
                        (*NUM_CPUS as f64 / requested_num_cpus).clamp(1.0, *NUM_CPUS as f64)
                            as usize,
                    )
                }
            }
            _ => Ok(*NUM_CPUS),
        }
    }
}
