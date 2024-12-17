use std::{cmp::max, sync::Arc};

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

fn num_parallel_exprs(projection: &[ExprRef]) -> usize {
    max(
        projection
            .iter()
            .map(|expr| expr.has_compute())
            .filter(|x| *x)
            .count(),
        1,
    )
}

pub struct ProjectOperator {
    projection: Arc<Vec<ExprRef>>,
    max_concurrency: usize,
    parallel_exprs: usize,
}

impl ProjectOperator {
    pub fn new(projection: Vec<ExprRef>) -> DaftResult<Self> {
        let (max_concurrency, parallel_exprs) = Self::get_optimal_allocation(&projection)?;
        Ok(Self {
            projection: Arc::new(projection),
            max_concurrency,
            parallel_exprs,
        })
    }

    // This function is used to determine the optimal allocation of concurrency and expression parallelism
    fn get_optimal_allocation(projection: &[ExprRef]) -> DaftResult<(usize, usize)> {
        let resource_request = get_resource_request(projection);
        // The number of CPUs available for the operator.
        let available_cpus = match resource_request {
            // If the resource request specifies a number of CPUs, the available cpus is the number of actual CPUs
            // divided by the requested number of CPUs, clamped to (1, NUM_CPUS).
            // E.g. if the resource request specifies 2 CPUs and NUM_CPUS is 4, the number of available cpus is 2.
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
        }?;

        let max_parallel_exprs = num_parallel_exprs(projection);

        // Calculate optimal concurrency using ceiling division
        // Example: For 128 CPUs and 60 parallel expressions:
        // max_concurrency = (128 + 60 - 1) / 60 = 3 concurrent tasks
        // This ensures we never exceed max_parallel_exprs per task
        let max_concurrency = (available_cpus + max_parallel_exprs - 1) / max_parallel_exprs;

        // Calculate actual parallel expressions per task using floor division
        // Example: For 128 CPUs and 3 concurrent tasks:
        // num_parallel_exprs = 128 / 3 = 42 parallel expressions per task
        // This ensures even distribution across concurrent tasks
        let num_parallel_exprs = available_cpus / max_concurrency;

        Ok((max_concurrency, num_parallel_exprs))
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
        let num_parallel_exprs = self.parallel_exprs;
        runtime
            .spawn(async move {
                let out = if num_parallel_exprs > 1 {
                    input
                        .par_eval_expression_list(&projection, num_parallel_exprs)
                        .await?
                } else {
                    input.eval_expression_list(&projection)?
                };
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
        Ok(self.max_concurrency)
    }
}
