use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_dsl::{functions::python::get_resource_request, ExprRef};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::{ExecutionTaskSpawner, NUM_CPUS};

pub struct ProjectOperator {
    projection: Arc<Vec<ExprRef>>,
    memory_request: u64,
}

impl ProjectOperator {
    pub fn new(projection: Vec<ExprRef>) -> Self {
        let memory_request = get_resource_request(&projection)
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);
        Self {
            projection: Arc::new(projection),
            memory_request,
        }
    }
}

impl IntermediateOperator for ProjectOperator {
    #[instrument(skip_all, name = "ProjectOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let projection = self.projection.clone();
        let memory_request = self.memory_request;
        task_spawner
            .spawn_with_memory_request(
                memory_request,
                async move {
                    let out = input.eval_expression_list(&projection)?;
                    Ok((
                        state,
                        IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "Project"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Project: {}",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        if let Some(resource_request) = get_resource_request(&self.projection) {
            let multiline_display = resource_request.multiline_display();
            res.push(format!(
                "Resource request = {{ {} }}",
                multiline_display.join(", ")
            ));
        } else {
            res.push("Resource request = None".to_string());
        }
        res
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
