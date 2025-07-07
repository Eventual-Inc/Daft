use std::{cmp::max, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_runtime::get_compute_pool_num_threads;
use daft_dsl::{
    common_treenode::{self, TreeNode},
    expr::bound_expr::BoundExpr,
    functions::{
        python::{get_resource_request, PythonUDF},
        FunctionExpr, ScalarFunction,
    },
    Expr,
};
use daft_functions_uri::download::UrlDownloadArgs;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::{ExecutionRuntimeContext, ExecutionTaskSpawner};
fn num_parallel_exprs(projection: &[BoundExpr]) -> usize {
    max(
        projection
            .iter()
            .filter(|expr| expr.inner().has_compute())
            .count(),
        1,
    )
}

fn smallest_batch_size(prev: Option<usize>, next: Option<usize>) -> Option<usize> {
    match (prev, next) {
        (Some(p), Some(n)) => Some(std::cmp::min(p, n)),
        (Some(p), None) => Some(p),
        (None, Some(n)) => Some(n),
        (None, None) => None,
    }
}

const CONNECTION_BATCH_FACTOR: usize = 4;
const DEFAULT_URL_MAX_CONNECTIONS: usize = 32;

/// Gets the batch size from the first UDF encountered in a given slice of expressions
/// Errors if no UDF is found
pub fn try_get_batch_size(exprs: &[BoundExpr]) -> Option<usize> {
    let mut projection_batch_size = None;
    for expr in exprs {
        expr.inner()
            .apply(|e| {
                let found_batch_size = match e.as_ref() {
                    Expr::Function {
                        func: FunctionExpr::Python(PythonUDF { batch_size, .. }),
                        ..
                    } => *batch_size,
                    Expr::ScalarFunction(ScalarFunction { udf, inputs, .. })
                        if udf.name() == "url_download" =>
                    {
                        let UrlDownloadArgs {
                            max_connections, ..
                        } = inputs.clone().try_into()?;
                        let max_connections =
                            max_connections.unwrap_or(DEFAULT_URL_MAX_CONNECTIONS);
                        Some(max_connections * CONNECTION_BATCH_FACTOR)
                    }
                    _ => None,
                };

                projection_batch_size =
                    smallest_batch_size(projection_batch_size, found_batch_size);
                Ok(common_treenode::TreeNodeRecursion::Continue)
            })
            .unwrap();
    }

    projection_batch_size
}

pub struct ProjectOperator {
    projection: Arc<Vec<BoundExpr>>,
    max_concurrency: usize,
    parallel_exprs: usize,
    memory_request: u64,
    batch_size: Option<usize>,
}

impl ProjectOperator {
    pub fn new(projection: Vec<BoundExpr>) -> DaftResult<Self> {
        let memory_request = get_resource_request(&projection)
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);
        let (max_concurrency, parallel_exprs) = Self::get_optimal_allocation(&projection)?;
        let batch_size = try_get_batch_size(&projection);
        Ok(Self {
            projection: Arc::new(projection),
            memory_request,
            max_concurrency,
            parallel_exprs,
            batch_size,
        })
    }

    // This function is used to determine the optimal allocation of concurrency and expression parallelism
    fn get_optimal_allocation(projection: &[BoundExpr]) -> DaftResult<(usize, usize)> {
        let resource_request = get_resource_request(projection);
        let num_cpus = get_compute_pool_num_threads();
        // The number of CPUs available for the operator.
        let available_cpus = match resource_request {
            // If the resource request specifies a number of CPUs, the available cpus is the number of actual CPUs
            // divided by the requested number of CPUs, clamped to (1, NUM_CPUS).
            // E.g. if the resource request specifies 2 CPUs and NUM_CPUS is 4, the number of available cpus is 2.
            Some(resource_request) if resource_request.num_cpus().is_some() => {
                let requested_num_cpus = resource_request.num_cpus().unwrap();
                if requested_num_cpus > num_cpus as f64 {
                    Err(DaftError::ValueError(format!(
                        "Requested {} CPUs but found only {} available",
                        requested_num_cpus, num_cpus
                    )))
                } else {
                    Ok((num_cpus as f64 / requested_num_cpus).clamp(1.0, num_cpus as f64) as usize)
                }
            }
            _ => Ok(num_cpus),
        }?;

        let max_parallel_exprs = num_parallel_exprs(projection);

        // Calculate optimal concurrency using ceiling division
        // Example: For 128 CPUs and 60 parallel expressions:
        // max_concurrency = 128.div_ceil(60) = 3 concurrent tasks
        let max_concurrency = available_cpus.div_ceil(max_parallel_exprs);

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
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let projection = self.projection.clone();
        let num_parallel_exprs = self.parallel_exprs;
        let memory_request = self.memory_request;
        task_spawner
            .spawn_with_memory_request(
                memory_request,
                async move {
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
        if let Some(resource_request) = get_resource_request(self.projection.as_ref()) {
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
        Ok(self.max_concurrency)
    }

    fn morsel_size_range(&self, runtime_handle: &ExecutionRuntimeContext) -> (usize, usize) {
        if let Some(batch_size) = self.batch_size {
            (batch_size, batch_size)
        } else {
            (0, runtime_handle.default_morsel_size())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::{DataType, Field};
    use daft_dsl::{
        expr::bound_col,
        functions::{FunctionArg, FunctionArgs},
        lit,
    };
    use daft_functions_uri::download::UrlDownload;

    use super::*;

    #[test]
    fn test_get_batch_size() {
        let projection = vec![
            BoundExpr::new_unchecked(bound_col(0, Field::new("a", DataType::Utf8))),
            BoundExpr::new_unchecked(bound_col(1, Field::new("b", DataType::Utf8))),
        ];
        let batch_size = try_get_batch_size(projection.as_slice());
        assert_eq!(batch_size, None);
    }

    #[test]
    fn test_get_batch_size_with_url() {
        let projection = vec![
            BoundExpr::new_unchecked(bound_col(0, Field::new("a", DataType::Utf8))),
            BoundExpr::new_unchecked(bound_col(1, Field::new("b", DataType::Utf8))),
            BoundExpr::new_unchecked(
                Expr::ScalarFunction(ScalarFunction {
                    udf: Arc::new(UrlDownload),
                    inputs: FunctionArgs::try_new(vec![FunctionArg::unnamed(bound_col(
                        0,
                        Field::new("a", DataType::Utf8),
                    ))])
                    .unwrap(),
                })
                .arced(),
            ),
        ];

        let batch_size = try_get_batch_size(projection.as_slice());
        assert_eq!(
            batch_size,
            Some(CONNECTION_BATCH_FACTOR * DEFAULT_URL_MAX_CONNECTIONS)
        );
    }

    #[test]
    fn test_get_batch_size_with_url_and_batch_size() {
        let projection = vec![
            BoundExpr::new_unchecked(bound_col(0, Field::new("a", DataType::Utf8))),
            BoundExpr::new_unchecked(
                Expr::ScalarFunction(ScalarFunction {
                    udf: Arc::new(UrlDownload),
                    inputs: FunctionArgs::try_new(vec![
                        FunctionArg::unnamed(bound_col(0, Field::new("a", DataType::Utf8))),
                        FunctionArg::named("max_connections", lit(10)),
                    ])
                    .unwrap(),
                })
                .arced(),
            ),
        ];

        let batch_size = try_get_batch_size(projection.as_slice());
        assert_eq!(batch_size, Some(CONNECTION_BATCH_FACTOR * 10));
    }

    #[test]
    fn test_get_batch_size_with_multiple_urls() {
        let projection = vec![
            BoundExpr::new_unchecked(bound_col(0, Field::new("a", DataType::Utf8))),
            BoundExpr::new_unchecked(bound_col(1, Field::new("b", DataType::Utf8))),
            BoundExpr::new_unchecked(
                Expr::ScalarFunction(ScalarFunction {
                    udf: Arc::new(UrlDownload),
                    inputs: FunctionArgs::try_new(vec![
                        FunctionArg::unnamed(bound_col(0, Field::new("a", DataType::Utf8))),
                        FunctionArg::named("max_connections", lit(4)),
                    ])
                    .unwrap(),
                })
                .arced(),
            ),
            BoundExpr::new_unchecked(
                Expr::ScalarFunction(ScalarFunction {
                    udf: Arc::new(UrlDownload),
                    inputs: FunctionArgs::try_new(vec![FunctionArg::unnamed(bound_col(
                        1,
                        Field::new("b", DataType::Utf8),
                    ))])
                    .unwrap(),
                })
                .arced(),
            ),
        ];

        let batch_size = try_get_batch_size(projection.as_slice());
        assert_eq!(batch_size, Some(CONNECTION_BATCH_FACTOR * 4));
    }

    // TODO: Add test for UDFs, can't create a fake one for testing
}
