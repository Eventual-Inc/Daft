use std::{cmp::max, sync::Arc, time::Duration};

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use common_runtime::get_compute_pool_num_threads;
use daft_dsl::{
    Expr,
    common_treenode::{self, TreeNode},
    expr::bound_expr::BoundExpr,
    functions::{BuiltinScalarFn, BuiltinScalarFnVariant, scalar::ScalarFn},
};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{
    ExecutionTaskSpawner,
    dynamic_batching::{
        DynBatchingStrategy, LatencyConstrainedBatchingStrategy, StaticBatchingStrategy,
    },
    pipeline::{MorselSizeRequirement, NodeName},
};

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

/// Gets the batch size from the first UDF encountered in a given slice of expressions
pub fn try_get_batch_size(exprs: &[BoundExpr]) -> Option<usize> {
    let mut projection_batch_size = None;
    for expr in exprs {
        expr.inner()
            .apply(|e| {
                let found_batch_size = match e.as_ref() {
                    Expr::ScalarFn(ScalarFn::Builtin(BuiltinScalarFn {
                        func: BuiltinScalarFnVariant::Async(udf),
                        inputs,
                        ..
                    })) => udf.preferred_batch_size(inputs.clone()).unwrap_or(None),
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
    batch_size: Option<usize>,
}

impl ProjectOperator {
    pub fn new(projection: Vec<BoundExpr>) -> DaftResult<Self> {
        let (max_concurrency, parallel_exprs) = Self::get_optimal_allocation(&projection)?;
        let batch_size = try_get_batch_size(&projection);
        Ok(Self {
            projection: Arc::new(projection),
            max_concurrency,
            parallel_exprs,
            batch_size,
        })
    }

    // This function is used to determine the optimal allocation of concurrency and expression parallelism
    fn get_optimal_allocation(projection: &[BoundExpr]) -> DaftResult<(usize, usize)> {
        let num_cpus = get_compute_pool_num_threads();
        let max_parallel_exprs = num_parallel_exprs(projection);

        // Calculate optimal concurrency using ceiling division
        // Example: For 128 CPUs and 60 parallel expressions:
        // max_concurrency = 128.div_ceil(60) = 3 concurrent tasks
        let max_concurrency = num_cpus.div_ceil(max_parallel_exprs);

        // Calculate actual parallel expressions per task using floor division
        // Example: For 128 CPUs and 3 concurrent tasks:
        // num_parallel_exprs = 128 / 3 = 42 parallel expressions per task
        // This ensures even distribution across concurrent tasks
        let num_parallel_exprs = num_cpus / max_concurrency;

        Ok((max_concurrency, num_parallel_exprs))
    }
}

impl IntermediateOperator for ProjectOperator {
    type State = ();
    type BatchingStrategy = DynBatchingStrategy;

    #[instrument(skip_all, name = "ProjectOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let projection = self.projection.clone();
        let num_parallel_exprs = self.parallel_exprs;

        task_spawner
            .spawn(
                async move {
                    let out = if num_parallel_exprs > 1 {
                        input
                            .par_eval_expression_list(&projection, num_parallel_exprs)
                            .await?
                    } else {
                        input
                            .eval_expression_list_async(Arc::unwrap_or_clone(projection))
                            .await?
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

    fn name(&self) -> NodeName {
        "Project".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Project
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Project: {}",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(self.max_concurrency)
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.batch_size
            .map(|batch_size| MorselSizeRequirement::Flexible(0, batch_size))
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(())
    }

    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        let cfg = daft_context::get_context().execution_config();

        Ok(if cfg.enable_dynamic_batching {
            match cfg.dynamic_batching_strategy.as_str() {
                "latency_constrained" | "auto" => {
                    let reqs = self.morsel_size_requirement().unwrap_or_default();

                    let MorselSizeRequirement::Flexible(min_batch_size, max_batch_size) = reqs
                    else {
                        return Err(DaftError::ValueError(
                            "cannot use strict batch size requirement with dynamic batching"
                                .to_string(),
                        ));
                    };
                    LatencyConstrainedBatchingStrategy {
                        target_batch_latency: Duration::from_millis(5000),
                        latency_tolerance: Duration::from_millis(1000),
                        step_size_alpha: 2048,
                        correction_delta: 64,
                        b_min: min_batch_size,
                        b_max: max_batch_size,
                    }
                    .into()
                }
                _ => unreachable!("should already be checked in the ctx"),
            }
        } else {
            StaticBatchingStrategy::new(self.morsel_size_requirement().unwrap_or_default()).into()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::{DataType, Field};
    use daft_dsl::{
        expr::bound_col,
        functions::{AsyncScalarUDF, BuiltinScalarFnVariant, FunctionArg, FunctionArgs},
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
                BuiltinScalarFn {
                    func: BuiltinScalarFnVariant::Async(Arc::new(UrlDownload)),
                    inputs: FunctionArgs::try_new(vec![FunctionArg::unnamed(bound_col(
                        0,
                        Field::new("a", DataType::Utf8),
                    ))])
                    .unwrap(),
                }
                .into(),
            ),
        ];

        let batch_size = try_get_batch_size(projection.as_slice());
        assert_eq!(
            batch_size,
            Some(UrlDownload::CONNECTION_BATCH_FACTOR * UrlDownload::DEFAULT_URL_MAX_CONNECTIONS)
        );
    }

    #[test]
    fn test_get_batch_size_with_url_and_batch_size() {
        let projection = vec![
            BoundExpr::new_unchecked(bound_col(0, Field::new("a", DataType::Utf8))),
            BoundExpr::new_unchecked(
                BuiltinScalarFn {
                    func: BuiltinScalarFnVariant::Async(Arc::new(UrlDownload)),
                    inputs: FunctionArgs::try_new(vec![
                        FunctionArg::unnamed(bound_col(0, Field::new("a", DataType::Utf8))),
                        FunctionArg::named("max_connections", lit(10)),
                    ])
                    .unwrap(),
                }
                .into(),
            ),
        ];

        let batch_size = try_get_batch_size(projection.as_slice());
        assert_eq!(batch_size, Some(UrlDownload::CONNECTION_BATCH_FACTOR * 10));
    }

    #[test]
    fn test_get_batch_size_with_multiple_urls() {
        let input_0 = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(bound_col(0, Field::new("a", DataType::Utf8))),
            FunctionArg::named("max_connections", lit(4)),
        ])
        .unwrap();
        let input_1 = FunctionArgs::try_new(vec![FunctionArg::unnamed(bound_col(
            1,
            Field::new("b", DataType::Utf8),
        ))])
        .unwrap();

        let projection = vec![
            BoundExpr::new_unchecked(bound_col(0, Field::new("a", DataType::Utf8))),
            BoundExpr::new_unchecked(bound_col(1, Field::new("b", DataType::Utf8))),
            BoundExpr::new_unchecked(
                BuiltinScalarFn {
                    func: BuiltinScalarFnVariant::Async(Arc::new(UrlDownload)),
                    inputs: input_0.clone(),
                }
                .into(),
            ),
            BoundExpr::new_unchecked(
                BuiltinScalarFn {
                    func: BuiltinScalarFnVariant::Async(Arc::new(UrlDownload)),
                    inputs: input_1.clone(),
                }
                .into(),
            ),
        ];
        let expected_batch_size = vec![input_0, input_1]
            .into_iter()
            .map(|input| UrlDownload.preferred_batch_size(input).unwrap().unwrap())
            .min();

        let batch_size = try_get_batch_size(projection.as_slice());
        assert_eq!(batch_size, expected_batch_size);
    }
}
