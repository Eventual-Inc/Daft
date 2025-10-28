use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{prelude::SchemaRef, series::Series};
use daft_dsl::{expr::bound_expr::BoundExpr, functions::python::UDFProperties};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{
    ExecutionTaskSpawner, TaskSet,
    intermediate_ops::udf::remap_used_cols,
    pipeline::{MorselSizeRequirement, NodeName},
};

struct AsyncUdfParams {
    expr: BoundExpr,
    udf_properties: UDFProperties,
    passthrough_columns: Vec<BoundExpr>,
    output_schema: SchemaRef,
    required_cols: Vec<usize>,
}

pub struct AsyncUdfSink {
    params: Arc<AsyncUdfParams>,
}

impl AsyncUdfSink {
    pub fn try_new(
        expr: BoundExpr,
        udf_properties: UDFProperties,
        passthrough_columns: Vec<BoundExpr>,
        output_schema: &SchemaRef,
    ) -> DaftResult<Self> {
        let (expr, required_cols) = remap_used_cols(expr);

        Ok(Self {
            params: Arc::new(AsyncUdfParams {
                expr,
                udf_properties,
                passthrough_columns,
                output_schema: output_schema.clone(),
                required_cols,
            }),
        })
    }
}

pub struct AsyncUdfState {
    udf_expr: BoundExpr,
    #[cfg(feature = "python")]
    task_locals: &'static pyo3_async_runtimes::TaskLocals,
    task_set: TaskSet<DaftResult<RecordBatch>>,
    udf_initialized: bool,
}

impl StreamingSink for AsyncUdfSink {
    type State = AsyncUdfState;

    #[instrument(skip_all, name = "AsyncUdfSink::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        #[cfg(feature = "python")]
        {
            let params = self.params.clone();
            spawner
                .spawn(
                    async move {
                        use daft_dsl::functions::python::initialize_udfs;

                        let input_batches = input.get_tables()?;

                        if !state.udf_initialized {
                            state.udf_expr = BoundExpr::new_unchecked(initialize_udfs(
                                state.udf_expr.inner().clone(),
                            )?);
                            state.udf_initialized = true;
                        }

                        for batch in input_batches.as_ref() {
                            let params = params.clone();
                            let expr = state.udf_expr.clone();
                            let task_locals = state.task_locals;
                            let batch = batch.clone();
                            state.task_set.spawn(async move {
                                let func_input = batch.get_columns(params.required_cols.as_slice());

                                let mut result: Series = func_input
                                    .eval_expression_async(expr, Some(task_locals))
                                    .await?;

                                if result.len() == 1 {
                                    result = result.broadcast(batch.num_rows())?;
                                }

                                let passthrough_input = batch
                                    .eval_expression_list(params.passthrough_columns.as_slice())?;
                                let output_batch = passthrough_input
                                    .append_column(params.output_schema.clone(), result)?;
                                Ok(output_batch)
                            });
                        }

                        // Drain any ready tasks non-blockingly using a zero-timeout join_next loop
                        let mut ready_batches: Vec<RecordBatch> = Vec::new();
                        while let Some(join_res) = state.task_set.try_join_next() {
                            let batch = join_res??;
                            ready_batches.push(batch);
                        }

                        if ready_batches.is_empty() {
                            Ok((state, StreamingSinkOutput::NeedMoreInput(None)))
                        } else {
                            let output = Arc::new(MicroPartition::new_loaded(
                                params.output_schema.clone(),
                                Arc::new(ready_batches),
                                None,
                            ));
                            Ok((state, StreamingSinkOutput::NeedMoreInput(Some(output))))
                        }
                    },
                    Span::current(),
                )
                .into()
        }
        #[cfg(not(feature = "python"))]
        {
            panic!("AsyncUdfSink requires the 'python' feature to be enabled");
        }
    }

    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let mut remaining: Vec<RecordBatch> = Vec::new();
                    for mut state in states {
                        while let Some(join_res) = state.task_set.join_next().await {
                            let batch = join_res??;
                            remaining.push(batch);
                        }
                    }

                    if remaining.is_empty() {
                        Ok(None)
                    } else {
                        let output = Arc::new(MicroPartition::new_loaded(
                            params.output_schema.clone(),
                            Arc::new(remaining),
                            None,
                        ));
                        Ok(Some(output))
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        let udf_name = if let Some((_, udf_name)) = self.params.udf_properties.name.rsplit_once('.')
        {
            udf_name
        } else {
            self.params.udf_properties.name.as_str()
        };
        format!("Async UDF {}", udf_name).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::AsyncUDFProject
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![
            format!("Async UDF: {}", self.params.udf_properties.name.as_str()),
            format!("Expr = {}", self.params.expr),
            format!(
                "Passthrough Columns = [{}]",
                self.params.passthrough_columns.iter().join(", ")
            ),
        ];
        if let Some(resource_request) = &self.params.udf_properties.resource_request {
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

    fn make_state(&self) -> Self::State {
        AsyncUdfState {
            udf_expr: self.params.expr.clone(),
            #[cfg(feature = "python")]
            task_locals: common_runtime::get_task_locals(),
            task_set: TaskSet::new(),
            udf_initialized: false,
        }
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.params
            .udf_properties
            .batch_size
            .map(MorselSizeRequirement::Strict)
    }
}
