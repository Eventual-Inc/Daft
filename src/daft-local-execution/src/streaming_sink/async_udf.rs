use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use common_error::DaftResult;
use common_metrics::{
    CPU_US_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, StatSnapshot, operator_metrics::OperatorCounter,
    ops::NodeType,
};
use daft_core::{prelude::SchemaRef, series::Series};
use daft_dsl::{
    expr::bound_expr::BoundExpr, functions::python::UDFProperties,
    operator_metrics::OperatorMetrics,
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use opentelemetry::{KeyValue, global, metrics::Meter};
use smallvec::SmallVec;
use tracing::{Span, instrument};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
    StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{
    ExecutionTaskSpawner, TaskSet,
    intermediate_ops::udf::remap_used_cols,
    pipeline::{MorselSizeRequirement, NodeName},
    runtime_stats::{Counter, RuntimeStats},
};

struct AsyncUdfParams {
    expr: BoundExpr,
    udf_properties: UDFProperties,
    passthrough_columns: Vec<BoundExpr>,
    output_schema: SchemaRef,
    required_cols: Vec<usize>,
}

struct AsyncUdfRuntimeStats {
    meter: Meter,
    node_kv: Vec<KeyValue>,
    cpu_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    custom_counters: Mutex<HashMap<Arc<str>, Counter>>,
}

impl RuntimeStats for AsyncUdfRuntimeStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        let counters = self.custom_counters.lock().unwrap();
        let mut entries = SmallVec::with_capacity(3 + counters.len());

        entries.push((
            CPU_US_KEY.into(),
            Stat::Duration(Duration::from_micros(self.cpu_us.load(ordering))),
        ));
        entries.push((ROWS_IN_KEY.into(), Stat::Count(self.rows_in.load(ordering))));
        entries.push((
            ROWS_OUT_KEY.into(),
            Stat::Count(self.rows_out.load(ordering)),
        ));

        for (name, counter) in counters.iter() {
            entries.push((name.clone().into(), Stat::Count(counter.load(ordering))));
        }

        StatSnapshot(entries)
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.add(cpu_us, self.node_kv.as_slice());
    }
}

impl AsyncUdfRuntimeStats {
    fn new(id: usize) -> Self {
        let meter = global::meter("daft.local.node_stats");
        let node_kv = vec![KeyValue::new("node_id", id.to_string())];

        Self {
            cpu_us: Counter::new(&meter, CPU_US_KEY.into(), None),
            rows_in: Counter::new(&meter, ROWS_IN_KEY.into(), None),
            rows_out: Counter::new(&meter, ROWS_OUT_KEY.into(), None),
            custom_counters: Mutex::new(HashMap::new()),
            node_kv,
            meter,
        }
    }

    fn update_metrics(&self, metrics: OperatorMetrics) {
        let mut counters = self.custom_counters.lock().unwrap();
        for (name, counter_data) in metrics {
            let OperatorCounter {
                value,
                description,
                attributes,
            } = counter_data;

            let mut key_values = self.node_kv.clone();
            key_values.extend(attributes.into_iter().map(|(k, v)| KeyValue::new(k, v)));

            match counters.get_mut(name.as_str()) {
                Some(existing) => {
                    existing.add(value, key_values.as_slice());
                }
                None => {
                    let counter = Counter::new(
                        &self.meter,
                        name.clone().into(),
                        description.map(Cow::Owned),
                    );
                    counter.add(value, key_values.as_slice());
                    counters.insert(name.into(), counter);
                }
            }
        }
    }
}

pub struct AsyncUdfSink {
    params: Arc<AsyncUdfParams>,
}

const DEFAULT_MAX_INFLIGHT_TASKS: usize = 64;
fn get_max_inflight_tasks() -> usize {
    let max_inflight_tasks = std::env::var("DAFT_MAX_ASYNC_UDF_INFLIGHT_TASKS");
    if let Ok(max_inflight_tasks) = max_inflight_tasks {
        if let Ok(max_inflight_tasks) = max_inflight_tasks.parse::<usize>() {
            max_inflight_tasks.max(1)
        } else {
            DEFAULT_MAX_INFLIGHT_TASKS
        }
    } else {
        DEFAULT_MAX_INFLIGHT_TASKS
    }
}

impl AsyncUdfSink {
    pub fn new(
        expr: BoundExpr,
        udf_properties: UDFProperties,
        passthrough_columns: Vec<BoundExpr>,
        output_schema: &SchemaRef,
    ) -> Self {
        let (expr, required_cols) = remap_used_cols(expr);

        Self {
            params: Arc::new(AsyncUdfParams {
                expr,
                udf_properties,
                passthrough_columns,
                output_schema: output_schema.clone(),
                required_cols,
            }),
        }
    }
}

pub struct AsyncUdfState {
    udf_expr: BoundExpr,
    task_set: TaskSet<DaftResult<RecordBatch>>,
    udf_initialized: bool,
}

impl StreamingSink for AsyncUdfSink {
    type State = AsyncUdfState;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
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
            let runtime_stats = spawner
                .runtime_stats
                .clone()
                .as_any_arc()
                .downcast::<AsyncUdfRuntimeStats>()
                .expect("Expected AsyncUdfRuntimeStats in task_spawner.runtime_stats");
            spawner
                .spawn(
                    {
                        async move {
                            use daft_dsl::functions::python::initialize_udfs;

                            if !state.udf_initialized {
                                state.udf_expr = BoundExpr::new_unchecked(initialize_udfs(
                                    state.udf_expr.inner().clone(),
                                )?);
                                state.udf_initialized = true;
                            }

                            // Spawn tasks for each batch
                            for batch in input.record_batches() {
                                let params = params.clone();
                                let expr = state.udf_expr.clone();
                                let batch = batch.clone();
                                let runtime_stats = runtime_stats.clone();
                                state.task_set.spawn(async move {
                                    let func_input =
                                        batch.get_columns(params.required_cols.as_slice());

                                    let mut collected_metrics = OperatorMetrics::default();
                                    let mut result: Series = func_input
                                        .eval_expression_async_with_metrics(
                                            expr,
                                            &mut collected_metrics,
                                        )
                                        .await?;
                                    runtime_stats.update_metrics(collected_metrics);

                                    if result.len() == 1 {
                                        result = result.broadcast(batch.num_rows())?;
                                    }

                                    let passthrough_input = batch.eval_expression_list(
                                        params.passthrough_columns.as_slice(),
                                    )?;
                                    let output_batch = passthrough_input
                                        .append_column(params.output_schema.clone(), result)?;
                                    Ok(output_batch)
                                });
                            }

                            // Drain any ready tasks non-blockingly
                            let mut ready_batches = Vec::new();
                            while let Some(join_res) = state.task_set.try_join_next() {
                                let batch = join_res??;
                                ready_batches.push(batch);
                            }

                            // Force drain tasks until the number of inflight tasks is less than the concurrency limit
                            let mut num_inflight_tasks = state.task_set.len();
                            let max_inflight_tasks = get_max_inflight_tasks();
                            while num_inflight_tasks > max_inflight_tasks {
                                if let Some(join_res) = state.task_set.join_next().await {
                                    let batch = join_res??;
                                    ready_batches.push(batch);
                                }
                                num_inflight_tasks = state.task_set.len();
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
        mut states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        debug_assert!(states.len() == 1, "AsyncUdfSink should only have one state");
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let state = states.first_mut().unwrap();
                    if let Some(join_res) = state.task_set.join_next().await {
                        let batch = join_res??;
                        Ok(StreamingSinkFinalizeOutput::HasMoreOutput {
                            states,
                            output: Some(Arc::new(MicroPartition::new_loaded(
                                params.output_schema.clone(),
                                Arc::new(vec![batch]),
                                None,
                            ))),
                        })
                    } else {
                        Ok(StreamingSinkFinalizeOutput::Finished(None))
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

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(AsyncUdfState {
            udf_expr: self.params.expr.clone(),
            task_set: TaskSet::new(),
            udf_initialized: false,
        })
    }

    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(AsyncUdfRuntimeStats::new(id))
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.params
            .udf_properties
            .batch_size
            .map(MorselSizeRequirement::Strict)
            .or_else(|| {
                let is_scalar_udf = self.params.udf_properties.is_scalar;
                if is_scalar_udf {
                    Some(MorselSizeRequirement::Strict(1))
                } else {
                    None
                }
            })
    }
    fn batching_strategy(&self) -> Self::BatchingStrategy {
        crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        )
    }
}
