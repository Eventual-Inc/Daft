use std::{
    borrow::Cow,
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
    vec,
};

use common_error::{DaftError, DaftResult};
use common_metrics::{
    CPU_US_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, StatSnapshot, operator_metrics::OperatorCounter,
    ops::NodeType,
};
use common_resource_request::ResourceRequest;
use common_runtime::get_compute_pool_num_threads;
use daft_core::{prelude::SchemaRef, series::Series};
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    Column, Expr, ExprRef,
    common_treenode::{Transformed, TreeNode},
    expr::{BoundColumn, bound_expr::BoundExpr},
    functions::python::UDFProperties,
    operator_metrics::OperatorMetrics,
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use opentelemetry::{KeyValue, global, metrics::Meter};
#[cfg(feature = "python")]
use pyo3::{Py, prelude::*};
use smallvec::SmallVec;
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
    runtime_stats::{Counter, RuntimeStats},
};

/// Given an expression, extract the indexes of used columns and remap them to
/// new indexes from 0...count-1, where count is the # of used columns.
///
/// Note that if there are no used columns, we just return the first
/// because we can't execute UDFs on empty recordbatches.
pub(crate) fn remap_used_cols(expr: BoundExpr) -> (BoundExpr, Vec<usize>) {
    let mut count = 0;
    let mut cols_to_idx = HashMap::new();
    let new_expr = expr
        .into_inner()
        .transform_down(|expr: ExprRef| {
            if let Expr::Column(Column::Bound(BoundColumn { index, field })) = expr.as_ref() {
                if !cols_to_idx.contains_key(index) {
                    cols_to_idx.insert(*index, count);
                    count += 1;
                }

                let new_index = cols_to_idx[index];
                Ok(Transformed::yes(Arc::new(Expr::Column(Column::Bound(
                    BoundColumn {
                        index: new_index,
                        field: field.clone(),
                    },
                )))))
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .expect("Error occurred when visiting for required columns");

    let required_cols = if cols_to_idx.is_empty() {
        vec![0]
    } else {
        let mut required_cols = vec![0; count];
        for (original_idx, final_idx) in cols_to_idx {
            required_cols[final_idx] = original_idx;
        }
        required_cols
    };

    (BoundExpr::new_unchecked(new_expr.data), required_cols)
}

struct UdfRuntimeStats {
    meter: Meter,
    node_kv: Vec<KeyValue>,
    cpu_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    custom_counters: Mutex<HashMap<Arc<str>, Counter>>,
}

impl RuntimeStats for UdfRuntimeStats {
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

impl UdfRuntimeStats {
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

/// Common parameters for UDF handle and operator
struct UdfParams {
    udf_properties: UDFProperties,
    passthrough_columns: Vec<BoundExpr>,
    output_schema: SchemaRef,
    required_cols: Vec<usize>,
}

#[cfg(feature = "python")]
enum UdfHandle {
    Thread,
    Process(Option<Py<PyAny>>),
}

#[cfg(feature = "python")]
impl UdfHandle {
    fn get_or_create_handle(&mut self, udf_expr: &BoundExpr) -> DaftResult<&mut Py<PyAny>> {
        match self {
            // Create process handle if it doesn't exist
            Self::Process(None) => {
                let py_expr = PyExpr::from(udf_expr.inner().clone());

                let handle = Python::attach(|py| {
                    // create python object
                    Ok::<pyo3::Py<pyo3::PyAny>, PyErr>(
                        py.import(pyo3::intern!(py, "daft.execution.udf"))?
                            .getattr(pyo3::intern!(py, "UdfHandle"))?
                            .call1((py_expr,))?
                            .unbind(),
                    )
                })?;

                *self = Self::Process(Some(handle));
            }
            // Handle already created, nothing to do
            Self::Process(_) => {}
            // Cannot create process handle for Thread variant
            Self::Thread => {
                return Err(DaftError::ValueError(
                    "Cannot create process handle for Thread variant".to_string(),
                ));
            }
        }

        match self {
            Self::Process(Some(handle)) => Ok(handle),
            Self::Process(None) => unreachable!("Process handle should be created by now"),
            Self::Thread => unreachable!("Thread variant does not have a handle"),
        }
    }

    fn teardown(&self) -> DaftResult<()> {
        match self {
            Self::Process(Some(handle)) => Python::attach(|py| {
                handle
                    .bind(py)
                    .call_method0(pyo3::intern!(py, "teardown"))?;
                Ok(())
            }),
            Self::Process(None) => Ok(()),
            Self::Thread => Ok(()),
        }
    }

    fn eval_input_with_handle(
        &mut self,
        expr: &BoundExpr,
        input: RecordBatch,
        udf_name: &str,
        worker_idx: usize,
        runtime_stats: &UdfRuntimeStats,
    ) -> DaftResult<Series> {
        use common_metrics::python::PyOperatorMetrics;
        use daft_recordbatch::python::PyRecordBatch;

        use crate::STDOUT;

        let handle = self.get_or_create_handle(expr)?;
        let (result, stdout_lines, metrics) = Python::attach(|py| {
            let (py_result, py_stdout_lines, py_metrics) = handle
                .bind(py)
                .call_method1(
                    pyo3::intern!(py, "eval_input"),
                    (PyRecordBatch::from(input),),
                )?
                .extract::<(PyRecordBatch, Vec<String>, PyOperatorMetrics)>()?;
            PyResult::Ok((
                RecordBatch::from(py_result),
                py_stdout_lines,
                py_metrics.inner,
            ))
        })?;

        let label = format!("[`{}` Worker #{}]", udf_name, worker_idx);
        for line in stdout_lines {
            STDOUT.print(&label, &line);
        }

        runtime_stats.update_metrics(metrics);

        debug_assert!(
            result.num_columns() == 1,
            "UDF should return a single column"
        );
        Ok(result.get_column(0).clone())
    }

    fn eval_input_inline(
        &self,
        udf_expr: &mut BoundExpr,
        func_input: RecordBatch,
        runtime_stats: &UdfRuntimeStats,
    ) -> DaftResult<Series> {
        use daft_dsl::functions::python::initialize_udfs;

        // Only actually initialized the first time
        *udf_expr = BoundExpr::new_unchecked(initialize_udfs(udf_expr.inner().clone())?);
        let mut collected_metrics = OperatorMetrics::default();
        let result = func_input.eval_expression_with_metrics(udf_expr, &mut collected_metrics)?;
        runtime_stats.update_metrics(collected_metrics);
        Ok(result)
    }

    pub(crate) fn eval_input(
        &mut self,
        expr: &mut BoundExpr,
        params: &UdfParams,
        worker_idx: usize,
        input: Arc<MicroPartition>,
        runtime_stats: Arc<UdfRuntimeStats>,
    ) -> DaftResult<Arc<MicroPartition>> {
        let input_batches = input.record_batches();
        let mut output_batches = Vec::with_capacity(input_batches.len());

        for batch in input_batches {
            // Prepare inputs
            let func_input = batch.get_columns(params.required_cols.as_slice());

            // Call the UDF
            let mut result_series = match self {
                Self::Thread => self.eval_input_inline(expr, func_input, &runtime_stats)?,
                #[cfg(feature = "python")]
                Self::Process(_) => self.eval_input_with_handle(
                    expr,
                    func_input,
                    params.udf_properties.name.as_str(),
                    worker_idx,
                    &runtime_stats,
                )?,
            };

            // If result.len() == 1 (because it was a 0-column UDF), broadcast to right size
            if result_series.len() == 1 {
                result_series = result_series.broadcast(batch.num_rows())?;
            }

            // Append result to passthrough
            let passthrough_input =
                batch.eval_expression_list(params.passthrough_columns.as_slice())?;
            let output_batch =
                passthrough_input.append_column(params.output_schema.clone(), result_series)?;
            output_batches.push(output_batch);
        }

        Ok(Arc::new(MicroPartition::new_loaded(
            params.output_schema.clone(),
            Arc::new(output_batches),
            None,
        )))
    }
}

#[cfg(feature = "python")]
impl Drop for UdfHandle {
    fn drop(&mut self) {
        let result = self.teardown();

        if let Err(e) = result {
            log::error!("Error tearing down UDF actor: {}", e);
        }
    }
}

pub(crate) struct UdfState {
    expr: BoundExpr,
    worker_idx: usize,
    #[cfg(feature = "python")]
    udf_handle: UdfHandle,
}

pub(crate) struct UdfOperator {
    params: Arc<UdfParams>,
    expr: BoundExpr,
    worker_count: AtomicUsize,
    concurrency: usize,
    memory_request: u64,
    input_schema: SchemaRef,
}

impl UdfOperator {
    pub fn try_new(
        expr: BoundExpr,
        udf_properties: UDFProperties,
        passthrough_columns: Vec<BoundExpr>,
        output_schema: &SchemaRef,
        input_schema: &SchemaRef,
    ) -> DaftResult<Self> {
        // Determine optimal parallelism
        let resource_request = udf_properties.resource_request.as_ref();
        let max_concurrency =
            Self::get_optimal_allocation(udf_properties.name.as_str(), resource_request)?;
        // If parallelism is already specified, use that
        let concurrency = udf_properties
            .concurrency
            .map(|c| c.get())
            .unwrap_or(max_concurrency);

        let memory_request = resource_request
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);

        let (expr, required_cols) = remap_used_cols(expr);

        Ok(Self {
            expr,
            params: Arc::new(UdfParams {
                udf_properties,
                passthrough_columns,
                output_schema: output_schema.clone(),
                required_cols,
            }),
            worker_count: AtomicUsize::new(0),
            concurrency,
            memory_request,
            input_schema: input_schema.clone(),
        })
    }

    // This function is used to determine the optimal allocation of concurrency and expression parallelism
    fn get_optimal_allocation(
        full_name: &str,
        resource_request: Option<&ResourceRequest>,
    ) -> DaftResult<usize> {
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
                        "`{full_name}` requested {requested_num_cpus} CPUs but found only {num_cpus} available"
                    )))
                } else {
                    Ok((num_cpus as f64 / requested_num_cpus).clamp(1.0, num_cpus as f64) as usize)
                }
            }
            _ => Ok(num_cpus),
        }?;

        Ok(available_cpus)
    }
}

impl IntermediateOperator for UdfOperator {
    type State = UdfState;
    type BatchingStrategy = DynBatchingStrategy;

    #[instrument(skip_all, name = "UdfOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let memory_request = self.memory_request;
        let runtime_stats = task_spawner
            .runtime_stats
            .clone()
            .as_any_arc()
            .downcast::<UdfRuntimeStats>()
            .expect("Expected UdfRuntimeStats in task_spawner.runtime_stats");
        let params = self.params.clone();
        let fut = task_spawner.spawn_with_memory_request(
            memory_request,
            async move {
                #[cfg(feature = "python")]
                {
                    let result = state.udf_handle.eval_input(
                        &mut state.expr,
                        &params,
                        state.worker_idx,
                        input,
                        runtime_stats,
                    )?;
                    let res = IntermediateOperatorResult::NeedMoreInput(Some(result));
                    Ok((state, res))
                }
                #[cfg(not(feature = "python"))]
                {
                    unimplemented!("UdfOperator::execute is not implemented without Python");
                }
            },
            Span::current(),
        );
        fut.into()
    }

    fn name(&self) -> NodeName {
        let udf_name = if let Some((_, udf_name)) = self.params.udf_properties.name.rsplit_once('.')
        {
            udf_name
        } else {
            self.params.udf_properties.name.as_str()
        };

        format!("UDF {}", udf_name).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::UDFProject
    }

    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(UdfRuntimeStats::new(id))
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![
            format!("UDF: {}", self.params.udf_properties.name.as_str()),
            format!("Expr = {}", self.expr),
            format!(
                "Passthrough Columns = [{}]",
                self.params.passthrough_columns.iter().join(", ")
            ),
            format!(
                "Properties = {{ {} }}",
                UDFProperties {
                    concurrency: Some(
                        NonZeroUsize::new(self.concurrency)
                            .expect("UDF concurrency is always >= 1")
                    ),
                    ..self.params.udf_properties.clone()
                }
                .multiline_display(false)
                .join(", ")
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
        let worker_count = self.worker_count.fetch_add(1, Ordering::SeqCst);

        // Check if any inputs or the output are Python-dtype columns
        // Those should by default run on the same thread
        let fields = self.input_schema.fields();
        let is_arrow_dtype = self
            .params
            .required_cols
            .iter()
            .all(|idx| fields[*idx].dtype.is_arrow())
            && self
                .expr
                .inner()
                .to_field(self.input_schema.as_ref())?
                .dtype
                .is_arrow();

        let use_process = self.params.udf_properties.is_actor_pool_udf()
            || self.params.udf_properties.use_process.unwrap_or(false);

        #[cfg(feature = "python")]
        {
            let udf_handle = if use_process {
                if is_arrow_dtype {
                    // Can use process when all types are arrow-serializable
                    UdfHandle::Process(None)
                } else {
                    // Cannot use process with non-arrow types, fall back to thread
                    log::warn!(
                        "UDF `{}` requires a non-arrow-serializable input column. The UDF will run on the same thread as the daft process.",
                        self.params.udf_properties.name
                    );
                    UdfHandle::Thread
                }
            } else {
                UdfHandle::Thread
            };

            Ok(UdfState {
                expr: self.expr.clone(),
                worker_idx: worker_count,
                udf_handle,
            })
        }
        #[cfg(not(feature = "python"))]
        {
            unimplemented!("UdfOperator::make_state is not implemented without Python");
        }
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(self.concurrency)
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.params
            .udf_properties
            .batch_size
            .map(MorselSizeRequirement::Strict)
    }

    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        let cfg = daft_context::get_context().execution_config();

        Ok(if cfg.enable_dynamic_batching {
            match cfg.dynamic_batching_strategy.as_str() {
                "latency_constrained" | "auto" => {
                    // TODO: allow udf to accept a min/max batch size instead of just a strict batch size.
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
                        latency_tolerance: Duration::from_millis(1000), // udf's have high variance so we have a high tolerance
                        step_size_alpha: 16, // step size is small as udfs are expensive
                        correction_delta: 4, // similarly the correction delta is small because the step size is small
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
