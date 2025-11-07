use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
    vec,
};

use common_error::{DaftError, DaftResult};
use common_metrics::{Stat, StatSnapshotSend, ops::NodeType, smallvec};
use common_resource_request::ResourceRequest;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
#[cfg(feature = "python")]
use daft_core::series::Series;
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    Column, Expr, ExprRef,
    common_treenode::{Transformed, TreeNode},
    expr::{BoundColumn, bound_expr::BoundExpr},
    functions::python::UDFProperties,
};
use daft_micropartition::MicroPartition;
#[cfg(feature = "python")]
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::{Py, prelude::*, types::PyDict};
use tracing::{Span, instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{MorselSizeRequirement, NodeName},
    runtime_stats::{CPU_US_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, RuntimeStats},
    udf_metrics,
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

#[derive(Default)]
struct UdfRuntimeStats {
    cpu_us: AtomicU64,
    rows_in: AtomicU64,
    rows_out: AtomicU64,
    counters: Mutex<HashMap<Arc<str>, u64>>,
    gauges: Mutex<HashMap<Arc<str>, f64>>,
}

impl RuntimeStats for UdfRuntimeStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshotSend {
        let mut entries = smallvec![];

        entries.push((
            Arc::<str>::from(CPU_US_KEY),
            Stat::Duration(Duration::from_micros(self.cpu_us.load(ordering))),
        ));
        entries.push((
            Arc::<str>::from(ROWS_IN_KEY),
            Stat::Count(self.rows_in.load(ordering)),
        ));
        entries.push((
            Arc::<str>::from(ROWS_OUT_KEY),
            Stat::Count(self.rows_out.load(ordering)),
        ));

        {
            let counters = self.counters.lock().unwrap();
            for (name, value) in counters.iter() {
                entries.push((name.clone(), Stat::Count(*value)));
            }
        }

        {
            let gauges = self.gauges.lock().unwrap();
            for (name, value) in gauges.iter() {
                entries.push((name.clone(), Stat::Float(*value)));
            }
        }

        StatSnapshotSend(entries)
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.fetch_add(cpu_us, Ordering::Relaxed);
    }
}

impl UdfRuntimeStats {
    fn apply_updates(&self, mut updates: UdfMetricUpdates) {
        if updates.is_empty() {
            return;
        }

        if !updates.counters.is_empty() {
            let mut counters = self.counters.lock().unwrap();
            for (name, value) in updates.counters.drain() {
                let key = Arc::<str>::from(name);
                *counters.entry(key).or_default() += value;
            }
        }

        if !updates.gauges.is_empty() {
            let mut gauges = self.gauges.lock().unwrap();
            for (name, value) in updates.gauges.drain() {
                gauges.insert(Arc::<str>::from(name), value);
            }
        }
    }
}

#[derive(Default)]
struct UdfMetricUpdates {
    counters: HashMap<String, u64>,
    gauges: HashMap<String, f64>,
}

impl UdfMetricUpdates {
    fn is_empty(&self) -> bool {
        self.counters.is_empty() && self.gauges.is_empty()
    }

    fn absorb(&mut self, mut other: UdfMetricUpdates) {
        for (name, value) in other.counters.drain() {
            *self.counters.entry(name).or_default() += value;
        }

        for (name, value) in other.gauges.drain() {
            self.gauges.insert(name, value);
        }
    }
}

fn take_python_udf_metrics(udf_id: &str) -> DaftResult<UdfMetricUpdates> {
    let snapshot = udf_metrics::take_snapshot_for_udf(udf_id);
    let mut updates = UdfMetricUpdates::default();
    updates.counters = snapshot.counters;
    updates.gauges = snapshot.gauges;
    Ok(updates)
}

#[cfg(feature = "python")]
fn metric_updates_from_py(metrics: &Bound<'_, PyDict>) -> PyResult<UdfMetricUpdates> {
    let mut updates = UdfMetricUpdates::default();

    if let Some(counters_any) = metrics.get_item("counters")? {
        if let Ok(counters) = counters_any.cast::<PyDict>() {
            for (name, value) in counters.iter() {
                let metric_name: String = name.extract()?;
                let amount: u64 = value.extract()?;
                updates.counters.insert(metric_name, amount);
            }
        }
    }

    if let Some(gauges_any) = metrics.get_item("gauges")? {
        if let Ok(gauges) = gauges_any.cast::<PyDict>() {
            for (name, value) in gauges.iter() {
                let metric_name: String = name.extract()?;
                let gauge_value: f64 = value.extract()?;
                updates.gauges.insert(metric_name, gauge_value);
            }
        }
    }

    Ok(updates)
}

/// Common parameters for UDF handle and operator
struct UdfParams {
    expr: BoundExpr,
    udf_properties: UDFProperties,
    passthrough_columns: Vec<BoundExpr>,
    output_schema: SchemaRef,
    required_cols: Vec<usize>,
}

pub(crate) struct UdfHandle {
    params: Arc<UdfParams>,
    udf_expr: BoundExpr,
    worker_idx: usize,
    // Optional PyObject handle to external UDF worker.
    // Required for ActorPoolUDFs
    // Optional for stateless UDFs
    //   - Starts as None indicating that the UDF is run in-line with the thread
    //   - If excessive GIL contention is detected, the UDF will be moved to an external worker
    // Second bool indicates if the UDF was initialized
    #[cfg(feature = "python")]
    handle: Option<Py<PyAny>>,
}

impl UdfHandle {
    fn no_handle(params: Arc<UdfParams>, udf_expr: BoundExpr, worker_idx: usize) -> Self {
        Self {
            params,
            udf_expr,
            worker_idx,
            #[cfg(feature = "python")]
            handle: None,
        }
    }

    fn create_handle(&mut self) -> DaftResult<()> {
        #[cfg(feature = "python")]
        {
            let py_expr = PyExpr::from(self.udf_expr.inner().clone());

            let handle = Python::attach(|py| {
                // create python object
                Ok::<pyo3::Py<pyo3::PyAny>, PyErr>(
                    py.import(pyo3::intern!(py, "daft.execution.udf"))?
                        .getattr(pyo3::intern!(py, "UdfHandle"))?
                        .call1((py_expr, self.params.udf_properties.name.as_str()))?
                        .unbind(),
                )
            })?;

            self.handle = Some(handle);
        }

        #[cfg(not(feature = "python"))]
        {
            panic!("Cannot create a UDF handle without compiling for Python");
        }

        Ok(())
    }

    #[cfg(feature = "python")]
    fn eval_input_with_handle(
        &self,
        input: RecordBatch,
        handle: &pyo3::Py<pyo3::PyAny>,
    ) -> DaftResult<(Series, UdfMetricUpdates)> {
        use daft_recordbatch::python::PyRecordBatch;

        use crate::STDOUT;

        let (result, stdout_lines, metrics_payload) = Python::attach(|py| {
            handle
                .bind(py)
                .call_method1(
                    pyo3::intern!(py, "eval_input"),
                    (PyRecordBatch::from(input),),
                )?
                .extract::<(PyRecordBatch, Vec<String>, Py<PyAny>)>()
        })?;

        let label = format!(
            "[`{}` Worker #{}]",
            self.params.udf_properties.name, self.worker_idx
        );
        for line in stdout_lines {
            STDOUT.print(&label, &line);
        }

        let metrics = Python::attach(|py| -> PyResult<UdfMetricUpdates> {
            let bound = metrics_payload.bind(py);
            let dict = bound.cast::<PyDict>()?;
            metric_updates_from_py(&dict)
        })
        .map_err(DaftError::from)?;

        let result: RecordBatch = result.into();
        debug_assert!(
            result.num_columns() == 1,
            "UDF should return a single column"
        );
        Ok((result.get_column(0).clone(), metrics))
    }

    #[cfg(feature = "python")]
    fn eval_input_inline(&mut self, func_input: RecordBatch) -> DaftResult<Series> {
        use daft_dsl::functions::python::initialize_udfs;

        // Only actually initialized the first time
        self.udf_expr = BoundExpr::new_unchecked(initialize_udfs(self.udf_expr.inner().clone())?);
        func_input.eval_expression(&self.udf_expr)
    }

    #[cfg(not(feature = "python"))]
    fn eval_input(&mut self, input: Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        panic!("Cannot evaluate a UDF without compiling for Python");
    }

    #[cfg(feature = "python")]
    fn eval_input(
        &mut self,
        input: Arc<MicroPartition>,
    ) -> DaftResult<(Arc<MicroPartition>, UdfMetricUpdates)> {
        let input_batches = input.get_tables()?;
        let mut output_batches = Vec::with_capacity(input_batches.len());
        let mut aggregated_metrics = UdfMetricUpdates::default();

        let using_actor = self.handle.is_some();
        let inline_udf_id: Option<String> = if !using_actor {
            Some(self.params.udf_properties.name.clone())
        } else {
            None
        };

        for batch in input_batches.as_ref() {
            // Prepare inputs
            let func_input = batch.get_columns(self.params.required_cols.as_slice());

            // Call the UDF
            let (mut result, batch_metrics) = if let Some(handle) = self.handle.as_ref() {
                let (series, metrics) = self.eval_input_with_handle(func_input, handle)?;
                (series, Some(metrics))
            } else {
                (self.eval_input_inline(func_input)?, None)
            };

            if let Some(metrics) = batch_metrics {
                aggregated_metrics.absorb(metrics);
            }
            // If result.len() == 1 (because it was a 0-column UDF), broadcast to right size
            if result.len() == 1 {
                result = result.broadcast(batch.num_rows())?;
            }

            // Append result to passthrough
            let passthrough_input =
                batch.eval_expression_list(self.params.passthrough_columns.as_slice())?;
            let output_batch =
                passthrough_input.append_column(self.params.output_schema.clone(), result)?;
            output_batches.push(output_batch);
        }

        let metrics = if let Some(udf_name) = inline_udf_id.as_deref() {
            take_python_udf_metrics(udf_name)?
        } else {
            aggregated_metrics
        };

        Ok((
            Arc::new(MicroPartition::new_loaded(
                self.params.output_schema.clone(),
                Arc::new(output_batches),
                None,
            )),
            metrics,
        ))
    }

    fn teardown(&self) -> DaftResult<()> {
        #[cfg(feature = "python")]
        {
            let Some(handle) = &self.handle else {
                return Ok(());
            };

            Python::attach(|py| {
                handle
                    .bind(py)
                    .call_method0(pyo3::intern!(py, "teardown"))?;
                Ok(())
            })
        }

        #[cfg(not(feature = "python"))]
        {
            Ok(())
        }
    }
}

impl Drop for UdfHandle {
    fn drop(&mut self) {
        let result = self.teardown();

        if let Err(e) = result {
            log::error!("Error tearing down UDF actor: {}", e);
        }
    }
}

/// Each UdfState holds a handle to a single Python process.
/// The concurrency of the Python process pool is thus tied to the concurrency of the operator
/// and the local executor handles task scheduling.
pub(crate) struct UdfState {
    udf_handle: UdfHandle,
}

pub(crate) struct UdfOperator {
    params: Arc<UdfParams>,
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
            params: Arc::new(UdfParams {
                expr,
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

    #[instrument(skip_all, name = "UdfOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let memory_request = self.memory_request;
        let udf_runtime_stats = task_spawner
            .runtime_stats
            .clone()
            .as_any_arc()
            .downcast::<UdfRuntimeStats>()
            .expect("Expected UdfRuntimeStats in task_spawner.runtime_stats");
        let fut = task_spawner.spawn_with_memory_request(
            memory_request,
            async move {
                let (result, metrics) = state.udf_handle.eval_input(input)?;
                if !metrics.is_empty() {
                    udf_runtime_stats.apply_updates(metrics);
                }

                let res = IntermediateOperatorResult::NeedMoreInput(Some(result));
                Ok((state, res))
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

    fn make_runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        Arc::new(UdfRuntimeStats::default())
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![
            format!("UDF: {}", self.params.udf_properties.name.as_str()),
            format!("Expr = {}", self.params.expr),
            format!(
                "Passthrough Columns = [{}]",
                self.params.passthrough_columns.iter().join(", ")
            ),
            format!("Concurrency = {}", self.concurrency),
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
                .params
                .expr
                .inner()
                .to_field(self.input_schema.as_ref())?
                .dtype
                .is_arrow();

        let create_handle = self.params.udf_properties.is_actor_pool_udf()
            || self.params.udf_properties.use_process.unwrap_or(false);

        let mut udf_handle =
            UdfHandle::no_handle(self.params.clone(), self.params.expr.clone(), worker_count);

        if create_handle {
            if is_arrow_dtype {
                udf_handle.create_handle()?;
            } else {
                // Should only warn when concurrency or use_process is set
                log::warn!(
                    "UDF `{}` requires a non-arrow-serializable input column. The UDF will run on the same thread as the daft process.",
                    self.params.udf_properties.name
                );
            }
        }

        Ok(UdfState { udf_handle })
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
}
