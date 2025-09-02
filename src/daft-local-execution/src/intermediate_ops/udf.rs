use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    vec,
};

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
#[cfg(feature = "python")]
use daft_core::series::Series;
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    common_treenode::{TreeNode, TreeNodeRecursion},
    expr::bound_expr::BoundExpr,
    functions::python::UDFProperties,
    Column, Expr, ExprRef,
};
use daft_micropartition::MicroPartition;
#[cfg(feature = "python")]
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{
    ops::NodeType,
    pipeline::{MorselSizeRequirement, NodeName},
    ExecutionTaskSpawner,
};

fn get_required_columns(expr: &BoundExpr) -> Vec<BoundExpr> {
    let mut cols = vec![];
    expr.inner()
        .apply(&mut |expr: &ExprRef| {
            if matches!(expr.as_ref(), Expr::Column(Column::Bound(..))) {
                cols.push(BoundExpr::new_unchecked(expr.clone()));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("Error occurred when visiting for required columns");
    cols
}

/// Common parameters for UDF handle and operator
struct UdfParams {
    expr: BoundExpr,
    udf_properties: UDFProperties,
    passthrough_columns: Vec<BoundExpr>,
    output_schema: SchemaRef,
    required_cols: Vec<BoundExpr>,
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
    handle: Option<PyObject>,
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
            let udf_name = self.params.udf_properties.name.clone();

            let handle = Python::with_gil(|py| {
                // create python object
                Ok::<PyObject, PyErr>(
                    py.import(pyo3::intern!(py, "daft.execution.udf"))?
                        .getattr(pyo3::intern!(py, "UdfHandle"))?
                        .call1((udf_name, py_expr))?
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
    fn eval_input_with_handle(&self, input: RecordBatch, handle: &PyObject) -> DaftResult<Series> {
        use daft_recordbatch::python::PyRecordBatch;

        use crate::STDOUT;

        let (rb, outs) = Python::with_gil(|py| {
            handle
                .bind(py)
                .call_method1(
                    pyo3::intern!(py, "eval_input"),
                    (PyRecordBatch::from(input),),
                )?
                .extract::<(PyRecordBatch, Vec<String>)>()
        })?;

        let label = format!(
            "[`{}` Worker #{}]",
            self.params.udf_properties.name, self.worker_idx
        );
        for line in outs {
            STDOUT.print(&label, &line);
        }

        let rb: RecordBatch = rb.into();
        Ok(rb.get_column(0).clone())
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
    fn eval_input(&mut self, input: Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        let input_batches = input.get_tables()?;
        let mut output_batches = Vec::with_capacity(input_batches.len());

        for batch in input_batches.as_ref() {
            // Prepare inputs
            eprintln!("batch: \n{}", batch);
            let func_input = batch.eval_expression_list(self.params.required_cols.as_slice())?;
            eprintln!("func_input: \n{}", func_input);

            // Call the UDF
            let mut result = if let Some(handle) = &self.handle {
                self.eval_input_with_handle(func_input, handle)
            } else {
                self.eval_input_inline(func_input)
            }?;
            // If result.len() == 1 (because it was a 0-column UDF), broadcast to right size
            if result.len() == 1 {
                result = result.broadcast(batch.num_rows())?;
            }
            eprintln!("result: \n{}", result);

            // Append result to passthrough
            let passthrough_input =
                batch.eval_expression_list(self.params.passthrough_columns.as_slice())?;
            let output_batch = passthrough_input.append_column(result)?;
            output_batches.push(output_batch);
        }

        Ok(Arc::new(MicroPartition::new_loaded(
            self.params.output_schema.clone(),
            Arc::new(output_batches),
            None,
        )))
    }

    fn teardown(&self) -> DaftResult<()> {
        #[cfg(feature = "python")]
        {
            let Some(handle) = &self.handle else {
                return Ok(());
            };

            Python::with_gil(|py| {
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
        let concurrency = udf_properties.concurrency.unwrap_or(max_concurrency);

        let memory_request = resource_request
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);

        let required_cols = get_required_columns(&expr);

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
        let fut = task_spawner.spawn_with_memory_request(
            memory_request,
            async move {
                let res = state
                    .udf_handle
                    .eval_input(input)
                    .map(|result| IntermediateOperatorResult::NeedMoreInput(Some(result)))?;
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
        let is_arrow_dtype = self
            .input_schema
            .fields()
            .iter()
            .all(|f| f.dtype.is_arrow())
            && self
                .params
                .expr
                .inner()
                .to_field(self.input_schema.as_ref())?
                .dtype
                .is_arrow();

        let mut udf_handle =
            UdfHandle::no_handle(self.params.clone(), self.params.expr.clone(), worker_count);

        if self.params.udf_properties.is_actor_pool_udf()
            || self
                .params
                .udf_properties
                .use_process
                .unwrap_or(is_arrow_dtype)
        {
            udf_handle.create_handle()?;
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
