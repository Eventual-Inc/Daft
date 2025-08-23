use std::{
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
    vec,
};

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    expr::{bound_expr::BoundExpr, count_udfs},
    functions::{
        python::{get_udf_properties, UDFProperties},
        FunctionExpr,
    },
    Expr,
};
#[cfg(feature = "python")]
use daft_micropartition::python::PyMicroPartition;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use rand::Rng;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{
    ops::NodeType,
    pipeline::{MorselSizeRequirement, NodeName},
    ExecutionTaskSpawner,
};

const NUM_TEST_ITERATIONS_RANGE: RangeInclusive<usize> = 10..=20;
const GIL_CONTRIBUTION_THRESHOLD: f64 = 0.5;

/// Common parameters for UDF handle and operator
struct UdfParams {
    udf_expr: BoundExpr,
    passthrough_columns: Vec<BoundExpr>,
    udf_name: String,
    output_schema: SchemaRef,
}

pub(crate) struct UdfHandle {
    params: Arc<UdfParams>,
    worker_idx: usize,
    // Optional PyObject handle to external UDF worker.
    // Required for ActorPoolUDFs
    // Optional for stateless UDFs
    //   - Starts as None indicating that the UDF is run in-line with the thread
    //   - If excessive GIL contention is detected, the UDF will be moved to an external worker
    // Second bool indicates if the UDF was initialized
    #[cfg(feature = "python")]
    handle: Option<PyObject>,
    // Data used to track GIL contention
    check_gil_contention: bool,
    min_num_test_iterations: usize,
    total_runtime: Duration,
    total_gil_contention: Duration,
    num_batches: usize,
}

impl UdfHandle {
    fn no_handle(
        params: Arc<UdfParams>,
        worker_idx: usize,
        check_gil_contention: bool,
        min_num_test_iterations: usize,
    ) -> Self {
        Self {
            params,
            worker_idx,
            #[cfg(feature = "python")]
            handle: None,
            check_gil_contention,
            min_num_test_iterations,
            total_runtime: Duration::from_secs(0),
            total_gil_contention: Duration::from_secs(0),
            num_batches: 0,
        }
    }

    fn create_handle(&mut self) -> DaftResult<()> {
        #[cfg(feature = "python")]
        {
            let py_expr = PyExpr::from(self.params.udf_expr.as_ref().clone());
            let passthrough_exprs = self
                .params
                .passthrough_columns
                .iter()
                .map(|expr| PyExpr::from(expr.as_ref().clone()))
                .collect::<Vec<_>>();

            let handle = Python::with_gil(|py| {
                // create python object
                Ok::<PyObject, PyErr>(
                    py.import(pyo3::intern!(py, "daft.execution.udf"))?
                        .getattr(pyo3::intern!(py, "UdfHandle"))?
                        .call1((py_expr, passthrough_exprs))?
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
        input: Arc<MicroPartition>,
        handle: &PyObject,
    ) -> DaftResult<Arc<MicroPartition>> {
        use crate::STDOUT;

        let (micropartition, outs) = Python::with_gil(|py| {
            handle
                .bind(py)
                .call_method1(
                    pyo3::intern!(py, "eval_input"),
                    (PyMicroPartition::from(input),),
                )?
                .extract::<(PyMicroPartition, Vec<String>)>()
        })?;

        let label = format!("[`{}` Worker #{}]", self.params.udf_name, self.worker_idx);
        for line in outs {
            STDOUT.print(&label, &line);
        }
        Ok(micropartition.into())
    }

    #[cfg(feature = "python")]
    fn eval_input_inline(&mut self, input: Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        // Extract the udf_expr into a PythonUDF and optional name

        use daft_dsl::{
            functions::{python::LegacyPythonUDF, scalar::ScalarFn},
            python_udf::PyScalarFn,
        };
        let inner_expr = self.params.udf_expr.inner();
        let (inner_expr, out_name) = inner_expr.unwrap_alias();

        enum UdfImpl<'a> {
            Legacy(&'a LegacyPythonUDF),
            PyScalarFn(&'a PyScalarFn),
        }

        let (func, input_exprs) = match inner_expr.as_ref() {
            Expr::Function {
                func: FunctionExpr::Python(func),
                inputs: input_exprs,
            } => (UdfImpl::Legacy(func), input_exprs.clone()),
            Expr::ScalarFn(ScalarFn::Python(f)) => (UdfImpl::PyScalarFn(f), f.args()),
            _ => {
                return Err(DaftError::InternalError(format!(
                    "Expected a Python UDF, got {}",
                    inner_expr
                )))
            }
        };

        let input_exprs = BoundExpr::bind_all(&input_exprs, input.schema().as_ref())?;

        let input_batches = input.get_tables()?;
        let mut output_batches = Vec::with_capacity(input_batches.len());

        for batch in input_batches.as_ref() {
            use std::time::Instant;

            // Get the functions inputs
            let func_input = batch.eval_expression_list(input_exprs.as_slice())?;
            // Call the UDF, getting the GIL contention time and total runtime
            let start_time = Instant::now();
            let (mut result, gil_contention_time) = match &func {
                UdfImpl::Legacy(f) => f.call_udf(func_input.columns())?,
                UdfImpl::PyScalarFn(f) => f.call(func_input.columns())?,
            };
            let total_runtime = start_time.elapsed();

            // Rename if necessary
            if let Some(out_name) = out_name.as_ref() {
                result = result.rename(out_name);
            }

            // Update the state
            self.total_runtime += total_runtime;
            self.total_gil_contention += gil_contention_time;
            self.num_batches += 1;

            let passthrough_input =
                batch.eval_expression_list(self.params.passthrough_columns.as_slice())?;
            let series = passthrough_input.append_column(result)?;
            output_batches.push(series);
        }

        // Switch to external process if we hit the GIL contention threshold
        if self.check_gil_contention
            && self.num_batches > self.min_num_test_iterations
            && (self.total_gil_contention.as_secs_f64() / self.total_runtime.as_secs_f64())
                > GIL_CONTRIBUTION_THRESHOLD
        {
            self.create_handle()?;
        }

        Ok(Arc::new(MicroPartition::new_loaded(
            self.params.output_schema.clone(),
            Arc::new(output_batches),
            None,
        )))
    }

    fn eval_input(&mut self, input: Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        #[cfg(not(feature = "python"))]
        {
            panic!("Cannot evaluate a UDF without compiling for Python");
        }

        #[cfg(feature = "python")]
        {
            if let Some(handle) = &self.handle {
                self.eval_input_with_handle(input, handle)
            } else {
                self.eval_input_inline(input)
            }
        }
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
///
/// TODO: Implement a work-stealing dispatcher in the executor to improve pipelining.
pub(crate) struct UdfState {
    udf_handle: UdfHandle,
}

pub(crate) struct UdfOperator {
    params: Arc<UdfParams>,
    worker_count: AtomicUsize,
    udf_properties: UDFProperties,
    concurrency: usize,
    memory_request: u64,
}

impl UdfOperator {
    pub fn try_new(
        project: BoundExpr,
        passthrough_columns: Vec<BoundExpr>,
        output_schema: &SchemaRef,
    ) -> DaftResult<Self> {
        let project_unbound = project.inner().clone();

        let num_udfs = count_udfs(&[project_unbound.clone()]);
        assert_eq!(num_udfs, 1, "Expected only one udf in an udf project");
        let udf_properties = get_udf_properties(&project_unbound);

        // Determine optimal parallelism
        let resource_request = &udf_properties.resource_request;
        let max_concurrency =
            Self::get_optimal_allocation(&udf_properties.name, resource_request.as_ref())?;
        // If parallelism is already specified, use that
        let concurrency = udf_properties.concurrency.unwrap_or(max_concurrency);

        let memory_request = resource_request
            .as_ref()
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);

        Ok(Self {
            params: Arc::new(UdfParams {
                udf_expr: project,
                passthrough_columns,
                udf_name: udf_properties.name.clone(),
                output_schema: output_schema.clone(),
            }),
            worker_count: AtomicUsize::new(0),
            udf_properties,
            concurrency,
            memory_request,
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
        let udf_name = if let Some((_, udf_name)) = self.params.udf_name.rsplit_once('.') {
            udf_name
        } else {
            &self.params.udf_name
        };

        format!("UDF {}", udf_name).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::UDFProject
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("UDF Executor:".to_string());
        res.push(format!(
            "UDF {} = {}",
            self.params.udf_name, self.params.udf_expr
        ));
        res.push(format!(
            "Passthrough Columns = [{}]",
            self.params.passthrough_columns.iter().join(", ")
        ));
        res.push(format!("Concurrency = {}", self.concurrency));
        if let Some(resource_request) = &self.udf_properties.resource_request {
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
        let mut rng = rand::thread_rng();

        let mut udf_handle = UdfHandle::no_handle(
            self.params.clone(),
            worker_count,
            matches!(self.udf_properties.use_process, Some(false)),
            rng.gen_range(NUM_TEST_ITERATIONS_RANGE),
        );

        if self.udf_properties.is_actor_pool_udf()
            || self.udf_properties.use_process.unwrap_or(false)
        {
            udf_handle.create_handle()?;
        }

        Ok(UdfState { udf_handle })
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(self.concurrency)
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.udf_properties
            .batch_size
            .map(MorselSizeRequirement::Strict)
    }
}

// TODO: Add test for UDFs, can't create a fake one for testing
