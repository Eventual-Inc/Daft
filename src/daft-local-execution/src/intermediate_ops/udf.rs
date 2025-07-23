use std::{ops::RangeInclusive, sync::Arc, time::Duration, vec};

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    expr::{bound_expr::BoundExpr, count_udfs},
    functions::{
        python::{
            get_resource_request, get_udf_names, get_use_process, try_get_batch_size_from_udf,
            try_get_concurrency,
        },
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
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::{ExecutionRuntimeContext, ExecutionTaskSpawner};

const NUM_TEST_ITERATIONS_RANGE: RangeInclusive<usize> = 10..=20;
const GIL_CONTRIBUTION_THRESHOLD: f64 = 0.5;

struct UdfHandle {
    udf_expr: BoundExpr,
    passthrough_columns: Vec<BoundExpr>,
    output_schema: SchemaRef,
    // Optional PyObject handle to external UDF worker.
    // Required for ActorPoolUDFs
    // Optional for stateless UDFs
    //   - Starts as None indicating that the UDF is run in-line with the thread
    //   - If excessive GIL contention is detected, the UDF will be moved to an external worker
    #[cfg(feature = "python")]
    inner: Option<PyObject>,
    // Data used to track GIL contention
    check_gil_contention: bool,
    min_num_test_iterations: usize,
    total_runtime: Duration,
    total_gil_contention: Duration,
    num_batches: usize,
}

impl UdfHandle {
    fn no_handle(
        udf_expr: &BoundExpr,
        passthrough_columns: &[BoundExpr],
        output_schema: SchemaRef,
        check_gil_contention: bool,
        min_num_test_iterations: usize,
    ) -> Self {
        Self {
            udf_expr: udf_expr.clone(),
            passthrough_columns: passthrough_columns.to_vec(),
            output_schema,
            #[cfg(feature = "python")]
            inner: None,
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
            let py_expr = PyExpr::from(self.udf_expr.as_ref().clone());
            let passthrough_exprs = self
                .passthrough_columns
                .iter()
                .map(|expr| PyExpr::from(expr.as_ref().clone()))
                .collect::<Vec<_>>();
            self.inner = Some(Python::with_gil(|py| {
                // create python object
                Ok::<PyObject, PyErr>(
                    py.import(pyo3::intern!(py, "daft.execution.udf"))?
                        .getattr(pyo3::intern!(py, "UdfHandle"))?
                        .call1((py_expr, passthrough_exprs))?
                        .unbind(),
                )
            })?);
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
        inner: &PyObject,
    ) -> DaftResult<Arc<MicroPartition>> {
        Python::with_gil(|py| {
            Ok(inner
                .bind(py)
                .call_method1(
                    pyo3::intern!(py, "eval_input"),
                    (PyMicroPartition::from(input),),
                )?
                .extract::<PyMicroPartition>()?
                .into())
        })
    }

    #[cfg(feature = "python")]
    fn eval_input_inline(&mut self, input: Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        // Extract the udf_expr into a PythonUDF and optional name
        let inner_expr = self.udf_expr.inner();
        let (inner_expr, out_name) = inner_expr.unwrap_alias();
        let Expr::Function {
            func: FunctionExpr::Python(func),
            inputs: input_exprs,
        } = inner_expr.as_ref()
        else {
            return Err(DaftError::InternalError(format!(
                "Expected a Python UDF, got {}",
                inner_expr
            )));
        };

        let input_exprs = BoundExpr::bind_all(input_exprs, input.schema().as_ref())?;

        let input_batches = input.get_tables()?;
        let mut output_batches = Vec::with_capacity(input_batches.len());

        // Iterate over MicroPartition batches
        for batch in input_batches.as_ref() {
            use std::time::Instant;

            // Get the functions inputs
            let func_input = batch.eval_expression_list(input_exprs.as_slice())?;
            // Call the UDF, getting the GIL contention time and total runtime
            let start_time = Instant::now();
            let (mut result, gil_contention_time) = func.call_udf(func_input.columns())?;
            let end_time = Instant::now();
            let total_runtime = end_time - start_time;

            // Rename if necessary
            if let Some(out_name) = out_name.as_ref() {
                result = result.rename(out_name);
            }

            // Update the state
            self.total_runtime += total_runtime;
            self.total_gil_contention += gil_contention_time;
            self.num_batches += 1;

            let passthrough_input =
                batch.eval_expression_list(self.passthrough_columns.as_slice())?;
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
            self.output_schema.clone(),
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
            if let Some(inner) = &self.inner {
                self.eval_input_with_handle(input, inner)
            } else {
                self.eval_input_inline(input)
            }
        }
    }

    fn teardown(&self) -> DaftResult<()> {
        #[cfg(feature = "python")]
        {
            let Some(inner) = &self.inner else {
                return Ok(());
            };

            Python::with_gil(|py| {
                inner.bind(py).call_method0(pyo3::intern!(py, "teardown"))?;
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
struct UdfState {
    pub udf_handle: UdfHandle,
}

impl IntermediateOpState for UdfState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct UdfOperator {
    project: BoundExpr,
    passthrough_columns: Vec<BoundExpr>,
    output_schema: SchemaRef,
    is_actor_pool_udf: bool,
    concurrency: usize,
    batch_size: Option<usize>,
    memory_request: u64,
    use_process: Option<bool>,
}

impl UdfOperator {
    pub fn try_new(
        project: BoundExpr,
        passthrough_columns: Vec<BoundExpr>,
        output_schema: &SchemaRef,
    ) -> DaftResult<Self> {
        let project_unbound = project.inner().clone();

        // count_udfs counts both actor pool and stateless udfs
        let num_udfs = count_udfs(&[project_unbound.clone()]);
        assert_eq!(num_udfs, 1, "Expected only one udf in an udf project");

        // Determine if its an ActorPoolUDF or not
        let exp_concurrency = try_get_concurrency(&project_unbound);
        let is_actor_pool_udf = exp_concurrency.is_some();

        let use_process = get_use_process(&project_unbound)?;

        let full_name = get_udf_names(&project_unbound);
        let resource_request = get_resource_request(&[project_unbound.clone()]);

        // Determine optimal parallelism
        let max_concurrency = Self::get_optimal_allocation(&full_name, resource_request.as_ref())?;
        // If parallelism is already specified, use that
        let concurrency = exp_concurrency.unwrap_or(max_concurrency);
        let batch_size = try_get_batch_size_from_udf(&project_unbound)?;
        let memory_request = resource_request
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);

        Ok(Self {
            project,
            passthrough_columns,
            output_schema: output_schema.clone(),
            is_actor_pool_udf,
            concurrency,
            batch_size,
            memory_request,
            use_process,
        })
    }

    // This function is used to determine the optimal allocation of concurrency and expression parallelism
    fn get_optimal_allocation(
        full_name: &[String],
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
                        "{}: Requested {} CPUs but only found {} available",
                        full_name.join(", "),
                        requested_num_cpus,
                        num_cpus
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
    #[instrument(skip_all, name = "UdfOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let memory_request = self.memory_request;
        let fut = task_spawner.spawn_with_memory_request(
            memory_request,
            async move {
                let udf_state = state
                    .as_any_mut()
                    .downcast_mut::<UdfState>()
                    .expect("UdfState");
                let res = udf_state
                    .udf_handle
                    .eval_input(input)
                    .map(|result| IntermediateOperatorResult::NeedMoreInput(Some(result)))?;
                Ok((state, res))
            },
            Span::current(),
        );
        fut.into()
    }

    fn name(&self) -> &'static str {
        "UdfOperator"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("UDF Executor:".to_string());
        res.push(format!(
            "UDF {} = {}",
            get_udf_names(self.project.inner()).first().unwrap(),
            self.project
        ));
        res.push(format!(
            "Passthrough Columns = [{}]",
            self.passthrough_columns.iter().join(", ")
        ));
        res.push(format!("Concurrency = {:?}", self.concurrency));
        if let Some(resource_request) = get_resource_request(&[self.project.clone()]) {
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

    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        let mut rng = rand::thread_rng();

        let mut udf_handle = UdfHandle::no_handle(
            &self.project,
            &self.passthrough_columns,
            self.output_schema.clone(),
            matches!(self.use_process, Some(false)),
            rng.gen_range(NUM_TEST_ITERATIONS_RANGE),
        );
        if self.is_actor_pool_udf || self.use_process.unwrap_or(false) {
            udf_handle.create_handle()?;
        }

        Ok(Box::new(UdfState { udf_handle }))
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(self.concurrency)
    }

    fn morsel_size_range(&self, runtime_handle: &ExecutionRuntimeContext) -> (usize, usize) {
        if let Some(batch_size) = self.batch_size {
            (batch_size, batch_size)
        } else {
            (0, runtime_handle.default_morsel_size())
        }
    }
}

// TODO: Add test for UDFs, can't create a fake one for testing
