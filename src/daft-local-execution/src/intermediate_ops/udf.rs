use std::{sync::Arc, time::Duration, vec};

use common_error::{DaftError, DaftResult};
use common_runtime::get_compute_pool_num_threads;
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    expr::{bound_expr::BoundExpr, count_udfs},
    functions::{
        python::{
            get_resource_request, get_udf_names, try_get_batch_size_from_udf, try_get_concurrency,
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
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::{ExecutionRuntimeContext, ExecutionTaskSpawner};

struct UdfHandle {
    udf_expr: BoundExpr,
    passthrough_columns: Vec<BoundExpr>,
    // Optional PyObject handle to external UDF worker.
    // Required for ActorPoolUDFs
    // Optional for stateless UDFs
    //   - Starts as None indicating that the UDF is run in-line with the thread
    //   - If excessive GIL contention is detected, the UDF will be moved to an external worker
    #[cfg(feature = "python")]
    inner: Option<PyObject>,
    // Data used to track GIL contention
    total_runtime: Duration,
    total_gil_contention: Duration,
    num_batches: usize,
}

impl UdfHandle {
    fn no_handle(udf_expr: &BoundExpr, passthrough_columns: &[BoundExpr]) -> Self {
        Self {
            udf_expr: udf_expr.clone(),
            passthrough_columns: passthrough_columns.to_vec(),
            #[cfg(feature = "python")]
            inner: None,
            total_runtime: Duration::from_secs(0),
            total_gil_contention: Duration::from_secs(0),
            num_batches: 0,
        }
    }

    fn create_handle(&mut self) -> DaftResult<()> {
        #[cfg(feature = "python")]
        {
            let py_expr = PyExpr::from(self.udf_expr.as_ref().clone());
            self.inner = Some(Python::with_gil(|py| {
                // create python object
                Ok::<PyObject, PyErr>(
                    py.import(pyo3::intern!(py, "daft.execution.udf"))?
                        .getattr(pyo3::intern!(py, "UdfHandle"))?
                        .call1((py_expr,))?
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

        let mut out_batches = vec![];

        // Iterate over MicroPartition batches
        for batch in input.get_tables()?.as_ref() {
            // Get the functions inputs
            let func_input = batch.eval_expression_list(input_exprs.as_slice())?;
            // Call the UDF, getting the GIL contention time
            let (mut result, gil_contention_time) = func.call_udf(func_input.columns())?;
            if let Some(out_name) = out_name.as_ref() {
                result = result.rename(out_name);
            }

            // Update the state
            self.total_runtime += gil_contention_time;
            self.total_gil_contention += gil_contention_time;
            self.num_batches += 1;

            let passthrough_input =
                batch.eval_expression_list(self.passthrough_columns.as_slice())?;
            let series = passthrough_input.append_column(result)?;
            out_batches.push(series);
        }

        // Switch to external process if we hit the GIL contention threshold
        if self.num_batches > 10
            && (self.total_gil_contention.as_secs_f64() / self.total_runtime.as_secs_f64()) > 0.2
        {
            self.create_handle()?;
        }

        let out_schema = out_batches[0].schema.clone();
        Ok(Arc::new(MicroPartition::new_loaded(
            out_schema,
            Arc::new(out_batches),
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
    concurrency: Option<usize>,
    batch_size: Option<usize>,
    memory_request: u64,
}

impl UdfOperator {
    pub fn try_new(project: BoundExpr, passthrough_columns: Vec<BoundExpr>) -> DaftResult<Self> {
        let project_unbound = project.inner().clone();

        // count_udfs counts both actor pool and stateless udfs
        let num_udfs = count_udfs(&[project_unbound.clone()]);
        assert_eq!(num_udfs, 1, "Expected only one udf in an udf project");

        let concurrency = try_get_concurrency(&[project_unbound.clone()]);
        let batch_size = try_get_batch_size_from_udf(&[project_unbound.clone()])?;
        let memory_request = get_resource_request(&[project_unbound])
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);
        Ok(Self {
            project,
            passthrough_columns,
            concurrency,
            batch_size,
            memory_request,
        })
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
        res.push(format!("UDF = {}", self.project));
        res.push(format!(
            "Passthrough = [{}]",
            self.passthrough_columns
                .iter()
                .flat_map(|expr| get_udf_names(expr.inner()))
                .join(", ")
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
        // TODO: Pass relevant CUDA_VISIBLE_DEVICES to the udf
        let mut udf_handle = UdfHandle::no_handle(&self.project, &self.passthrough_columns);
        if self.concurrency.is_some() {
            udf_handle.create_handle()?;
        }

        Ok(Box::new(UdfState { udf_handle }))
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(self
            .concurrency
            .unwrap_or_else(get_compute_pool_num_threads))
    }

    fn morsel_size_range(&self, runtime_handle: &ExecutionRuntimeContext) -> (usize, usize) {
        if let Some(batch_size) = self.batch_size {
            (batch_size, batch_size)
        } else {
            (0, runtime_handle.default_morsel_size())
        }
    }
}
