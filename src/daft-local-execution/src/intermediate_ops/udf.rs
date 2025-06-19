use std::{sync::Arc, vec};

use common_error::DaftResult;
use common_runtime::get_compute_pool_num_threads;
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    expr::{bound_expr::BoundExpr, count_udfs},
    functions::python::{
        get_resource_request, get_udf_names, try_get_batch_size_from_udf, try_get_concurrency,
    },
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
    #[cfg(feature = "python")]
    inner: PyObject,
}

impl UdfHandle {
    fn try_new(projection: &[BoundExpr]) -> DaftResult<Self> {
        #[cfg(feature = "python")]
        {
            let handle = Python::with_gil(|py| {
                // create python object
                Ok::<PyObject, PyErr>(
                    py.import(pyo3::intern!(py, "daft.execution.udf"))?
                        .getattr(pyo3::intern!(py, "UdfHandle"))?
                        .call1((projection
                            .iter()
                            .map(|expr| PyExpr::from(expr.as_ref().clone()))
                            .collect::<Vec<_>>(),))?
                        .unbind(),
                )
            })?;

            Ok(Self { inner: handle })
        }

        #[cfg(not(feature = "python"))]
        {
            Ok(Self {})
        }
    }

    fn eval_input(&self, input: Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        #[cfg(feature = "python")]
        {
            Python::with_gil(|py| {
                Ok(self
                    .inner
                    .bind(py)
                    .call_method1(
                        pyo3::intern!(py, "eval_input"),
                        (PyMicroPartition::from(input),),
                    )?
                    .extract::<PyMicroPartition>()?
                    .into())
            })
        }

        #[cfg(not(feature = "python"))]
        {
            panic!("Cannot evaluate a UDF without compiling for Python");
        }
    }

    fn teardown(&self) -> DaftResult<()> {
        #[cfg(feature = "python")]
        {
            Python::with_gil(|py| {
                self.inner
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
struct UdfState {
    pub udf_handle: UdfHandle,
}

impl IntermediateOpState for UdfState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct UdfOperator {
    projection: Vec<BoundExpr>,
    concurrency: Option<usize>,
    batch_size: Option<usize>,
    memory_request: u64,
}

impl UdfOperator {
    pub fn try_new(projection: Vec<BoundExpr>) -> DaftResult<Self> {
        let projection_unbound = projection
            .iter()
            .map(|expr| expr.inner().clone())
            .collect::<Vec<_>>();

        // count_udfs counts both actor pool and stateless udfs
        let num_udfs = count_udfs(&projection_unbound);

        assert_eq!(num_udfs, 1, "Expected only one udf in an udf projection");

        let concurrency = try_get_concurrency(&projection_unbound);
        let batch_size = try_get_batch_size_from_udf(&projection_unbound)?;

        let memory_request = get_resource_request(&projection)
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);
        Ok(Self {
            projection,
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
        res.push(format!(
            "Projection = [{}]",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "UDFs = [{}]",
            self.projection
                .iter()
                .flat_map(|expr| get_udf_names(expr.inner()))
                .join(", ")
        ));
        res.push(format!("Concurrency = {:?}", self.concurrency));
        if let Some(resource_request) = get_resource_request(&self.projection) {
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
        Ok(Box::new(UdfState {
            udf_handle: UdfHandle::try_new(&self.projection)?,
        }))
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
