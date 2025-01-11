use std::{sync::Arc, vec};

use common_error::DaftResult;
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    count_actor_pool_udfs,
    functions::python::{get_batch_size, get_concurrency, get_resource_request, get_udf_names},
    ExprRef,
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
use crate::{
    dispatcher::{DispatchSpawner, RoundRobinDispatcher, UnorderedDispatcher},
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

struct ActorHandle {
    #[cfg(feature = "python")]
    inner: PyObject,
}

impl ActorHandle {
    fn try_new(projection: &[ExprRef]) -> DaftResult<Self> {
        #[cfg(feature = "python")]
        {
            let handle = Python::with_gil(|py| {
                // create python object
                Ok::<PyObject, PyErr>(
                    py.import(pyo3::intern!(py, "daft.execution.actor_pool_udf"))?
                        .getattr(pyo3::intern!(py, "ActorHandle"))?
                        .call1((projection
                            .iter()
                            .map(|expr| PyExpr::from(expr.clone()))
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

impl Drop for ActorHandle {
    fn drop(&mut self) {
        let result = self.teardown();

        if let Err(e) = result {
            log::error!("Error tearing down UDF actor: {}", e);
        }
    }
}

/// Each ActorPoolProjectState holds a handle to a single actor process.
/// The concurrency of the actor pool is thus tied to the concurrency of the operator
/// and the local executor handles task scheduling.
///
/// TODO: Implement a work-stealing dispatcher in the executor to improve pipelining.
struct ActorPoolProjectState {
    pub actor_handle: ActorHandle,
}

impl IntermediateOpState for ActorPoolProjectState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct ActorPoolProjectOperator {
    projection: Vec<ExprRef>,
    concurrency: usize,
    batch_size: Option<usize>,
    memory_request: u64,
}

impl ActorPoolProjectOperator {
    pub fn new(projection: Vec<ExprRef>) -> Self {
        let num_actor_pool_udfs: usize = count_actor_pool_udfs(&projection);

        assert_eq!(
            num_actor_pool_udfs, 1,
            "Expected only one actor pool udf in an actor pool project"
        );

        let concurrency = get_concurrency(&projection);
        let batch_size = get_batch_size(&projection);

        let memory_request = get_resource_request(&projection)
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);
        Self {
            projection,
            concurrency,
            batch_size,
            memory_request,
        }
    }
}

impl IntermediateOperator for ActorPoolProjectOperator {
    #[instrument(skip_all, name = "ActorPoolProjectOperator::execute")]
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
                let actor_pool_project_state = state
                    .as_any_mut()
                    .downcast_mut::<ActorPoolProjectState>()
                    .expect("ActorPoolProjectState");
                let res = actor_pool_project_state
                    .actor_handle
                    .eval_input(input)
                    .map(|result| IntermediateOperatorResult::NeedMoreInput(Some(result)))?;
                Ok((state, res))
            },
            Span::current(),
        );
        fut.into()
    }

    fn name(&self) -> &'static str {
        "ActorPoolProject"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("ActorPoolProject:".to_string());
        res.push(format!(
            "Projection = [{}]",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "UDFs = [{}]",
            self.projection.iter().flat_map(get_udf_names).join(", ")
        ));
        res.push(format!("Concurrency = {}", self.concurrency));
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
        // TODO: Pass relevant CUDA_VISIBLE_DEVICES to the actor
        Ok(Box::new(ActorPoolProjectState {
            actor_handle: ActorHandle::try_new(&self.projection)?,
        }))
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(self.concurrency)
    }

    fn dispatch_spawner(
        &self,
        runtime_handle: &ExecutionRuntimeContext,
        maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        if maintain_order {
            Arc::new(RoundRobinDispatcher::new(Some(
                self.batch_size
                    .unwrap_or_else(|| runtime_handle.default_morsel_size()),
            )))
        } else {
            Arc::new(UnorderedDispatcher::new(Some(
                self.batch_size
                    .unwrap_or_else(|| runtime_handle.default_morsel_size()),
            )))
        }
    }
}
