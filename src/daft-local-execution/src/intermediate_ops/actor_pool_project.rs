use std::sync::Arc;

use common_error::DaftResult;
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{functions::python::extract_stateful_udf_exprs, ExprRef};
#[cfg(feature = "python")]
use daft_micropartition::python::PyMicroPartition;
use daft_micropartition::MicroPartition;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use tracing::instrument;

use super::intermediate_op::{
    DynIntermediateOpState, IntermediateOperator, IntermediateOperatorResult,
    IntermediateOperatorState,
};
use crate::pipeline::PipelineResultType;

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
                    py.import_bound(pyo3::intern!(py, "daft.execution.stateful_actor"))?
                        .getattr(pyo3::intern!(py, "StatefulActorHandle"))?
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
            panic!("Cannot evaluate a stateful UDF without compiling for Python");
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
            log::error!("Error tearing down stateful UDF actor: {}", e);
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

impl DynIntermediateOpState for ActorPoolProjectState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct ActorPoolProjectOperator {
    projection: Vec<ExprRef>,
    concurrency: usize,
    batch_size: Option<usize>,
}

impl ActorPoolProjectOperator {
    pub fn new(projection: Vec<ExprRef>) -> Self {
        let stateful_udf_vec = projection
            .iter()
            .flat_map(|expr| extract_stateful_udf_exprs(expr.clone()))
            .collect::<Vec<_>>();

        let [stateful_udf] = stateful_udf_vec
            .try_into()
            .expect("Expected only one stateful udf in an actor pool project");

        Self {
            projection,
            concurrency: stateful_udf
                .concurrency
                .expect("Stateful UDF should have concurrency"),
            batch_size: stateful_udf.batch_size,
        }
    }
}

impl IntermediateOperator for ActorPoolProjectOperator {
    #[instrument(skip_all, name = "ActorPoolProjectOperator::execute")]
    fn execute(
        &self,
        _idx: usize,
        input: &PipelineResultType,
        state: &IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult> {
        state.with_state_mut::<ActorPoolProjectState, _, _>(|state| {
            state
                .actor_handle
                .eval_input(input.as_data().clone())
                .map(|result| IntermediateOperatorResult::NeedMoreInput(Some(result)))
        })
    }

    fn name(&self) -> &'static str {
        "ActorPoolProject"
    }

    fn make_state(&self) -> DaftResult<Box<dyn DynIntermediateOpState>> {
        // TODO: Pass relevant CUDA_VISIBLE_DEVICES to the actor
        Ok(Box::new(ActorPoolProjectState {
            actor_handle: ActorHandle::try_new(&self.projection)?,
        }))
    }

    fn max_concurrency(&self) -> usize {
        self.concurrency
    }

    fn morsel_size(&self) -> Option<usize> {
        self.batch_size
    }
}
