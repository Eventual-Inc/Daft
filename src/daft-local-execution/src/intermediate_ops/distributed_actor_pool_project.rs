use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    vec,
};

use common_error::DaftResult;
#[cfg(feature = "python")]
use daft_micropartition::python::PyMicroPartition;
use daft_micropartition::MicroPartition;
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

#[derive(Clone, Debug)]
pub(crate) struct ActorHandle {
    #[cfg(feature = "python")]
    inner: Arc<PyObject>,
}

impl ActorHandle {
    fn is_on_current_node(&self) -> DaftResult<bool> {
        #[cfg(feature = "python")]
        {
            Python::with_gil(|py| {
                let py_actor_handle = self.inner.bind(py);
                let is_on_current_node =
                    py_actor_handle.call_method0(pyo3::intern!(py, "is_on_current_node"))?;
                Ok(is_on_current_node.extract::<bool>()?)
            })
        }
        #[cfg(not(feature = "python"))]
        {
            panic!("Cannot check if an actor is on the current node without compiling for Python");
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
}

#[cfg(feature = "python")]
impl From<common_py_serde::PyObjectWrapper> for ActorHandle {
    fn from(value: common_py_serde::PyObjectWrapper) -> Self {
        Self { inner: value.0 }
    }
}

pub(crate) struct DistributedActorPoolProjectState {
    actor_handle: ActorHandle,
}

pub(crate) struct DistributedActorPoolProjectOperator {
    actor_handles: Vec<ActorHandle>,
    batch_size: Option<usize>,
    memory_request: u64,
    counter: AtomicUsize,
}

impl DistributedActorPoolProjectOperator {
    pub fn try_new(
        actor_handles: Vec<impl Into<ActorHandle>>,
        batch_size: Option<usize>,
        memory_request: u64,
    ) -> DaftResult<Self> {
        let actor_handles: Vec<ActorHandle> = actor_handles.into_iter().map(|e| e.into()).collect();

        // Filter for actors on the current node
        let mut local_actor_handles = Vec::new();
        for handle in &actor_handles {
            if handle.is_on_current_node()? {
                local_actor_handles.push(handle.clone());
            }
        }

        // If no actors are on the current node, use all actors as fallback
        let actor_handles = if local_actor_handles.is_empty() {
            actor_handles
        } else {
            local_actor_handles
        };

        Ok(Self {
            actor_handles,
            batch_size,
            memory_request,
            counter: AtomicUsize::new(0),
        })
    }
}

impl IntermediateOperator for DistributedActorPoolProjectOperator {
    type State = DistributedActorPoolProjectState;

    #[instrument(skip_all, name = "DistributedActorPoolProjectOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let memory_request = self.memory_request;
        let fut = task_spawner.spawn_with_memory_request(
            memory_request,
            async move {
                let res = state
                    .actor_handle
                    .eval_input(input)
                    .map(|result| IntermediateOperatorResult::NeedMoreInput(Some(result)))?;
                Ok((state, res))
            },
            Span::current(),
        );
        fut.into()
    }

    fn name(&self) -> NodeName {
        "DistributedActorPoolProject".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::DistributedActorPoolProject
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("DistributedActorPoolProject:".to_string());
        #[cfg(feature = "python")]
        res.push(format!(
            "ActorHandles = [{}]",
            self.actor_handles
                .iter()
                .map(|e| e.inner.to_string())
                .join(", ")
        ));
        res.push(format!("BatchSize = {}", self.batch_size.unwrap_or(0)));
        res.push(format!("MemoryRequest = {}", self.memory_request));
        res
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        let next_actor_handle_idx =
            self.counter.fetch_add(1, Ordering::SeqCst) % self.actor_handles.len();
        let next_actor_handle = &self.actor_handles[next_actor_handle_idx];
        Ok(DistributedActorPoolProjectState {
            actor_handle: next_actor_handle.clone(),
        })
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(self.actor_handles.len())
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.batch_size.map(MorselSizeRequirement::Strict)
    }
}
