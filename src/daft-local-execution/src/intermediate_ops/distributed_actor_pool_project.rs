use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    vec,
};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_micropartition::MicroPartition;
#[cfg(feature = "python")]
use daft_micropartition::python::PyMicroPartition;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use rand::Rng;
use tracing::{Span, instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{MorselSizeRequirement, NodeName},
};

#[derive(Clone, Debug)]
pub(crate) struct ActorHandle {
    #[cfg(feature = "python")]
    inner: Arc<Py<PyAny>>,
}

impl ActorHandle {
    fn get_actors_on_current_node(actor_handles: Vec<Self>) -> DaftResult<(Vec<Self>, Vec<Self>)> {
        #[cfg(feature = "python")]
        {
            let actor_handles = actor_handles
                .into_iter()
                .map(|e| e.inner)
                .collect::<Vec<_>>();

            let (local_actors, remote_actors) = Python::attach(|py| {
                let actor_handles = actor_handles
                    .into_iter()
                    .map(|e| e.as_ref().clone_ref(py))
                    .collect::<Vec<_>>();
                let ray_actor_pool_udf_module =
                    py.import(pyo3::intern!(py, "daft.execution.ray_actor_pool_udf"))?;
                let (local_actors, remote_actors) = ray_actor_pool_udf_module
                    .call_method1(
                        pyo3::intern!(py, "get_ready_actors_by_location"),
                        (actor_handles,),
                    )?
                    .extract::<(Vec<Py<PyAny>>, Vec<Py<PyAny>>)>()?;
                DaftResult::Ok((local_actors, remote_actors))
            })?;

            let local_actors = local_actors
                .into_iter()
                .map(|e| Self { inner: Arc::new(e) })
                .collect::<Vec<_>>();
            let remote_actors = remote_actors
                .into_iter()
                .map(|e| Self { inner: Arc::new(e) })
                .collect::<Vec<_>>();
            Ok((local_actors, remote_actors))
        }
        #[cfg(not(feature = "python"))]
        {
            unimplemented!(
                "ActorHandle::get_actors_on_current_node is not implemented without Python"
            );
        }
    }

    #[cfg(feature = "python")]
    async fn eval_input(&self, input: Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        let inner = self.inner.clone();
        let result =
            common_runtime::python::execute_python_coroutine::<_, PyMicroPartition>(move |py| {
                let coroutine = inner.call_method1(
                    py,
                    pyo3::intern!(py, "eval_input"),
                    (PyMicroPartition::from(input),),
                )?;
                Ok(coroutine.into_bound(py))
            })
            .await?;
        Ok(result.into())
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
        let (local_actor_handles, remote_actor_handles) =
            ActorHandle::get_actors_on_current_node(actor_handles)?;

        let actor_handles = match local_actor_handles.len() {
            0 => remote_actor_handles,
            _ => local_actor_handles,
        };

        let init_counter = if actor_handles.is_empty() {
            0
        } else {
            rand::thread_rng().gen_range(0..actor_handles.len())
        };

        Ok(Self {
            actor_handles,
            batch_size,
            memory_request,
            counter: AtomicUsize::new(init_counter),
        })
    }
}

impl IntermediateOperator for DistributedActorPoolProjectOperator {
    type State = DistributedActorPoolProjectState;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "DistributedActorPoolProjectOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let memory_request = self.memory_request;
        #[cfg(feature = "python")]
        {
            let fut = task_spawner.spawn_with_memory_request(
                memory_request,
                async move {
                    let res =
                        state.actor_handle.eval_input(input).await.map(|result| {
                            IntermediateOperatorResult::NeedMoreInput(Some(result))
                        })?;
                    Ok((state, res))
                },
                Span::current(),
            );
            fut.into()
        }
        #[cfg(not(feature = "python"))]
        {
            unimplemented!(
                "DistributedActorPoolProjectOperator::execute is not implemented without Python"
            );
        }
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
        res.push(format!("BatchSize = {}", self.batch_size.unwrap_or(0)));
        res.push(format!("MemoryRequest = {}", self.memory_request));
        res
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        // Check if we need to initialize the filtered actor handles
        #[cfg(feature = "python")]
        {
            let next_actor_handle_idx =
                self.counter.fetch_add(1, Ordering::SeqCst) % self.actor_handles.len();
            let next_actor_handle = &self.actor_handles[next_actor_handle_idx];
            Ok(DistributedActorPoolProjectState {
                actor_handle: next_actor_handle.clone(),
            })
        }
        #[cfg(not(feature = "python"))]
        {
            unimplemented!(
                "DistributedActorPoolProjectOperator::make_state is not implemented without Python"
            );
        }
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        // We set the max concurrency to be the number of actor handles * 2 to such that each actor handle has 2 workers submitting to it.
        // This allows inputs to be queued up concurrently with UDF execution.
        Ok(self.actor_handles.len() * 2)
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.batch_size.map(MorselSizeRequirement::Strict)
    }
    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }
}
