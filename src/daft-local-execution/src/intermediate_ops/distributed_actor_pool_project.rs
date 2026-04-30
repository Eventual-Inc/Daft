use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    vec,
};

use daft_common::error::{DaftError, DaftResult};
use daft_common::metrics::ops::NodeType;
use daft_core::prelude::{Schema, SchemaRef};
use daft_dsl::expr::bound_expr::BoundExpr;
#[cfg(feature = "python")]
use daft_micropartition::python::PyMicroPartition;
use daft_micropartition::{MicroPartition, partitioning::Partition};
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use rand::Rng;
use tracing::{Span, instrument};

use super::intermediate_op::{IntermediateOpExecuteResult, IntermediateOperator};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, MorselSizeRequirement, NodeName},
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
    async fn eval_input(&self, input: MicroPartition) -> DaftResult<MicroPartition> {
        let inner = self.inner.clone();
        let result =
            daft_common::runtime::python::execute_python_coroutine::<_, PyMicroPartition>(move |py| {
                let coroutine = inner.call_method1(
                    py,
                    pyo3::intern!(py, "eval_input"),
                    (PyMicroPartition::from(input),),
                )?;
                Ok(coroutine.into_bound(py))
            })
            .await?;
        let mp: Arc<MicroPartition> = result.into();
        Ok(Arc::try_unwrap(mp).unwrap_or_else(|a| a.as_ref().clone()))
    }
}

#[cfg(feature = "python")]
impl From<daft_common::py_serde::PyObjectWrapper> for ActorHandle {
    fn from(value: daft_common::py_serde::PyObjectWrapper) -> Self {
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
    passthrough_columns: Vec<BoundExpr>, // Columns that no need to be passed to UDFActor for processing
    required_columns: Vec<usize>,        // UDF input columns
    schema: SchemaRef,                   // Output schema
    counter: AtomicUsize,
}

impl DistributedActorPoolProjectOperator {
    pub fn try_new(
        actor_handles: Vec<impl Into<ActorHandle>>,
        batch_size: Option<usize>,
        memory_request: u64,
        passthrough_columns: Vec<BoundExpr>,
        required_columns: Vec<usize>,
        schema: SchemaRef,
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
            rand::rng().random_range(0..actor_handles.len())
        };

        Ok(Self {
            actor_handles,
            batch_size,
            memory_request,
            passthrough_columns,
            required_columns,
            schema,
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
        input: MicroPartition,
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        task_spawner: &ExecutionTaskSpawner,
        _input_id: InputId,
    ) -> IntermediateOpExecuteResult<Self> {
        #[cfg(feature = "python")]
        {
            let memory_request = self.memory_request;
            let required_columns = self.required_columns.clone();
            let passthrough_columns = self.passthrough_columns.clone();
            let output_schema = self.schema.clone();

            let fut = task_spawner.spawn_with_memory_request(
                memory_request,
                async move {
                    // Prune input by required cols
                    let input_schema = input.schema();
                    let pruned_input = MicroPartition::new_loaded(
                        Arc::new(Schema::new(
                            required_columns
                                .iter()
                                .map(|idx| input_schema[*idx].clone()),
                        )),
                        Arc::new(
                            input
                                .record_batches()
                                .iter()
                                .map(|batch| batch.get_columns(required_columns.as_slice()))
                                .collect::<Vec<_>>(),
                        ),
                        None,
                    );
                    let input_num_rows = pruned_input.num_rows();

                    // Call the UDF with pruned input
                    let eval_output = state.actor_handle.eval_input(pruned_input).await?;
                    if eval_output.schema().fields().len() != 1 {
                        return Err(DaftError::InternalError(format!(
                            "UDF output schema must be a single column, but got '{}'",
                            eval_output.schema().field_names().join(", ")
                        )));
                    }

                    if input_num_rows != eval_output.num_rows() {
                        return Err(DaftError::InternalError(format!(
                            "The number of rows is mismatch between UDF input {} and output {}",
                            input_num_rows,
                            eval_output.num_rows()
                        )));
                    }

                    // Combine the eval output with passthrough columns
                    let mut output_batches = Vec::with_capacity(input.num_rows());
                    for (input_record, output_record) in input
                        .record_batches()
                        .iter()
                        .zip(eval_output.record_batches().iter())
                    {
                        let passthrough_record =
                            input_record.eval_expression_list(passthrough_columns.as_slice())?;

                        output_batches.push(passthrough_record.append_column(
                            output_schema.clone(),
                            output_record.get_column(0).clone(),
                        )?);
                    }

                    Ok((
                        state,
                        MicroPartition::new_loaded(
                            output_schema.clone(),
                            Arc::new(output_batches),
                            None,
                        ),
                    ))
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

    fn make_state(&self) -> Self::State {
        // Check if we need to initialize the filtered actor handles
        #[cfg(feature = "python")]
        {
            let next_actor_handle_idx =
                self.counter.fetch_add(1, Ordering::SeqCst) % self.actor_handles.len();
            let next_actor_handle = &self.actor_handles[next_actor_handle_idx];
            DistributedActorPoolProjectState {
                actor_handle: next_actor_handle.clone(),
            }
        }
        #[cfg(not(feature = "python"))]
        {
            unimplemented!(
                "DistributedActorPoolProjectOperator::make_state is not implemented without Python"
            );
        }
    }

    fn max_concurrency(&self) -> usize {
        // We set the max concurrency to be the number of actor handles * 2 to such that each actor handle has 2 workers submitting to it.
        // This allows inputs to be queued up concurrently with UDF execution.
        self.actor_handles.len() * 2
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.batch_size
            .and_then(NonZeroUsize::new)
            .map(MorselSizeRequirement::Strict)
    }
    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }
}
