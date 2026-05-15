use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_micropartition::MicroPartition;
use pyo3::{Py, PyAny};
use tokio::sync::OnceCell;
use tracing::{Span, instrument};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
    StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

pub(crate) struct DistributedLimitSinkState {
    actor: Arc<Py<PyAny>>,
    /// Shared across every `PerInputState` for this sink. Ensures `start_task`
    /// is called exactly once per sink instance, even if the streaming-sink
    /// machinery creates multiple states (e.g., one per `InputId`). Calling
    /// `start_task` more than once with the same `task_id` would rewind prior
    /// claims and corrupt the global budget.
    started: Arc<OnceCell<()>>,
}

pub struct DistributedLimitSink {
    actor: Arc<Py<PyAny>>,
    task_id: String,
    limit: u64,
    offset: Option<u64>,
    /// One per sink. All `DistributedLimitSinkState`s clone this `Arc` so that
    /// `start_task` is only ever called once on the actor for this task.
    started: Arc<OnceCell<()>>,
}

impl DistributedLimitSink {
    pub fn new(actor: Arc<Py<PyAny>>, task_id: String, limit: u64, offset: Option<u64>) -> Self {
        Self {
            actor,
            task_id,
            limit,
            offset,
            started: Arc::new(OnceCell::new()),
        }
    }
}

impl StreamingSink for DistributedLimitSink {
    type State = DistributedLimitSinkState;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;

    #[instrument(skip_all, name = "DistributedLimitSink::sink")]
    fn execute(
        &self,
        input: MicroPartition,
        state: DistributedLimitSinkState,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        let task_id = self.task_id.clone();
        spawner
            .spawn(
                async move {
                    let started_actor = state.actor.clone();
                    let started_tid = task_id.clone();
                    state
                        .started
                        .get_or_try_init(|| async move {
                            common_runtime::python::execute_python_coroutine_noreturn(move |py| {
                                let coroutine = started_actor.call_method1(
                                    py,
                                    pyo3::intern!(py, "start_task"),
                                    (started_tid,),
                                )?;
                                Ok(coroutine.into_bound(py))
                            })
                            .await
                        })
                        .await?;

                    let num_rows = input.len();
                    let actor = state.actor.clone();
                    let tid = task_id.clone();
                    let (skip, take, done) = common_runtime::python::execute_python_coroutine::<
                        _,
                        (i64, i64, bool),
                    >(move |py| {
                        let coroutine = actor.call_method1(
                            py,
                            pyo3::intern!(py, "claim"),
                            (tid, num_rows),
                        )?;
                        Ok(coroutine.into_bound(py))
                    })
                    .await?;

                    let skip = skip as usize;
                    let take = take as usize;
                    let output = if take == 0 {
                        MicroPartition::empty(Some(input.schema()))
                    } else {
                        input.slice(skip, skip + take)?
                    };

                    if done {
                        Ok((state, StreamingSinkOutput::Finished(Some(output.into()))))
                    } else {
                        Ok((state, StreamingSinkOutput::NeedMoreInput(Some(output.into()))))
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        format!("DistributedLimit {}", self.limit).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Limit
    }

    fn multiline_display(&self) -> Vec<String> {
        match &self.offset {
            Some(o) => vec![format!(
                "DistributedLimit: Num Rows = {}, Offset = {}",
                self.limit, o
            )],
            None => vec![format!("DistributedLimit: {}", self.limit)],
        }
    }

    fn finalize(
        &self,
        _states: Vec<Self::State>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        Ok(StreamingSinkFinalizeOutput::Finished(None)).into()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(DistributedLimitSinkState {
            actor: self.actor.clone(),
            started: self.started.clone(),
        })
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn batching_strategy(&self) -> Self::BatchingStrategy {
        crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        )
    }
}
