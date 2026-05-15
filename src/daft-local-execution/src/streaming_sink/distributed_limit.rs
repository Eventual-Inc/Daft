use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_micropartition::MicroPartition;
use pyo3::{Py, PyAny};
use tracing::{Span, instrument};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
    StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

/// Per-task state for [`DistributedLimitSink`].
///
/// `started` flips to true after the first `actor.start_task(task_id)` call.
/// With `max_concurrency = 1` there's exactly one state per executing task, so
/// `start_task` is called once per task lifetime — which is what the actor's
/// retry-rewind semantics expect.
pub(crate) struct DistributedLimitSinkState {
    actor: Arc<Py<PyAny>>,
    started: bool,
}

pub struct DistributedLimitSink {
    actor: Arc<Py<PyAny>>,
    task_id: String,
    limit: u64,
    offset: Option<u64>,
}

impl DistributedLimitSink {
    pub fn new(actor: Arc<Py<PyAny>>, task_id: String, limit: u64, offset: Option<u64>) -> Self {
        Self {
            actor,
            task_id,
            limit,
            offset,
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
        mut state: DistributedLimitSinkState,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        let task_id = self.task_id.clone();
        spawner
            .spawn(
                async move {
                    if !state.started {
                        let actor = state.actor.clone();
                        let tid = task_id.clone();
                        common_runtime::python::execute_python_coroutine_noreturn(move |py| {
                            let coroutine = actor.call_method1(
                                py,
                                pyo3::intern!(py, "start_task"),
                                (tid,),
                            )?;
                            Ok(coroutine.into_bound(py))
                        })
                        .await?;
                        state.started = true;
                    }

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
            started: false,
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
