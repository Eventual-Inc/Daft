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

pub struct DistributedLimitSink {
    actor: Arc<Py<PyAny>>,
    task_id: String,
    limit: u64,
    offset: Option<u64>,
    /// Guards the single `start_task` call to the actor. The streaming-sink
    /// framework may invoke `make_state` more than once per sink, but
    /// `start_task` must run exactly once per `task_id` — a second call
    /// would rewind the prior claims as if this were a retry and corrupt
    /// the global budget.
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
    type State = ();
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;

    #[instrument(skip_all, name = "DistributedLimitSink::sink")]
    fn execute(
        &self,
        input: MicroPartition,
        _state: (),
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        let actor = self.actor.clone();
        let task_id = self.task_id.clone();
        let started = self.started.clone();
        spawner
            .spawn(
                async move {
                    let started_actor = actor.clone();
                    let started_tid = task_id.clone();
                    started
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
                    let (skip, take, done) = common_runtime::python::execute_python_coroutine::<
                        _,
                        (i64, i64, bool),
                    >(move |py| {
                        let coroutine = actor.call_method1(
                            py,
                            pyo3::intern!(py, "claim"),
                            (task_id, num_rows),
                        )?;
                        Ok(coroutine.into_bound(py))
                    })
                    .await?;

                    debug_assert!(skip >= 0, "actor returned negative skip: {skip}");
                    debug_assert!(take >= 0, "actor returned negative take: {take}");
                    let skip = skip as usize;
                    let take = take as usize;
                    let output = if take == 0 {
                        MicroPartition::empty(Some(input.schema()))
                    } else {
                        input.slice(skip, skip + take)?
                    };

                    let signal = if done {
                        StreamingSinkOutput::Finished(Some(output.into()))
                    } else {
                        StreamingSinkOutput::NeedMoreInput(Some(output.into()))
                    };
                    Ok(((), signal))
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
        Ok(())
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
