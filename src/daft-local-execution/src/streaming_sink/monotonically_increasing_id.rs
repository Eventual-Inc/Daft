use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use tracing::{Span, instrument};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
    StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

pub(crate) struct MonotonicallyIncreasingIdState {
    id_offset: u64,
}

impl MonotonicallyIncreasingIdState {
    fn fetch_and_increment_offset(&mut self, increment: u64) -> u64 {
        let id_offset = self.id_offset;
        self.id_offset += increment;
        id_offset
    }
}

struct MonotonicallyIncreasingIdParams {
    column_name: String,
    starting_offset: Option<u64>,
    output_schema: SchemaRef,
}

pub struct MonotonicallyIncreasingIdSink {
    params: Arc<MonotonicallyIncreasingIdParams>,
}
impl MonotonicallyIncreasingIdSink {
    pub fn new(
        column_name: String,
        starting_offset: Option<u64>,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            params: Arc::new(MonotonicallyIncreasingIdParams {
                column_name,
                starting_offset,
                output_schema,
            }),
        }
    }
}

impl StreamingSink for MonotonicallyIncreasingIdSink {
    type State = MonotonicallyIncreasingIdState;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "MonotonicallyIncreasingIdSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let mut id_offset = state.fetch_and_increment_offset(input.len() as u64);

                    let tables = input.record_batches();
                    let mut results = Vec::with_capacity(tables.len());
                    for t in tables {
                        let len = t.len() as u64;
                        results.push(t.add_monotonically_increasing_id(
                            0,
                            id_offset,
                            &params.column_name,
                        )?);
                        id_offset += len;
                    }

                    let out = MicroPartition::new_loaded(
                        params.output_schema.clone(),
                        results.into(),
                        None,
                    );

                    Ok((
                        state,
                        StreamingSinkOutput::NeedMoreInput(Some(Arc::new(out))),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Monotonic ID".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::MonotonicallyIncreasingId
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["Monotonic ID".to_string()]
    }

    fn finalize(
        &self,
        _states: Vec<Self::State>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        Ok(StreamingSinkFinalizeOutput::Finished(None)).into()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(MonotonicallyIncreasingIdState {
            id_offset: self.params.starting_offset.unwrap_or(0),
        })
    }

    // Monotonically increasing id is a memory-bound operation, so there's no performance benefit to parallelizing it.
    // Furthermore, it is much simpler to implement as a single-threaded operation, since we can just keep track of the current id offset without synchronization.
    fn max_concurrency(&self) -> usize {
        1
    }
    fn batching_strategy(&self) -> Self::BatchingStrategy {
        crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        )
    }
}
