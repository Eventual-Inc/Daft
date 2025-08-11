use std::sync::Arc;

use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use tracing::{instrument, Span};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{ops::NodeType, pipeline::NodeName, ExecutionTaskSpawner};

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

                    let tables = input.get_tables()?;
                    let mut results = Vec::with_capacity(tables.len());
                    for t in tables.iter() {
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
        "MonotonicallyIncreasingId".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::MonotonicallyIncreasingId
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["MonotonicallyIncreasingId".to_string()]
    }

    fn finalize(
        &self,
        _states: Vec<Self::State>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn make_state(&self) -> Self::State {
        MonotonicallyIncreasingIdState {
            id_offset: self.params.starting_offset.unwrap_or(0),
        }
    }

    // Monotonically increasing id is a memory-bound operation, so there's no performance benefit to parallelizing it.
    // Furthermore, it is much simpler to implement as a single-threaded operation, since we can just keep track of the current id offset without synchronization.
    fn max_concurrency(&self) -> usize {
        1
    }
}
