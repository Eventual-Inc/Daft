use std::sync::Arc;

use common_runtime::RuntimeRef;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    StreamingSinkState,
};
use crate::{
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    ExecutionRuntimeContext,
};

struct MonotonicallyIncreasingIdState {
    id_offset: u64,
}

impl MonotonicallyIncreasingIdState {
    fn fetch_and_increment_offset(&mut self, increment: u64) -> u64 {
        let id_offset = self.id_offset;
        self.id_offset += increment;
        id_offset
    }
}

impl StreamingSinkState for MonotonicallyIncreasingIdState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct MonotonicallyIncreasingIdParams {
    column_name: String,
    output_schema: SchemaRef,
}

pub struct MonotonicallyIncreasingIdSink {
    params: Arc<MonotonicallyIncreasingIdParams>,
}
impl MonotonicallyIncreasingIdSink {
    pub fn new(column_name: String, output_schema: SchemaRef) -> Self {
        Self {
            params: Arc::new(MonotonicallyIncreasingIdParams {
                column_name,
                output_schema,
            }),
        }
    }
}

impl StreamingSink for MonotonicallyIncreasingIdSink {
    #[instrument(skip_all, name = "MonotonicallyIncreasingIdSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        runtime_ref: &RuntimeRef,
    ) -> StreamingSinkExecuteResult {
        let params = self.params.clone();
        runtime_ref
            .spawn(async move {
                let mut id_offset = state
                    .as_any_mut()
                    .downcast_mut::<MonotonicallyIncreasingIdState>()
                    .expect("MonotonicallyIncreasingIdOperator should have MonotonicallyIncreasingIdState")
                    .fetch_and_increment_offset(input.len() as u64);

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
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "MonotonicallyIncreasingId"
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn StreamingSinkState>>,
        _runtime_ref: &RuntimeRef,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(MonotonicallyIncreasingIdState { id_offset: 0 })
    }

    // Monotonically increasing id is a memory-bound operation, so there's no performance benefit to parallelizing it.
    // Furthermore, it is much simpler to implement as a single-threaded operation, since we can just keep track of the current id offset without synchronization.
    fn max_concurrency(&self) -> usize {
        1
    }

    fn dispatch_spawner(
        &self,
        _runtime_handle: &ExecutionRuntimeContext,
        _maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        Arc::new(UnorderedDispatcher::new(None))
    }
}
