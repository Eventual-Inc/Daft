use std::sync::Arc;

use daft_micropartition::MicroPartition;
use tracing::{instrument, Span};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    StreamingSinkState,
};
use crate::{
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

struct LimitSinkState {
    remaining: usize,
}

impl LimitSinkState {
    fn new(remaining: usize) -> Self {
        Self { remaining }
    }

    fn get_remaining_mut(&mut self) -> &mut usize {
        &mut self.remaining
    }
}

impl StreamingSinkState for LimitSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct LimitSink {
    limit: usize,
}

impl LimitSink {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl StreamingSink for LimitSink {
    #[instrument(skip_all, name = "LimitSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult {
        let input_num_rows = input.len();

        let remaining = state
            .as_any_mut()
            .downcast_mut::<LimitSinkState>()
            .expect("Limit sink should have LimitSinkState")
            .get_remaining_mut();
        use std::cmp::Ordering::{Equal, Greater, Less};
        match input_num_rows.cmp(remaining) {
            Less => {
                *remaining -= input_num_rows;
                Ok((state, StreamingSinkOutput::NeedMoreInput(Some(input)))).into()
            }
            Equal => {
                *remaining = 0;
                Ok((state, StreamingSinkOutput::Finished(Some(input)))).into()
            }
            Greater => {
                let to_head = *remaining;
                *remaining = 0;
                spawner
                    .spawn(
                        async move {
                            let taken = input.head(to_head)?;
                            Ok((state, StreamingSinkOutput::Finished(Some(taken.into()))))
                        },
                        Span::current(),
                    )
                    .into()
            }
        }
    }

    fn name(&self) -> &'static str {
        "Limit"
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("Limit: {}", self.limit)]
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn StreamingSinkState>>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(LimitSinkState::new(self.limit))
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn dispatch_spawner(
        &self,
        _runtime_handle: &ExecutionRuntimeContext,
        _maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        // Limits are greedy, so we don't need to buffer any input.
        // They are also not concurrent, so we don't need to worry about ordering.
        Arc::new(UnorderedDispatcher::unbounded())
    }
}
