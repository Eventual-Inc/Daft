use std::{
    cmp::Ordering::{Equal, Greater, Less},
    sync::Arc,
};

use common_error::{ensure, DaftResult};
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
    // The remaining number of rows to skip
    remaining_skip: usize,
    // The remaining number of rows to fetch
    remaining_take: usize,
}

impl LimitSinkState {
    fn new(offset: Option<usize>, limit: Option<usize>) -> Self {
        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);
        Self {
            remaining_skip: offset,
            remaining_take: limit.saturating_sub(offset),
        }
    }

    fn get_state_mut(&mut self) -> (&mut usize, &mut usize) {
        (&mut self.remaining_skip, &mut self.remaining_take)
    }
}

impl StreamingSinkState for LimitSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct LimitSink {
    offset: Option<usize>,
    limit: Option<usize>,
}

impl LimitSink {
    pub fn try_new(offset: Option<usize>, limit: Option<usize>) -> DaftResult<Self> {
        ensure!(
            offset.is_some() || limit.is_some(),
            ValueError:"Invalid LimitSink, offset and limit are both None."
        );
        Ok(Self { offset, limit })
    }
}

impl StreamingSink for LimitSink {
    #[instrument(skip_all, name = "LimitSink::sink")]
    fn execute(
        &self,
        mut input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult {
        let input_num_rows = input.len();

        let (remaining_skip, remaining_take) = state
            .as_any_mut()
            .downcast_mut::<LimitSinkState>()
            .expect("Limit sink should have LimitSinkState")
            .get_state_mut();

        if *remaining_skip > 0 {
            let skip_num_rows = (*remaining_skip).min(input_num_rows);
            *remaining_skip -= skip_num_rows;
            if skip_num_rows >= input_num_rows {
                return Ok((state, StreamingSinkOutput::NeedMoreInput(None))).into();
            }

            let (_, tail) = input.split_at(skip_num_rows).unwrap();
            input = tail.into();
        }

        match input_num_rows.cmp(remaining_take) {
            Less => {
                *remaining_take -= input_num_rows;
                Ok((state, StreamingSinkOutput::NeedMoreInput(Some(input)))).into()
            }
            Equal => {
                *remaining_take = 0;
                Ok((state, StreamingSinkOutput::Finished(Some(input)))).into()
            }
            Greater => {
                let to_head = *remaining_take;
                *remaining_take = 0;
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
        match (self.offset, self.limit) {
            (Some(o), Some(l)) => vec![format!("Limit: {}..{}", o, l)],
            (Some(o), None) => vec![format!("Limit: {}..", o)],
            (None, Some(l)) => vec![format!("Limit: {}", l)],
            (None, None) => unreachable!("Invalid LimitSink, offset and limit are both None."),
        }
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn StreamingSinkState>>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(LimitSinkState::new(self.offset, self.limit))
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
