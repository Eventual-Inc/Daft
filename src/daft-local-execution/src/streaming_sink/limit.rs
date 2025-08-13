use std::{
    cmp::Ordering::{Equal, Greater, Less},
    sync::Arc,
};

use daft_micropartition::MicroPartition;
use tracing::{instrument, Span};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    ops::NodeType,
    pipeline::NodeName,
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

pub(crate) struct LimitSinkState {
    // The remaining number of rows to skip
    remaining_skip: usize,
    // The remaining number of rows to fetch
    remaining_take: usize,
}

impl LimitSinkState {
    fn new(limit: usize, offset: Option<usize>) -> Self {
        Self {
            remaining_skip: offset.unwrap_or(0),
            remaining_take: limit,
        }
    }

    fn get_state_mut(&mut self) -> (&mut usize, &mut usize) {
        (&mut self.remaining_skip, &mut self.remaining_take)
    }
}

pub struct LimitSink {
    limit: usize,
    offset: Option<usize>,
}

impl LimitSink {
    pub fn new(limit: usize, offset: Option<usize>) -> Self {
        Self { limit, offset }
    }
}

impl StreamingSink for LimitSink {
    type State = LimitSinkState;

    #[instrument(skip_all, name = "LimitSink::sink")]
    fn execute(
        &self,
        mut input: Arc<MicroPartition>,
        mut state: LimitSinkState,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        let mut input_num_rows = input.len();

        let (remaining_skip, remaining_take) = state.get_state_mut();

        if *remaining_skip > 0 {
            let skip_num_rows = (*remaining_skip).min(input_num_rows);
            *remaining_skip -= skip_num_rows;
            if skip_num_rows >= input_num_rows {
                return Ok((
                    state,
                    StreamingSinkOutput::NeedMoreInput(Some(
                        MicroPartition::empty(Some(input.schema())).into(),
                    )),
                ))
                .into();
            }

            input = input.slice(skip_num_rows, input_num_rows).unwrap().into();
            input_num_rows = input.len();
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

    fn name(&self) -> NodeName {
        format!("Limit {}", self.limit).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Limit
    }

    fn multiline_display(&self) -> Vec<String> {
        match &self.offset {
            Some(o) => vec![format!("Limit: Num Rows = {}, Offset = {}", self.limit, o)],
            None => vec![format!("Limit: {}", self.limit)],
        }
    }

    fn finalize(
        &self,
        _states: Vec<Self::State>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn make_state(&self) -> Self::State {
        LimitSinkState::new(self.limit, self.offset)
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
