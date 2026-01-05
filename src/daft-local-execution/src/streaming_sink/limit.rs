use std::{
    cmp::Ordering::{Equal, Greater, Less},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_micropartition::MicroPartition;
use tracing::{Span, instrument};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
    StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

pub(crate) struct LimitSinkState {
    limit_tracker: Arc<Mutex<(usize, usize)>>,
}

impl LimitSinkState {
    fn new(limit_tracker: Arc<Mutex<(usize, usize)>>) -> Self {
        Self { limit_tracker }
    }
}

pub struct LimitSink {
    limit: usize,
    offset: Option<usize>,
    limit_tracker: Arc<Mutex<(usize, usize)>>,
}

impl LimitSink {
    pub fn new(limit: usize, offset: Option<usize>) -> Self {
        Self {
            limit,
            offset,
            limit_tracker: Arc::new(Mutex::new((limit, offset.unwrap_or(0)))),
        }
    }

    fn process_input(
        mut input: Arc<MicroPartition>,
        limit_tracker: Arc<Mutex<(usize, usize)>>,
    ) -> DaftResult<StreamingSinkOutput> {
        let mut limit_tracker = limit_tracker.lock().unwrap();
        let (remaining_take, remaining_skip) = &mut *limit_tracker;
        let mut input_num_rows = input.len();

        println!("process_input: initial remaining_take={}, remaining_skip={}, input_num_rows={}", remaining_take, remaining_skip, input_num_rows);

        if *remaining_skip > 0 {
            let skip_num_rows = (*remaining_skip).min(input.len());
            println!("Skipping rows: skip_num_rows={}, before update remaining_skip={}", skip_num_rows, remaining_skip);
            *remaining_skip -= skip_num_rows;
            println!("After update remaining_skip={}", remaining_skip);
            if skip_num_rows >= input.len() {
                println!("Skipped entire input, requesting more input.");
                return Ok(StreamingSinkOutput::NeedMoreInput(Some(
                    MicroPartition::empty(Some(input.schema())).into(),
                )));
            }

            input = input.slice(skip_num_rows, input.len())?.into();
            input_num_rows = input.len();
            println!("After slicing: new input_num_rows={}", input_num_rows);
        }

        match input_num_rows.cmp(remaining_take) {
            Less => {
                println!("input_num_rows < remaining_take ({} < {}), taking all input and requesting more", input_num_rows, remaining_take);
                *remaining_take -= input_num_rows;
                println!("After update remaining_take={}", remaining_take);
                Ok(StreamingSinkOutput::NeedMoreInput(Some(input)))
            }
            Equal => {
                println!("input_num_rows == remaining_take ({} == {}), finished with this input", input_num_rows, remaining_take);
                *remaining_take = 0;
                Ok(StreamingSinkOutput::Finished(Some(input)))
            }
            Greater => {
                println!("input_num_rows > remaining_take ({} > {}), taking head and finished", input_num_rows, remaining_take);
                let to_head = *remaining_take;
                *remaining_take = 0;
                Ok(StreamingSinkOutput::Finished(Some(
                    input.head(to_head)?.into(),
                )))
            }
        }
    }
}

impl StreamingSink for LimitSink {
    type State = LimitSinkState;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "LimitSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: LimitSinkState,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        Self::process_input(input, state.limit_tracker.clone())
            .map(|output| (state, output))
            .into()
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
    ) -> StreamingSinkFinalizeResult<Self> {
        Ok(StreamingSinkFinalizeOutput::Finished(None)).into()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(LimitSinkState::new(self.limit_tracker.clone()))
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
