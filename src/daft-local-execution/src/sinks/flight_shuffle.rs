use std::{collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_shuffles::{
    shuffle_cache::InProgressShuffleCache,
};
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{ops::NodeType, pipeline::NodeName, ExecutionTaskSpawner};

#[derive(Debug, Clone)]
pub struct FlightInfo {
    pub address: String,
    pub port: u16,
}

pub(crate) struct FlightShuffleState {
    shuffle_cache: Option<InProgressShuffleCache>,
    parts_buffer: VecDeque<Arc<MicroPartition>>,
}

impl FlightShuffleState {
    fn new(shuffle_cache: InProgressShuffleCache) -> Self {
        Self {
            shuffle_cache: Some(shuffle_cache),
            parts_buffer: VecDeque::new(),
        }
    }

    fn push(&mut self, part: Arc<MicroPartition>) {
        self.parts_buffer.push_back(part);
    }
}

pub struct FlightShuffleSink {
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    schema: SchemaRef,
    shuffle_dirs: Vec<String>,
    node_id: String,
    shuffle_stage_id: usize,
}

impl FlightShuffleSink {
    pub fn new(
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        schema: SchemaRef,
        shuffle_dirs: Vec<String>,
        node_id: String,
        shuffle_stage_id: usize,
    ) -> Self {
        Self {
            repartition_spec,
            num_partitions,
            schema,
            shuffle_dirs,
            node_id,
            shuffle_stage_id,
        }
    }
}

impl BlockingSink for FlightShuffleSink {
    type State = FlightShuffleState;

    #[instrument(skip_all, name = "FlightShuffleSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        state.push(input);
        Ok(BlockingSinkStatus::NeedMoreInput(state)).into()
    }

    #[instrument(skip_all, name = "FlightShuffleSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let schema = self.schema.clone();

        spawner
            .spawn(
                async move {
                    // For now, just collect all the partitions and drop them
                    // This is a simplified implementation to get the basic flow working
                    let mut _total_partitions = 0;
                    for mut state in states {
                        while let Some(_part) = state.parts_buffer.pop_front() {
                            _total_partitions += 1;
                        }
                    }

                    // Create a micropartition to signal completion
                    let info_mp = MicroPartition::new_loaded(
                        schema,
                        Arc::new(vec![]),
                        None,
                    );

                    Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(info_mp)]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "FlightShuffle".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "FlightShuffle: {} partitions to {} dirs",
            self.num_partitions,
            self.shuffle_dirs.len()
        )]
    }

    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        // For now, create a simple state without shuffle cache
        Ok(FlightShuffleState {
            shuffle_cache: None,
            parts_buffer: VecDeque::new(),
        })
    }
}