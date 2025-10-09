use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

pub(crate) enum IntoPartitionsState {
    Building(Vec<Arc<MicroPartition>>),
    Done,
}

impl IntoPartitionsState {
    fn push(&mut self, part: Arc<MicroPartition>) {
        if let Self::Building(parts) = self {
            parts.push(part);
        } else {
            panic!("IntoPartitionsSink should be in Building state");
        }
    }

    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let res = if let Self::Building(parts) = self {
            std::mem::take(parts)
        } else {
            panic!("IntoPartitionsSink should be in Building state");
        };
        *self = Self::Done;
        res
    }
}

pub struct IntoPartitionsSink {
    num_partitions: usize,
    schema: SchemaRef,
}

impl IntoPartitionsSink {
    pub fn new(num_partitions: usize, schema: SchemaRef) -> Self {
        Self {
            num_partitions,
            schema,
        }
    }
}

impl BlockingSink for IntoPartitionsSink {
    type State = IntoPartitionsState;

    #[instrument(skip_all, name = "IntoPartitionsSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        state.push(input);
        Ok(BlockingSinkStatus::NeedMoreInput(state)).into()
    }

    #[instrument(skip_all, name = "IntoPartitionsSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let num_partitions = self.num_partitions;
        let schema = self.schema.clone();

        spawner
            .spawn(
                async move {
                    // Collect all data from all states
                    let all_parts: Vec<Arc<MicroPartition>> = states
                        .into_iter()
                        .flat_map(|mut state| state.finalize())
                        .collect();

                    // Concatenate all data
                    let concatenated = MicroPartition::concat(all_parts)?;

                    let total_rows = concatenated.len();
                    let rows_per_partition = total_rows.div_ceil(num_partitions);

                    let mut outputs = Vec::new();
                    for i in 0..num_partitions {
                        let start_idx = i * rows_per_partition;
                        let end_idx = std::cmp::min(start_idx + rows_per_partition, total_rows);

                        if start_idx < total_rows {
                            let sliced_table = concatenated.slice(start_idx, end_idx)?;
                            outputs.push(Arc::new(sliced_table));
                        } else {
                            // Empty partition
                            let mp =
                                MicroPartition::new_loaded(schema.clone(), Arc::new(vec![]), None);
                            outputs.push(Arc::new(mp));
                        }
                    }
                    Ok(BlockingSinkFinalizeOutput::Finished(outputs))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "IntoPartitions".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::IntoPartitions
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "IntoPartitions: Into {} partitions",
            self.num_partitions
        )]
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(IntoPartitionsState::Building(Vec::new()))
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}
