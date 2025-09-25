use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use common_error::DaftResult;
use daft_core::{
    prelude::{Int32Array, Schema},
    series::IntoSeries,
};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{
    server::flight_server::register_shuffle_cache, shuffle_cache::InProgressShuffleCache,
};
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, ops::NodeType, pipeline::NodeName};

static COUNTER: AtomicU32 = AtomicU32::new(0);

pub struct FlightShuffleWriteState {
    shuffle_cache: Arc<InProgressShuffleCache>,
}

pub struct FlightShuffleWriteSink {
    num_partitions: usize,
    shuffle_id: u64,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
    partition_by: Option<Vec<ExprRef>>,
}

impl FlightShuffleWriteSink {
    pub fn new(
        num_partitions: usize,
        partition_by: Option<Vec<ExprRef>>,
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
    ) -> Self {
        Self {
            num_partitions,
            shuffle_id,
            shuffle_dirs,
            compression,
            partition_by,
        }
    }
}

impl BlockingSink for FlightShuffleWriteSink {
    type State = FlightShuffleWriteState;

    #[instrument(skip_all, name = "FlightShuffleWriteSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        spawner
            .spawn(
                async move {
                    state.shuffle_cache.push_partitions(vec![input]).await?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "FlightShuffleWriteSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let num_partitions = self.num_partitions;
        let shuffle_id = self.shuffle_id;
        spawner
            .spawn(
                async move {
                    // Close the shuffle cache to finalize it
                    let shuffle_cache = states.into_iter().next().unwrap().shuffle_cache;
                    let finalized_cache = shuffle_cache.close().await?;
                    let rows_per_partition = finalized_cache.rows_per_partition();
                    let bytes_per_partition = finalized_cache.bytes_per_partition();

                    // Register the shuffle cache with the flight server
                    register_shuffle_cache(shuffle_id, finalized_cache.into()).await?;

                    // let partitions = Int32Array::from_values(
                    //     "partition_idx",
                    //     (0..num_partitions).into_iter().map(|i| i as i32),
                    // );
                    let rows_per_partition = Int32Array::from_values(
                        "rows_per_partition",
                        rows_per_partition.into_iter().map(|i| i as i32),
                    )
                    .into_series();
                    let bytes_per_partition = Int32Array::from_values(
                        "bytes_per_partition",
                        bytes_per_partition.into_iter().map(|i| i as i32),
                    )
                    .into_series();
                    let schema = Schema::new(vec![
                        rows_per_partition.field().clone(),
                        bytes_per_partition.field().clone(),
                    ]);
                    let result = RecordBatch::new_with_size(
                        schema.clone(),
                        vec![rows_per_partition, bytes_per_partition],
                        num_partitions,
                    )?;
                    let result_mp =
                        MicroPartition::new_loaded(schema.into(), Arc::new(vec![result]), None);

                    Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(
                        result_mp,
                    )]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "FlightShuffleWrite".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition // Use the same node type as regular repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("FlightShuffleWrite: {} partitions", self.num_partitions),
            format!("Shuffle ID: {}", self.shuffle_id),
        ]
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        let next_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let shuffle_cache = InProgressShuffleCache::try_new(
            self.num_partitions,
            &self.shuffle_dirs,
            next_id.to_string(),
            self.shuffle_id,
            1024 * 1024 * 10,            // 10MB target file size
            self.compression.as_deref(), // No compression
            self.partition_by.clone(),   // Use the partition_by expressions from repartition_spec
        )?;
        Ok(FlightShuffleWriteState {
            shuffle_cache: Arc::new(shuffle_cache),
        })
    }
}
