use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::get_compute_pool_num_threads;
use daft_core::{
    prelude::{Int32Array, Schema},
    series::IntoSeries,
};
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{
    server::flight_server::ShuffleFlightServer, shuffle_cache::InProgressShuffleCache,
};
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

pub struct FlightShuffleWriteSink {
    num_partitions: usize,
    shuffle_id: u64,
    cache_id: String,
    partition_by: Option<Vec<ExprRef>>,
    // Shared shuffle cache
    shuffle_cache: Arc<InProgressShuffleCache>,
    // Flight shuffle server registry for this node (if enabled)
    shuffle_server: Arc<ShuffleFlightServer>,
}

impl FlightShuffleWriteSink {
    pub fn try_new(
        num_partitions: usize,
        partition_by: Option<Vec<ExprRef>>,
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        cache_id: String,
        shuffle_server: Arc<ShuffleFlightServer>,
    ) -> DaftResult<Self> {
        const TARGET_TOTAL_IN_MEMORY_SIZE_BYTES: usize = 1024 * 1024 * 2000; // 2000MB = ~2GB

        let shuffle_cache = shuffle_server.get_or_create_in_progress_shuffle_cache(
            num_partitions,
            &shuffle_dirs,
            shuffle_id,
            (TARGET_TOTAL_IN_MEMORY_SIZE_BYTES * get_compute_pool_num_threads() / num_partitions)
                .clamp(1024 * 1024 * 8, 1024 * 1024 * 128), // Min 8MB, Max 128MB
            compression.as_deref(),
            cache_id.clone(),
        )?;

        Ok(Self {
            num_partitions,
            shuffle_id,
            cache_id,
            partition_by,
            shuffle_cache,
            shuffle_server,
        })
    }
}

impl BlockingSink for FlightShuffleWriteSink {
    type State = ();

    #[instrument(skip_all, name = "FlightShuffleWriteSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        _state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let num_partitions = self.num_partitions;
        let partition_by = self.partition_by.clone();
        let shuffle_cache = self.shuffle_cache.clone();

        spawner
            .spawn(
                async move {
                    let partitioned = match &partition_by {
                        Some(partition_by) => {
                            let partition_by = BoundExpr::bind_all(partition_by, &input.schema())?;
                            input.partition_by_hash(&partition_by, num_partitions)?
                        }
                        None => input.partition_by_random(num_partitions, 0)?,
                    };

                    // Convert Vec<MicroPartition> to Vec<Arc<MicroPartition>>
                    let partitioned = partitioned.into_iter().map(Arc::new).collect();

                    // Send partitioned data to writers
                    shuffle_cache.push_partitioned_data(partitioned).await?;
                    Ok(())
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "FlightShuffleWriteSink::finalize")]
    fn finalize(
        &self,
        _states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let num_partitions = self.num_partitions;
        let shuffle_id = self.shuffle_id;
        let cache_id = self.cache_id.clone();
        let shuffle_server = self.shuffle_server.clone();
        spawner
            .spawn(
                async move {
                    let finalized_cache = shuffle_server
                        .release_in_progress_shuffle_cache(shuffle_id, &cache_id)
                        .await?;
                    let (rows_per_partition, bytes_per_partition) =
                        if let Some(finalized_cache) = finalized_cache {
                            (
                                finalized_cache.rows_per_partition(),
                                finalized_cache.bytes_per_partition(),
                            )
                        } else {
                            (vec![0; num_partitions], vec![0; num_partitions])
                        };

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
            format!("Cache ID: {}", self.cache_id),
        ]
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        // No per-state data needed, shuffle cache is shared in the sink
        Ok(())
    }
}
