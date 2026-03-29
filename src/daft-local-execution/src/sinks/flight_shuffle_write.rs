use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{
    prelude::{Int32Array, Schema},
    series::IntoSeries,
};
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{
    server::flight_server::register_shuffle_cache, shuffle_cache::InProgressShuffleCache,
};
use tracing::{Span, instrument};

use super::blocking_sink::{BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

pub(crate) struct FlightShuffleWriteState {
    cache: Option<Arc<InProgressShuffleCache>>,
    cache_id: String,
}

pub struct FlightShuffleWriteSink {
    num_partitions: usize,
    shuffle_id: u64,
    partition_by: Option<Vec<ExprRef>>,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
    target_in_memory_size_per_partition: usize,
}

impl FlightShuffleWriteSink {
    pub fn try_new(
        num_partitions: usize,
        partition_by: Option<Vec<ExprRef>>,
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        _cache_id: String,
    ) -> DaftResult<Self> {
        const TARGET_TOTAL_IN_MEMORY_SIZE_BYTES: usize = 1024 * 1024 * 2000;
        Ok(Self {
            num_partitions,
            shuffle_id,
            partition_by,
            shuffle_dirs,
            compression,
            target_in_memory_size_per_partition: (TARGET_TOTAL_IN_MEMORY_SIZE_BYTES
                / num_partitions)
                .clamp(1024 * 1024 * 8, 1024 * 1024 * 128),
        })
    }
}

impl BlockingSink for FlightShuffleWriteSink {
    type State = FlightShuffleWriteState;

    fn max_concurrency(&self) -> usize {
        1
    }

    #[instrument(skip_all, name = "FlightShuffleWriteSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let num_partitions = self.num_partitions;
        let shuffle_id = self.shuffle_id;
        let partition_by = self.partition_by.clone();
        let shuffle_dirs = self.shuffle_dirs.clone();
        let compression = self.compression.clone();
        let target_size = self.target_in_memory_size_per_partition;

        spawner
            .spawn(
                async move {
                    let cache = match &state.cache {
                        Some(c) => c.clone(),
                        None => {
                            let c = Arc::new(InProgressShuffleCache::try_new(
                                num_partitions,
                                &shuffle_dirs,
                                state.cache_id.clone(),
                                shuffle_id,
                                target_size,
                                compression.as_deref(),
                            )?);
                            state.cache = Some(c.clone());
                            c
                        }
                    };
                    let partitioned = match &partition_by {
                        Some(partition_by) => {
                            let partition_by = BoundExpr::bind_all(partition_by, &input.schema())?;
                            input.partition_by_hash(&partition_by, num_partitions)?
                        }
                        None => input.partition_by_random(num_partitions, 0)?,
                    };
                    cache
                        .push_partitioned_data(partitioned.into_iter().collect())
                        .await?;
                    Ok(state)
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
    ) -> BlockingSinkFinalizeResult {
        let num_partitions = self.num_partitions;
        let shuffle_id = self.shuffle_id;
        spawner
            .spawn(
                async move {
                    let mut all_rows: Vec<usize> = vec![0; num_partitions];
                    let mut all_bytes: Vec<usize> = vec![0; num_partitions];
                    for state in states {
                        let Some(cache) = state.cache else { continue };
                        let finalized = cache.close().await?;
                        for i in 0..num_partitions {
                            all_rows[i] += finalized.rows_per_partition()[i];
                            all_bytes[i] += finalized.bytes_per_partition()[i];
                        }
                        register_shuffle_cache(shuffle_id, finalized.into()).await?;
                    }
                    let rows_series = Int32Array::from_values(
                        "rows_per_partition",
                        all_rows.into_iter().map(|i| i as i32),
                    )
                    .into_series();
                    let bytes_series = Int32Array::from_values(
                        "bytes_per_partition",
                        all_bytes.into_iter().map(|i| i as i32),
                    )
                    .into_series();
                    let schema = Schema::new(vec![
                        rows_series.field().clone(),
                        bytes_series.field().clone(),
                    ]);
                    let result = RecordBatch::new_with_size(
                        schema.clone(),
                        vec![rows_series, bytes_series],
                        num_partitions,
                    )?;
                    Ok(vec![MicroPartition::new_loaded(
                        schema.into(),
                        Arc::new(vec![result]),
                        None,
                    )])
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "FlightShuffleWrite".into()
    }
    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }
    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "FlightShuffleWrite: {} partitions",
            self.num_partitions
        )]
    }

    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State> {
        Ok(FlightShuffleWriteState {
            cache: None,
            cache_id: input_id.to_string(),
        })
    }
}
