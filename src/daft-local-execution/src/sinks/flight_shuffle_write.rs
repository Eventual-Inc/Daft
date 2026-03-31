use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr};
use daft_micropartition::MicroPartition;
use daft_shuffles::{
    server::flight_server::ShuffleFlightServer, shuffle_cache::InProgressShuffleCache,
};
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    shuffle_metadata::{ShuffleMetadata, ShufflePartitionMetadata},
};

pub(crate) struct FlightShuffleWriteState {
    cache: Arc<InProgressShuffleCache>,
}

pub struct FlightShuffleWriteSink {
    num_partitions: usize,
    shuffle_id: u64,
    partition_by: Option<Vec<ExprRef>>,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
    target_in_memory_size_per_partition: usize,
    local_server: Arc<ShuffleFlightServer>,
    // We want num_compute_thread many instances of this operator, but only 1 instance of cache per task / input_id
    // To work around this, we save and reuse caches as states per input_id
    caches: Mutex<HashMap<InputId, Arc<InProgressShuffleCache>>>,
}

impl FlightShuffleWriteSink {
    pub fn try_new(
        num_partitions: usize,
        partition_by: Option<Vec<ExprRef>>,
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        local_server: Arc<ShuffleFlightServer>,
    ) -> DaftResult<Self> {
        const TARGET_TOTAL_IN_MEMORY_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 2; // 2GB
        Ok(Self {
            num_partitions,
            shuffle_id,
            partition_by,
            shuffle_dirs,
            compression,
            target_in_memory_size_per_partition: (TARGET_TOTAL_IN_MEMORY_SIZE_BYTES
                / num_partitions)
                .clamp(1024 * 1024 * 8, 1024 * 1024 * 128),
            local_server,
            caches: Mutex::new(HashMap::new()),
        })
    }
}

impl BlockingSink for FlightShuffleWriteSink {
    type State = FlightShuffleWriteState;

    #[instrument(skip_all, name = "FlightShuffleWriteSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let num_partitions = self.num_partitions;
        let partition_by = self.partition_by.clone();

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
                    state
                        .cache
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
        let shuffle_id = self.shuffle_id;
        let local_server = self.local_server.clone();

        spawner
            .spawn(
                async move {
                    // All states share the same cache — just take the first one.
                    let cache = states.into_iter().next().unwrap().cache;
                    let finalized = cache.close().await?;
                    let all_rows = finalized.rows_per_partition();
                    let all_bytes = finalized.bytes_per_partition();
                    local_server
                        .register_shuffle_cache(shuffle_id, finalized.into())
                        .await?;
                    let rows_series = Int32Array::from_values(
                        "rows_per_partition",
                        all_rows.iter().map(|&i| i as i32),
                    )
                    .into_series();
                    let bytes_series = Int32Array::from_values(
                        "bytes_per_partition",
                        all_bytes.iter().map(|&i| i as i32),
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
        let mut caches = self.caches.lock().unwrap();
        let cache = match caches.entry(input_id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let c = Arc::new(InProgressShuffleCache::try_new(
                    self.num_partitions,
                    &self.shuffle_dirs,
                    input_id.to_string(),
                    self.shuffle_id,
                    self.target_in_memory_size_per_partition,
                    self.compression.as_deref(),
                )?);
                e.insert(c.clone());
                c
            }
        };
        Ok(FlightShuffleWriteState { cache })
    }
}
