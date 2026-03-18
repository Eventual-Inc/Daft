use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{
    prelude::{Int32Array, Schema, SchemaRef},
    series::IntoSeries,
};
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{
    server::flight_server::ShuffleFlightServer, shuffle_cache::{InMemoryShuffleCache, InProgressShuffleCache},
};
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

pub struct InMemoryData {
    data: Vec<Vec<RecordBatch>>,
}

impl InMemoryData {
    pub fn append(&mut self, input: Vec<MicroPartition>) {
        for (partition, input) in self.data.iter_mut().zip(input.into_iter()) {
            let record_batches = input.record_batches();
            for record_batch in record_batches {
                partition.push(record_batch.clone());
            }
        }
    }
}

pub struct FlightShuffleWriteSink {
    num_partitions: usize,
    shuffle_id: u64,
    cache_id: String,
    partition_by: Option<Vec<ExprRef>>,
    // Shared shuffle cache
    shuffle_cache: Arc<InProgressShuffleCache>,
    // Flight shuffle server registry for this node (if enabled)
    shuffle_server: Arc<ShuffleFlightServer>,
    schema: SchemaRef,
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
        schema: SchemaRef,
    ) -> DaftResult<Self> {
        const TARGET_TOTAL_IN_MEMORY_SIZE_BYTES: usize = 1024 * 1024 * 2000; // 2000MB = ~2GB

        let shuffle_cache = InProgressShuffleCache::try_new(
            num_partitions,
            &shuffle_dirs,
            cache_id.clone(),
            shuffle_id,
            (TARGET_TOTAL_IN_MEMORY_SIZE_BYTES / num_partitions)
                .clamp(1024 * 1024 * 8, 1024 * 1024 * 128), // Min 8MB, Max 128MB
            compression.as_deref(),
        )?;

        Ok(Self {
            num_partitions,
            shuffle_id,
            cache_id,
            partition_by,
            shuffle_cache: Arc::new(shuffle_cache),
            shuffle_server,
            schema,
        })
    }
}

impl BlockingSink for FlightShuffleWriteSink {
    type State = InMemoryData;

    #[instrument(skip_all, name = "FlightShuffleWriteSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
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

                    state.append(partitioned);
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
    ) -> BlockingSinkFinalizeResult<Self> {
        let num_partitions = self.num_partitions;
        let shuffle_id = self.shuffle_id;
        let shuffle_server = self.shuffle_server.clone();
        let schema = self.schema.clone();
        let cache_id = self.cache_id.clone();
        
        spawner
            .spawn(
                async move {
                    let mut final_partitions = vec![vec![]; num_partitions];
                    for state in states {
                        for (partition, data) in state.data.iter().zip(final_partitions.iter_mut()) {
                            data.extend(partition.clone());
                        }
                    }

                    let final_partitions = final_partitions.into_iter().map(|p| Arc::new(MicroPartition::new_loaded(schema.clone(), Arc::new(p), None))).collect();
                    let in_memory_cache = InMemoryShuffleCache::new(schema, final_partitions, cache_id.clone());

                    // Register the shuffle cache with the flight server
                    shuffle_server
                        .register_shuffle_cache(shuffle_id, Arc::new(in_memory_cache))
                        .await?;

                    let rows_per_partition = Int32Array::from_values(
                        "rows_per_partition",
                        vec![],
                    )
                    .into_series();
                    let bytes_per_partition = Int32Array::from_values(
                        "bytes_per_partition",
                        vec![],
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
        Ok(InMemoryData { data: vec![vec![]; self.num_partitions] })
    }
}
