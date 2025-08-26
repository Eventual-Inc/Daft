use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use common_error::DaftResult;
use common_metrics::{snapshot, Stat, StatSnapshotSend};
use daft_core::prelude::SchemaRef;
use daft_dsl::{expr::bound_expr::BoundExpr, ExprRef};
use daft_micropartition::MicroPartition;
use daft_shuffles::shuffle_cache::InProgressShuffleCache;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{
    ops::NodeType,
    pipeline::NodeName,
    runtime_stats::{RuntimeStats, CPU_US_KEY, ROWS_EMITTED_KEY, ROWS_RECEIVED_KEY},
    ExecutionTaskSpawner,
};

#[derive(Default)]
struct FlightShuffleWriteStats {
    cpu_us: AtomicU64,
    rows_received: AtomicU64,
    rows_emitted: AtomicU64,
    bytes_written: AtomicU64,
}

impl RuntimeStats for FlightShuffleWriteStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshotSend {
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(self.cpu_us.load(ordering))),
            ROWS_RECEIVED_KEY; Stat::Count(self.rows_received.load(ordering)),
            ROWS_EMITTED_KEY; Stat::Count(self.rows_emitted.load(ordering)),
            "bytes written"; Stat::Bytes(self.bytes_written.load(ordering)),
        ]
    }

    fn add_rows_received(&self, rows: u64) {
        self.rows_received.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_rows_emitted(&self, rows: u64) {
        self.rows_emitted.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.fetch_add(cpu_us, Ordering::Relaxed);
    }
}

pub(crate) struct FlightShuffleWriteState {
    shuffle_cache: Arc<InProgressShuffleCache>,
}

impl FlightShuffleWriteState {
    pub fn new(shuffle_cache: Arc<InProgressShuffleCache>) -> Self {
        Self { shuffle_cache }
    }
}

pub(crate) struct FlightShuffleWriteSink {
    shuffle_id: String,
    num_partitions: usize,
    dirs: Vec<String>,
    node_id: String,
    shuffle_stage_id: usize,
    target_filesize: usize,
    compression: Option<String>,
    partition_by: Option<Vec<ExprRef>>,
    schema: SchemaRef,
}

impl FlightShuffleWriteSink {
    pub(crate) fn new(
        shuffle_id: String,
        num_partitions: usize,
        dirs: Vec<String>,
        node_id: String,
        shuffle_stage_id: usize,
        target_filesize: usize,
        compression: Option<String>,
        partition_by: Option<Vec<ExprRef>>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            shuffle_id,
            num_partitions,
            dirs,
            node_id,
            shuffle_stage_id,
            target_filesize,
            compression,
            partition_by,
            schema,
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
        let shuffle_cache = state.shuffle_cache.clone();

        spawner
            .spawn(
                async move {
                    shuffle_cache.push_partitions(vec![input]).await?;
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
        let schema = self.schema.clone();
        let shuffle_id = self.shuffle_id.clone();

        spawner
            .spawn(
                async move {
                    let mut all_shuffle_caches = vec![];

                    // Close all shuffle caches and collect their results
                    for state in states {
                        let shuffle_cache_result = state.shuffle_cache.close().await?;
                        all_shuffle_caches.push(shuffle_cache_result);
                    }

                    // Register all shuffle caches with the static flight server
                    for shuffle_cache in all_shuffle_caches {
                        if let Err(e) = daft_shuffles::globals::add_shuffle_cache_to_server(
                            shuffle_id.clone(),
                            Arc::new(shuffle_cache)
                        ).await {
                            return Err(e);
                        }
                    }

                    // Create a MicroPartition containing shuffle metadata/results
                    // This can be used by the distributed pipeline to coordinate the flight reads
                    let mp = Arc::new(MicroPartition::empty(Some(schema)));

                    Ok(BlockingSinkFinalizeOutput::Finished(vec![mp]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "FlightShuffle Write".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Write
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        let shuffle_cache = InProgressShuffleCache::try_new(
            self.num_partitions,
            &self.dirs,
            self.node_id.clone(),
            self.shuffle_stage_id,
            self.target_filesize,
            self.compression.as_deref(),
            self.partition_by.clone(),
        )?;

        Ok(FlightShuffleWriteState::new(Arc::new(shuffle_cache)))
    }

    fn make_runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        Arc::new(FlightShuffleWriteStats::default())
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![];
        lines.push(format!(
            "FlightShuffle Write: {} partitions",
            self.num_partitions
        ));
        if let Some(partition_by) = &self.partition_by {
            lines.push(format!("Partition by: {:?}", partition_by));
        }
        lines.push(format!("Node ID: {}", self.node_id));
        lines.push(format!("Shuffle stage ID: {}", self.shuffle_stage_id));
        lines
    }

    fn max_concurrency(&self) -> usize {
        1 // Single shuffle cache per node
    }
}
