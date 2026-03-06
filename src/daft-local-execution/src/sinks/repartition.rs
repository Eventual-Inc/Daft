use std::{
    fs::File,
    io::BufWriter,
    sync::{Arc, Mutex},
};

use arrow_array::RecordBatch as ArrowRecordBatch;
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::{Schema, SchemaRef};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::NodeName,
    resource_manager::get_or_init_spill_manager,
    spill::{SpillFile, SpillManager},
};

pub(crate) enum RepartitionFinalizeState {
    Building,
    Yielding {
        current_partition: usize,
        /// Yields one accumulated block per `.next()` call.
        /// Wrapped in Mutex to satisfy the Sync bound (only accessed sequentially).
        iter: Mutex<BlockAccumulator>,
    },
}

/// Iterator adapter that accumulates small chunks from an inner iterator
/// and yields one `Arc<MicroPartition>` per call, sized ≥ `target_block_size`.
/// The last yield may be smaller (the leftover tail).
/// Guarantees at least one yield per partition — emits an empty MicroPartition
/// if the inner iterator produces no data.
pub(crate) struct BlockAccumulator {
    inner: Box<dyn Iterator<Item = DaftResult<MicroPartition>> + Send>,
    schema: SchemaRef,
    target_block_size: usize,
    buffer: Vec<RecordBatch>,
    buffer_size: usize,
    emitted: bool,
}

impl BlockAccumulator {
    fn new(
        inner: Box<dyn Iterator<Item = DaftResult<MicroPartition>> + Send>,
        schema: SchemaRef,
        target_block_size: usize,
    ) -> Self {
        Self {
            inner,
            schema,
            target_block_size,
            buffer: Vec::new(),
            buffer_size: 0,
            emitted: false,
        }
    }

    /// Wrap buffered chunks into a MicroPartition and reset the buffer.
    fn flush(&mut self) -> Arc<MicroPartition> {
        self.emitted = true;
        self.buffer_size = 0;
        Arc::new(MicroPartition::new_loaded(
            self.schema.clone(),
            Arc::new(std::mem::take(&mut self.buffer)),
            None,
        ))
    }
}

impl Iterator for BlockAccumulator {
    type Item = DaftResult<Arc<MicroPartition>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next() {
                Some(Ok(part)) => {
                    for chunk in part.record_batches() {
                        self.buffer_size += chunk.size_bytes();
                        self.buffer.push(chunk.clone());
                    }
                    if self.buffer_size >= self.target_block_size {
                        return Some(Ok(self.flush()));
                    }
                }
                Some(Err(e)) => return Some(Err(e)),
                None if !self.buffer.is_empty() => return Some(Ok(self.flush())),
                None if !self.emitted => {
                    self.emitted = true;
                    return Some(Ok(Arc::new(MicroPartition::empty(Some(
                        self.schema.clone(),
                    )))));
                }
                None => return None,
            }
        }
    }
}

/// Index into a single spill file that contains data for all partitions.
/// Each spill event produces one file; `partition_batch_ranges[i]` gives the
/// `(start_batch_idx, num_batches)` for partition `i` within that file.
struct SpillIndex {
    spill_file: SpillFile,
    partition_batch_ranges: Vec<(u32, u32)>,
}

pub(crate) struct RepartitionState {
    // states[i] holds in-memory chunks for partition i
    states: Vec<Vec<MicroPartition>>,
    // Each SpillIndex represents one spill event (one file for all partitions)
    spill_indices: Vec<SpillIndex>,
    spill_manager: Arc<SpillManager>,
    mem_threshold: u64,
    current_mem: u64,
    finalize_state: RepartitionFinalizeState,
}

impl RepartitionState {
    fn push(&mut self, parts: Vec<MicroPartition>) -> DaftResult<()> {
        for (i, part) in parts.into_iter().enumerate() {
            self.current_mem += part.size_bytes() as u64;
            self.states[i].push(part);
        }

        if self.current_mem > self.mem_threshold {
            self.spill()?;
        }
        Ok(())
    }

    fn spill(&mut self) -> DaftResult<()> {
        let num_partitions = self.states.len();
        // Check if there's any data to spill
        let has_data = self.states.iter().any(|s| !s.is_empty());
        if !has_data {
            self.current_mem = 0;
            return Ok(());
        }

        // Get schema from the first non-empty partition
        let schema = self.states.iter().find(|s| !s.is_empty()).unwrap()[0].schema();

        let path = self.spill_manager.get_temp_spill_file();
        let spill_file = SpillFile::new(path.clone());
        let file = File::create(&path)?;
        let buf_writer = BufWriter::new(file);
        let mut writer = FileWriter::try_new(buf_writer, &schema.to_arrow()?)?;

        let mut partition_batch_ranges = Vec::with_capacity(num_partitions);
        let mut batch_idx: u32 = 0;

        for i in 0..num_partitions {
            let start = batch_idx;
            if !self.states[i].is_empty() {
                let chunks = std::mem::take(&mut self.states[i]);
                for mp in chunks {
                    for batch in mp.record_batches() {
                        let arrow_batch: ArrowRecordBatch = batch.clone().try_into()?;
                        writer.write(&arrow_batch)?;
                        batch_idx += 1;
                    }
                }
            }
            partition_batch_ranges.push((start, batch_idx - start));
        }

        writer.finish()?;

        self.spill_indices.push(SpillIndex {
            spill_file,
            partition_batch_ranges,
        });
        self.current_mem = 0;
        Ok(())
    }

    fn emit(
        &mut self,
        partition_idx: usize,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<MicroPartition>> + Send>> {
        let mut iterators: Vec<Box<dyn Iterator<Item = DaftResult<MicroPartition>> + Send>> =
            Vec::new();

        // 1. Spilled files — read only this partition's batch range from each SpillIndex
        for spill_index in &self.spill_indices {
            let (start, num_batches) = spill_index.partition_batch_ranges[partition_idx];
            if num_batches > 0 {
                let file = File::open(spill_index.spill_file.path())?;
                let mut reader = FileReader::try_new(file, None)?;
                reader.set_index(start as usize)?;
                let iter = SpillPartitionReader {
                    reader,
                    remaining: num_batches as usize,
                    cached_schema: None,
                };
                iterators.push(Box::new(iter));
            }
        }

        // 2. In-memory chunks
        if partition_idx < self.states.len() {
            let chunks = std::mem::take(&mut self.states[partition_idx]);
            if !chunks.is_empty() {
                iterators.push(Box::new(chunks.into_iter().map(Ok)));
            }
        }

        Ok(Box::new(iterators.into_iter().flatten()))
    }
}

/// Reads a specific partition's batch range from a spill file.
/// Opens a FileReader, seeks to the start batch, and reads exactly `remaining` batches.
struct SpillPartitionReader {
    reader: FileReader<File>,
    remaining: usize,
    cached_schema: Option<SchemaRef>,
}

impl Iterator for SpillPartitionReader {
    type Item = DaftResult<MicroPartition>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        match self.reader.next() {
            Some(Ok(batch)) => {
                self.remaining -= 1;
                let daft_schema = if let Some(s) = &self.cached_schema {
                    s.clone()
                } else {
                    let schema = self.reader.schema();
                    let s = match Schema::try_from(schema.as_ref()) {
                        Ok(s) => Arc::new(s),
                        Err(e) => return Some(Err(e)),
                    };
                    self.cached_schema = Some(s.clone());
                    s
                };

                match RecordBatch::from_arrow(daft_schema, batch.columns().to_vec()) {
                    Ok(rb) => Some(Ok(MicroPartition::new_loaded(
                        rb.schema.clone(),
                        Arc::new(vec![rb]),
                        None,
                    ))),
                    Err(e) => Some(Err(e)),
                }
            }
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }
}

pub struct RepartitionSink {
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    schema: SchemaRef,
    /// If set, use this as the target block size for finalize output chunking.
    /// If None, falls back to `resolve_memory_limit(memory_limit_bytes)`.
    target_block_size: Option<usize>,
    memory_limit_bytes: Option<usize>,
}

impl RepartitionSink {
    pub fn new(
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        schema: SchemaRef,
        target_block_size: Option<usize>,
        memory_limit_bytes: Option<usize>,
    ) -> Self {
        Self {
            repartition_spec,
            num_partitions,
            schema,
            target_block_size,
            memory_limit_bytes,
        }
    }

    /// Create a merged iterator over all states' data for a given partition.
    /// Drains spill files and in-memory chunks from all states.
    fn create_merged_iter(
        states: &mut [RepartitionState],
        partition_idx: usize,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<MicroPartition>> + Send>> {
        let mut iterators: Vec<Box<dyn Iterator<Item = DaftResult<MicroPartition>> + Send>> =
            Vec::new();
        for state in states.iter_mut() {
            let iter = state.emit(partition_idx)?;
            iterators.push(iter);
        }
        Ok(Box::new(iterators.into_iter().flatten()))
    }
}

/// Partitions the input according to the repartition spec, then pushes results into state.
async fn partition_and_push(
    input: Arc<MicroPartition>,
    mut state: RepartitionState,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    schema: SchemaRef,
) -> DaftResult<RepartitionState> {
    let partitioned = match repartition_spec {
        RepartitionSpec::Hash(config) => {
            let bound_exprs = config
                .by
                .iter()
                .map(|e| BoundExpr::try_new(e.clone(), &schema))
                .collect::<DaftResult<Vec<_>>>()?;
            input.partition_by_hash(&bound_exprs, num_partitions)?
        }
        RepartitionSpec::Random(_) => {
            let seed = 0;
            input.partition_by_random(num_partitions, seed)?
        }
        RepartitionSpec::Range(config) => {
            input.partition_by_range(&config.by, &config.boundaries, &config.descending)?
        }
        RepartitionSpec::IntoPartitions(config) => input.into_partitions(config.num_partitions)?,
    };
    state.push(partitioned)?;
    Ok(state)
}

impl BlockingSink for RepartitionSink {
    type State = RepartitionState;

    #[instrument(skip_all, name = "RepartitionSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let repartition_spec = self.repartition_spec.clone();
        let num_partitions = self.num_partitions;
        let schema = self.schema.clone();
        spawner
            .spawn(
                async move {
                    partition_and_push(input, state, repartition_spec, num_partitions, schema).await
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "RepartitionSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let num_partitions = self.num_partitions;
        let schema = self.schema.clone();
        let target_block_size = self.target_block_size.unwrap_or_else(|| {
            crate::resource_manager::resolve_memory_limit(self.memory_limit_bytes) as usize
        });

        spawner
            .spawn(
                async move {
                    let mut states = states;
                    if states.is_empty() || num_partitions == 0 {
                        return Ok(BlockingSinkFinalizeOutput::Finished(vec![]));
                    }

                    // Restore continuation state, or initialize for partition 0
                    let (mut current_partition, mut iter) = match std::mem::replace(
                        &mut states[0].finalize_state,
                        RepartitionFinalizeState::Building,
                    ) {
                        RepartitionFinalizeState::Building => {
                            let raw = Self::create_merged_iter(&mut states, 0)?;
                            (
                                0,
                                BlockAccumulator::new(raw, schema.clone(), target_block_size),
                            )
                        }
                        RepartitionFinalizeState::Yielding {
                            current_partition,
                            iter: mutex,
                        } => {
                            let acc = mutex.into_inner().unwrap_or_else(|e| e.into_inner());
                            (current_partition, acc)
                        }
                    };

                    // Try to get the next block (may skip empty partitions)
                    loop {
                        if let Some(result) = iter.next() {
                            let block = result?;
                            states[0].finalize_state = RepartitionFinalizeState::Yielding {
                                current_partition,
                                iter: Mutex::new(iter),
                            };
                            return Ok(BlockingSinkFinalizeOutput::HasMoreOutput {
                                states,
                                output: vec![block],
                            });
                        }

                        // Advance to next partition
                        current_partition += 1;
                        if current_partition >= num_partitions {
                            // All partitions done
                            return Ok(BlockingSinkFinalizeOutput::Finished(vec![]));
                        }
                        let raw = Self::create_merged_iter(&mut states, current_partition)?;
                        iter = BlockAccumulator::new(raw, schema.clone(), target_block_size);
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Repartition".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        match &self.repartition_spec {
            RepartitionSpec::Hash(config) => vec![format!(
                "Repartition: By {} into {} partitions",
                config.by.iter().map(|e| e.to_string()).join(", "),
                self.num_partitions
            )],
            RepartitionSpec::Random(_) => vec![format!(
                "Repartition: Random into {} partitions",
                self.num_partitions
            )],
            RepartitionSpec::IntoPartitions(config) => vec![format!(
                "Repartition: Into {} partitions",
                config.num_partitions
            )],
            RepartitionSpec::Range(config) => {
                let pairs = config
                    .by
                    .iter()
                    .zip(config.descending.iter())
                    .map(|(sb, d)| {
                        format!("({}, {})", sb, if *d { "descending" } else { "ascending" })
                    })
                    .join(", ");
                vec![
                    format!("Repartition: Range into {} partitions", self.num_partitions),
                    format!("By: {:?}", pairs),
                ]
            }
        }
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        let spill_manager = get_or_init_spill_manager(None).clone();
        let mem_limit = crate::resource_manager::resolve_memory_limit(self.memory_limit_bytes);
        Ok(RepartitionState {
            states: (0..self.num_partitions).map(|_| vec![]).collect(),
            spill_indices: Vec::new(),
            spill_manager,
            mem_threshold: mem_limit,
            current_mem: 0,
            finalize_state: RepartitionFinalizeState::Building,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_runtime::get_compute_runtime;
    use daft_core::{
        datatypes::{Field, Int64Array},
        prelude::{DataType, Schema},
        series::IntoSeries,
    };
    use daft_logical_plan::partitioning::{
        HashRepartitionConfig, IntoPartitionsConfig, RandomShuffleConfig, RepartitionSpec,
    };
    use daft_micropartition::MicroPartition;
    use daft_recordbatch::RecordBatch;
    use tracing::Span;

    use super::{
        BlockingSink, BlockingSinkFinalizeOutput, RepartitionFinalizeState, RepartitionSink,
        RepartitionState, partition_and_push,
    };
    use crate::{ExecutionTaskSpawner, resource_manager::MemoryManager, spill::SpillManager};

    /// A no-op RuntimeStats impl for tests.
    struct NoopRuntimeStats;
    impl crate::runtime_stats::RuntimeStats for NoopRuntimeStats {
        fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
            self
        }
        fn build_snapshot(
            &self,
            _ordering: std::sync::atomic::Ordering,
        ) -> common_metrics::StatSnapshot {
            common_metrics::StatSnapshot::Source(common_metrics::snapshot::SourceSnapshot {
                rows_out: 0,
                cpu_us: 0,
                bytes_read: 0,
            })
        }
        fn add_rows_in(&self, _rows: u64) {}
        fn add_rows_out(&self, _rows: u64) {}
        fn add_cpu_us(&self, _cpu_us: u64) {}
    }

    fn make_mp(col_name: &str, values: Vec<i64>) -> MicroPartition {
        let schema = Arc::new(Schema::new(vec![Field::new(col_name, DataType::Int64)]));
        let array = Int64Array::from_vec(col_name, values);
        let rb = RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap();
        MicroPartition::new_loaded(schema, Arc::new(vec![rb]), None)
    }

    fn make_state(
        num_partitions: usize,
        mem_threshold: u64,
        dir: &tempfile::TempDir,
    ) -> RepartitionState {
        RepartitionState {
            states: (0..num_partitions).map(|_| vec![]).collect(),
            spill_indices: Vec::new(),
            spill_manager: Arc::new(SpillManager::new(dir.path()).unwrap()),
            mem_threshold,
            current_mem: 0,
            finalize_state: RepartitionFinalizeState::Building,
        }
    }

    fn make_spawner() -> ExecutionTaskSpawner {
        ExecutionTaskSpawner::new(
            get_compute_runtime(),
            Arc::new(MemoryManager::new()),
            Arc::new(NoopRuntimeStats),
            Span::current(),
        )
    }

    fn count_rows_from_emit(state: &mut RepartitionState, partition_idx: usize) -> usize {
        let iter = state.emit(partition_idx).unwrap();
        let mut total = 0;
        for res in iter {
            let mp = res.unwrap();
            total += mp.len();
        }
        total
    }

    /// Collect all i64 values from a partition's emitted data.
    fn collect_values_from_emit(state: &mut RepartitionState, partition_idx: usize) -> Vec<i64> {
        let iter = state.emit(partition_idx).unwrap();
        let mut values = Vec::new();
        for res in iter {
            let mp = res.unwrap();
            for rb in mp.record_batches() {
                let col = rb.get_column(0);
                let arr = col.i64().unwrap();
                values.extend_from_slice(arr.as_slice());
            }
        }
        values
    }

    // ── RepartitionState unit tests ──────────────────────────────────────

    #[test]
    fn test_push_under_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(2, u64::MAX, &dir);

        let parts = vec![make_mp("a", vec![1, 2]), make_mp("a", vec![3, 4])];
        state.push(parts).unwrap();

        assert_eq!(state.states[0].len(), 1);
        assert_eq!(state.states[1].len(), 1);
        assert!(state.spill_indices.is_empty());
        assert!(state.current_mem > 0);
    }

    #[test]
    fn test_push_triggers_spill() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(2, 1, &dir);

        let parts = vec![make_mp("a", vec![1, 2, 3]), make_mp("a", vec![4, 5, 6])];
        state.push(parts).unwrap();

        assert!(state.states[0].is_empty());
        assert!(state.states[1].is_empty());
        assert_eq!(state.spill_indices.len(), 1);
        assert_eq!(state.current_mem, 0);
    }

    #[test]
    fn test_spill_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(3, u64::MAX, &dir);

        state.spill().unwrap();
        assert_eq!(state.current_mem, 0);
        assert!(state.spill_indices.is_empty());
    }

    #[test]
    fn test_spill_clears_memory() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(2, u64::MAX, &dir);

        let parts = vec![make_mp("a", vec![1, 2, 3]), make_mp("a", vec![4, 5])];
        state.push(parts).unwrap();
        assert!(state.current_mem > 0);

        state.spill().unwrap();

        assert_eq!(state.current_mem, 0);
        assert!(state.states[0].is_empty());
        assert!(state.states[1].is_empty());
        assert_eq!(state.spill_indices.len(), 1);
    }

    #[test]
    fn test_emit_in_memory_only() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(2, u64::MAX, &dir);

        let parts = vec![make_mp("a", vec![1, 2, 3]), make_mp("a", vec![4, 5])];
        state.push(parts).unwrap();

        assert_eq!(count_rows_from_emit(&mut state, 0), 3);
        assert_eq!(count_rows_from_emit(&mut state, 1), 2);
    }

    #[test]
    fn test_emit_spilled_only() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(2, u64::MAX, &dir);

        let parts = vec![make_mp("a", vec![10, 20, 30]), make_mp("a", vec![40, 50])];
        state.push(parts).unwrap();
        state.spill().unwrap();

        assert_eq!(count_rows_from_emit(&mut state, 0), 3);
        assert_eq!(count_rows_from_emit(&mut state, 1), 2);
    }

    #[test]
    fn test_emit_combined_spill_and_memory() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(2, u64::MAX, &dir);

        let parts1 = vec![make_mp("a", vec![1, 2]), make_mp("a", vec![3, 4])];
        state.push(parts1).unwrap();
        state.spill().unwrap();

        let parts2 = vec![make_mp("a", vec![5, 6, 7]), make_mp("a", vec![8])];
        state.push(parts2).unwrap();

        assert_eq!(count_rows_from_emit(&mut state, 0), 5);
        assert_eq!(count_rows_from_emit(&mut state, 1), 3);
    }

    #[test]
    fn test_emit_empty_partition() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(3, u64::MAX, &dir);

        // Don't push anything, emit from empty partition
        assert_eq!(count_rows_from_emit(&mut state, 2), 0);
    }

    #[test]
    fn test_single_spill_produces_one_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(2, u64::MAX, &dir);

        let parts = vec![make_mp("a", vec![1, 2, 3]), make_mp("a", vec![4, 5])];
        state.push(parts).unwrap();
        state.spill().unwrap();

        assert_eq!(state.spill_indices.len(), 1);
        // Both partitions' data is in one file
        assert_eq!(count_rows_from_emit(&mut state, 0), 3);
        assert_eq!(count_rows_from_emit(&mut state, 1), 2);
    }

    #[test]
    fn test_multiple_spills_produce_correct_spill_index_count() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(2, u64::MAX, &dir);

        // First spill
        let parts1 = vec![make_mp("a", vec![1, 2]), make_mp("a", vec![3])];
        state.push(parts1).unwrap();
        state.spill().unwrap();

        // Second spill
        let parts2 = vec![make_mp("a", vec![4, 5, 6]), make_mp("a", vec![7, 8])];
        state.push(parts2).unwrap();
        state.spill().unwrap();

        // Third spill
        let parts3 = vec![make_mp("a", vec![9]), make_mp("a", vec![10])];
        state.push(parts3).unwrap();
        state.spill().unwrap();

        assert_eq!(
            state.spill_indices.len(),
            3,
            "3 spills should produce 3 SpillIndices"
        );

        // Verify all data is readable
        let total: usize = (0..2).map(|i| count_rows_from_emit(&mut state, i)).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_empty_partition_emit_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(3, u64::MAX, &dir);

        // Push data into partitions 0 and 1; partition 2 gets an empty MicroPartition
        let parts = vec![
            make_mp("a", vec![1, 2, 3]),
            make_mp("a", vec![4, 5]),
            MicroPartition::empty(Some(Arc::new(Schema::new(vec![Field::new(
                "a",
                DataType::Int64,
            )])))),
        ];
        state.push(parts).unwrap();
        state.spill().unwrap();

        // Partition 2 should emit 0 rows
        assert_eq!(count_rows_from_emit(&mut state, 2), 0);
        // Partitions 0 and 1 should have data
        assert_eq!(count_rows_from_emit(&mut state, 0), 3);
        assert_eq!(count_rows_from_emit(&mut state, 1), 2);
    }

    #[test]
    fn test_spill_index_drop_cleans_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(2, u64::MAX, &dir);

        // Create 3 spill events
        for _ in 0..3 {
            let parts = vec![make_mp("a", vec![1, 2]), make_mp("a", vec![3])];
            state.push(parts).unwrap();
            state.spill().unwrap();
        }

        assert_eq!(state.spill_indices.len(), 3);

        // Verify files exist
        let arrow_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "arrow"))
            .collect();
        assert_eq!(arrow_files.len(), 3, "3 spills should produce 3 files");

        // Drop the state → SpillIndex → SpillFile → files deleted
        drop(state);

        let arrow_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "arrow"))
            .collect();
        assert!(
            arrow_files.is_empty(),
            "spill files should be cleaned up after drop"
        );
    }

    #[test]
    fn test_spill_preserves_per_partition_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(3, u64::MAX, &dir);

        // Push distinguishable data per partition
        let parts = vec![
            make_mp("a", vec![100, 200]),
            make_mp("a", vec![300, 400, 500]),
            make_mp("a", vec![600]),
        ];
        state.push(parts).unwrap();
        state.spill().unwrap();

        // Push a second round and spill again
        let parts2 = vec![
            make_mp("a", vec![101]),
            make_mp("a", vec![301, 401]),
            make_mp("a", vec![601, 701]),
        ];
        state.push(parts2).unwrap();
        state.spill().unwrap();

        // Verify each partition returns exactly its own values (order preserved)
        let mut p0 = collect_values_from_emit(&mut state, 0);
        p0.sort();
        assert_eq!(p0, vec![100, 101, 200]);

        let mut p1 = collect_values_from_emit(&mut state, 1);
        p1.sort();
        assert_eq!(p1, vec![300, 301, 400, 401, 500]);

        let mut p2 = collect_values_from_emit(&mut state, 2);
        p2.sort();
        assert_eq!(p2, vec![600, 601, 701]);
    }

    // ── Finalize integration tests ──────────────────────────────────────

    /// Helper: call finalize and collect all output blocks across iterations.
    /// Handles both `HasMoreOutput` (loop) and `Finished` (terminal).
    async fn run_finalize(
        sink: &RepartitionSink,
        mut states: Vec<RepartitionState>,
        spawner: &ExecutionTaskSpawner,
    ) -> Vec<Arc<MicroPartition>> {
        let mut all_blocks = Vec::new();
        loop {
            let result = sink.finalize(states, spawner).await.unwrap().unwrap();
            match result {
                BlockingSinkFinalizeOutput::HasMoreOutput { states: s, output } => {
                    all_blocks.extend(output);
                    states = s;
                }
                BlockingSinkFinalizeOutput::Finished(output) => {
                    all_blocks.extend(output);
                    break;
                }
            }
        }
        all_blocks
    }

    /// Finalize with empty state should produce a single empty MicroPartition per partition.
    #[tokio::test]
    async fn test_finalize_empty_state() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let sink = RepartitionSink::new(
            RepartitionSpec::Random(RandomShuffleConfig::new(None)),
            2,
            schema,
            Some(1024), // target_block_size doesn't matter for empty
            None,
        );

        let states = vec![make_state(2, u64::MAX, &dir)];
        let spawner = make_spawner();
        let blocks = run_finalize(&sink, states, &spawner).await;

        // 2 partitions, each should produce at least 1 block
        assert_eq!(blocks.len(), 2);
        for block in &blocks {
            assert_eq!(block.len(), 0);
        }
    }

    /// Finalize with data smaller than target_block_size should produce one block per partition.
    #[tokio::test]
    async fn test_finalize_small_data_single_block() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let mut state = make_state(1, u64::MAX, &dir);

        // Push 5 rows into partition 0
        state.states[0].push(make_mp("a", vec![1, 2, 3, 4, 5]));

        let sink = RepartitionSink::new(
            RepartitionSpec::Random(RandomShuffleConfig::new(None)),
            1,
            schema,
            Some(1024 * 1024), // 1MB target — way larger than our tiny data
            None,
        );

        let spawner = make_spawner();
        let blocks = run_finalize(&sink, vec![state], &spawner).await;

        // Single partition, data fits in one block
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].len(), 5);
    }

    /// Finalize with data exceeding target_block_size should split into multiple blocks.
    #[tokio::test]
    async fn test_finalize_chunks_to_target_block_size() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let mut state = make_state(1, u64::MAX, &dir);

        // Push multiple small chunks into partition 0
        for _ in 0..10 {
            state.states[0].push(make_mp("a", vec![1; 100]));
        }

        // Measure the size of one 100-row chunk to pick a target
        let sample = make_mp("a", vec![1; 100]);
        let chunk_size = sample.size_bytes();

        // Set target_block_size to ~2 chunks worth, so we expect ~5 output blocks
        let target = chunk_size * 2;

        let sink = RepartitionSink::new(
            RepartitionSpec::Random(RandomShuffleConfig::new(None)),
            1,
            schema,
            Some(target),
            None,
        );

        let spawner = make_spawner();
        let blocks = run_finalize(&sink, vec![state], &spawner).await;

        // Should have multiple blocks (roughly 5, but depends on size accounting)
        assert!(
            blocks.len() > 1,
            "Expected multiple blocks but got {}",
            blocks.len()
        );

        // Total rows must be preserved
        let total_rows: usize = blocks.iter().map(|b| b.len()).sum();
        assert_eq!(total_rows, 1000);
    }

    /// Finalize with a huge single chunk (>target) emits it as one block.
    #[tokio::test]
    async fn test_finalize_huge_chunk_no_slicing() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let mut state = make_state(1, u64::MAX, &dir);

        // Push one large chunk: 1000 rows
        state.states[0].push(make_mp("a", vec![42; 1000]));

        // Measure the size
        let big = make_mp("a", vec![42; 1000]);
        let big_size = big.size_bytes();

        // Set target to ~1/4 of the big chunk
        let target = big_size / 4;

        let sink = RepartitionSink::new(
            RepartitionSpec::Random(RandomShuffleConfig::new(None)),
            1,
            schema,
            Some(target),
            None,
        );

        let spawner = make_spawner();
        let blocks = run_finalize(&sink, vec![state], &spawner).await;

        // The large chunk exceeds target on first read, so it's emitted as one block.
        // No slicing is performed — downstream handles oversized blocks.
        let total_rows: usize = blocks.iter().map(|b| b.len()).sum();
        assert_eq!(total_rows, 1000);
    }

    /// Finalize with multiple partitions produces HasMoreOutput for each partition.
    #[tokio::test]
    async fn test_finalize_multi_partition_iteration() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let mut state = make_state(3, u64::MAX, &dir);

        // Push data into each partition
        state.states[0].push(make_mp("a", vec![1, 2, 3]));
        state.states[1].push(make_mp("a", vec![4, 5]));
        state.states[2].push(make_mp("a", vec![6]));

        let sink = RepartitionSink::new(
            RepartitionSpec::Random(RandomShuffleConfig::new(None)),
            3,
            schema,
            Some(1024 * 1024), // large target so each partition → 1 block
            None,
        );

        let spawner = make_spawner();
        let blocks = run_finalize(&sink, vec![state], &spawner).await;

        // 3 partitions, each producing 1 block
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].len(), 3);
        assert_eq!(blocks[1].len(), 2);
        assert_eq!(blocks[2].len(), 1);
    }

    /// Finalize with None target_block_size falls back to resolve_memory_limit.
    /// With an explicit memory_limit_bytes of 1MB, target = 1MB.
    #[tokio::test]
    async fn test_finalize_memory_limit_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let mut state = make_state(1, u64::MAX, &dir);

        // Push data that should fit in a single block when memory_limit is generous
        state.states[0].push(make_mp("a", vec![1; 10]));

        let sink = RepartitionSink::new(
            RepartitionSpec::Random(RandomShuffleConfig::new(None)),
            1,
            schema,
            None,                  // no explicit target
            Some(1024 * 1024 * 4), // 4MB memory limit → target = 4MB
        );

        let spawner = make_spawner();
        let blocks = run_finalize(&sink, vec![state], &spawner).await;

        // 10 rows of Int64 is tiny (<1KB), should be a single block
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].len(), 10);
    }

    /// Finalize with spilled data should read back and chunk correctly.
    #[tokio::test]
    async fn test_finalize_with_spilled_data() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let mut state = make_state(1, u64::MAX, &dir);

        // Push data and force spill
        state.states[0].push(make_mp("a", vec![1; 500]));
        state.spill().unwrap();
        state.states[0].push(make_mp("a", vec![2; 500]));

        let sink = RepartitionSink::new(
            RepartitionSpec::Random(RandomShuffleConfig::new(None)),
            1,
            schema,
            Some(1024 * 1024), // large target
            None,
        );

        let spawner = make_spawner();
        let blocks = run_finalize(&sink, vec![state], &spawner).await;

        // All 1000 rows should be present
        let total_rows: usize = blocks.iter().map(|b| b.len()).sum();
        assert_eq!(total_rows, 1000);
    }

    /// Finalize with multiple states (simulating multi-worker) merges all data.
    #[tokio::test]
    async fn test_finalize_multiple_states() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));

        let mut state1 = make_state(1, u64::MAX, &dir1);
        state1.states[0].push(make_mp("a", vec![1; 300]));

        let mut state2 = make_state(1, u64::MAX, &dir2);
        state2.states[0].push(make_mp("a", vec![2; 200]));

        let sink = RepartitionSink::new(
            RepartitionSpec::Random(RandomShuffleConfig::new(None)),
            1,
            schema,
            Some(1024 * 1024), // large target
            None,
        );

        let spawner = make_spawner();
        let blocks = run_finalize(&sink, vec![state1, state2], &spawner).await;

        // Both states' data merged: 300 + 200 = 500 rows
        let total_rows: usize = blocks.iter().map(|b| b.len()).sum();
        assert_eq!(total_rows, 500);
    }

    #[tokio::test]
    async fn test_finalize_per_block_streaming() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let mut state = make_state(1, u64::MAX, &dir);

        // Push 5 chunks into a single partition.
        for i in 0..5 {
            state.states[0].push(make_mp("a", vec![i; 100]));
        }

        // Measure chunk size to set target = 1 chunk.
        let sample = make_mp("a", vec![1; 100]);
        let chunk_size = sample.size_bytes();
        let target = chunk_size; // target = 1 chunk → each finalize call should emit ~1 block

        let sink = RepartitionSink::new(
            RepartitionSpec::Random(RandomShuffleConfig::new(None)),
            1,
            schema,
            Some(target),
            None,
        );

        let spawner = make_spawner();
        let mut states = vec![state];
        let mut call_count = 0;
        let mut total_rows = 0;

        loop {
            let result = sink.finalize(states, &spawner).await.unwrap().unwrap();
            call_count += 1;
            match result {
                BlockingSinkFinalizeOutput::HasMoreOutput { states: s, output } => {
                    assert_eq!(
                        output.len(),
                        1,
                        "Call {} returned {} blocks, expected 1",
                        call_count,
                        output.len()
                    );
                    total_rows += output[0].len();
                    states = s;
                }
                BlockingSinkFinalizeOutput::Finished(output) => {
                    // Last partition flush: may contain 1 block (leftover or empty)
                    for block in &output {
                        total_rows += block.len();
                    }
                    break;
                }
            }
        }

        // All 500 rows must be accounted for.
        assert_eq!(total_rows, 500);
        // Should have taken multiple calls (not all at once).
        assert!(
            call_count >= 3,
            "Expected >= 3 finalize calls for 5 chunks with target=1 chunk, got {}",
            call_count
        );
    }

    // ========== PARTITION_AND_PUSH ==========

    #[test]
    fn test_partition_and_push_random_sync() {
        // Test that random partitioning distributes rows correctly
        let dir = tempfile::tempdir().unwrap();
        let mut state = make_state(2, u64::MAX, &dir);

        // Partition 100 rows into 2 partitions using round-robin targets
        // (simulates random partitioning)
        let targets: Vec<u64> = (0..100).map(|i| i % 2).collect();
        let targets_arr = daft_core::datatypes::UInt64Array::from_vec("idx", targets);
        let parts = daft_micropartition::MicroPartition::new_loaded(
            state.states[0]
                .first()
                .map(|_| unreachable!())
                .unwrap_or_else(|| {
                    Arc::new(daft_core::prelude::Schema::new(vec![
                        daft_core::datatypes::Field::new("a", DataType::Int64),
                    ]))
                }),
            Arc::new(vec![{
                let array = daft_core::datatypes::Int64Array::from_vec("a", (0..100).collect());
                daft_recordbatch::RecordBatch::from_nonempty_columns(vec![array.into_series()])
                    .unwrap()
            }]),
            None,
        )
        .partition_by_index(&targets_arr, 2)
        .unwrap();

        let mp_parts: Vec<_> = parts.into_iter().map(|p| p).collect();
        state.push(mp_parts).unwrap();

        let total = count_rows_from_emit(&mut state, 0) + count_rows_from_emit(&mut state, 1);
        assert_eq!(total, 100, "total rows after push should be 100");
    }

    #[test]
    fn test_repartition_sink_name_and_display() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let sink = RepartitionSink::new(
            RepartitionSpec::Random(RandomShuffleConfig::new(None)),
            2,
            schema,
            None,
            None,
        );
        assert_eq!(
            <RepartitionSink as BlockingSink>::name(&sink).as_ref(),
            "Repartition"
        );
        let display = <RepartitionSink as BlockingSink>::multiline_display(&sink);
        assert!(!display.is_empty(), "multiline_display should not be empty");
    }

    #[tokio::test]
    async fn test_partition_and_push_hash() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let state = make_state(2, u64::MAX, &dir);
        let mp = Arc::new(make_mp("a", (0..10).collect()));
        let spec = RepartitionSpec::Hash(HashRepartitionConfig::new(
            Some(2),
            vec![daft_dsl::resolved_col("a").into()],
        ));
        let mut result_state = partition_and_push(mp, state, spec, 2, schema)
            .await
            .unwrap();
        let total =
            count_rows_from_emit(&mut result_state, 0) + count_rows_from_emit(&mut result_state, 1);
        assert_eq!(
            total, 10,
            "hash partition: all 10 rows distributed across 2 partitions"
        );
    }

    #[tokio::test]
    async fn test_partition_and_push_into_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let state = make_state(3, u64::MAX, &dir);
        let mp = Arc::new(make_mp("a", (0..6).collect()));
        let spec = RepartitionSpec::IntoPartitions(IntoPartitionsConfig::new(3));
        let mut result_state = partition_and_push(mp, state, spec, 3, schema)
            .await
            .unwrap();
        let total = count_rows_from_emit(&mut result_state, 0)
            + count_rows_from_emit(&mut result_state, 1)
            + count_rows_from_emit(&mut result_state, 2);
        assert_eq!(
            total, 6,
            "into_partitions: all 6 rows distributed across 3 partitions"
        );
    }

    #[tokio::test]
    async fn test_partition_and_push_range() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let state = make_state(2, u64::MAX, &dir);
        let mp = Arc::new(make_mp("a", vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        // Boundary at 5: partition 0 gets [1..5], partition 1 gets [6..10]
        let boundary_array = Int64Array::from_vec("a", vec![5]);
        let boundary_rb =
            RecordBatch::from_nonempty_columns(vec![boundary_array.into_series()]).unwrap();
        let bound_expr =
            daft_dsl::expr::bound_expr::BoundExpr::try_new(daft_dsl::resolved_col("a"), &schema)
                .unwrap();
        let spec = RepartitionSpec::Range(
            daft_logical_plan::partitioning::RangeRepartitionConfig::new(
                Some(2),
                boundary_rb,
                vec![bound_expr],
                vec![false],
            ),
        );
        let mut result_state = partition_and_push(mp, state, spec, 2, schema)
            .await
            .unwrap();
        let total =
            count_rows_from_emit(&mut result_state, 0) + count_rows_from_emit(&mut result_state, 1);
        assert_eq!(
            total, 10,
            "range partition: all 10 rows distributed across 2 partitions"
        );
    }
}
