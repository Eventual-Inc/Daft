use std::{
    cmp::Ordering,
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, OnceLock},
};

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_core::{
    array::ops::build_multi_array_bicompare,
    prelude::{SchemaRef, UInt64Array},
    series::Series,
};
use daft_dsl::{Column, Expr, expr::bound_expr::BoundExpr};
use daft_memory::MemoryPermit;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use tracing::{Span, instrument};

mod output;
mod run_builder;
use run_builder::RunBuilder;

use crate::memory_size::{SeriesSize, batch_bytes, partition_bytes};

#[cfg(test)]
mod tests;

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkReleaseResult,
    BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    spilling::{SpillBatchReader, SpillFile, SpillScopeId, SpillStreamWriter},
};

pub(crate) struct MemoryRun {
    partition: MicroPartition,
    memory: MemoryPermit,
    max_row_working_bytes: OnceLock<u64>,
}

pub(crate) struct SpillRun {
    files: VecDeque<SpillFile>,
    read_bytes: u64,
    max_row_working_bytes: u64,
}

impl MemoryRun {
    fn max_row_working_bytes(&self) -> DaftResult<u64> {
        if let Some(bytes) = self.max_row_working_bytes.get() {
            return Ok(*bytes);
        }
        let mut largest = 0;
        for batch in self.partition.record_batches() {
            let columns = batch
                .as_materialized_series()
                .into_iter()
                .map(SeriesSize::new)
                .collect::<DaftResult<Vec<_>>>()?;
            let size = RowWorkingSize::new(&columns);
            if !batch.is_empty() && size.variable.is_empty() {
                largest = largest.max(size.fixed);
            } else {
                for row in 0..batch.len() {
                    largest = largest.max(size.bytes(&columns, row)?);
                }
            }
        }
        let _ = self.max_row_working_bytes.set(largest);
        Ok(largest)
    }
}

/// Cache fixed-width costs once per input frame. Only variable-width columns
/// need to be inspected while selecting rows for a merge output batch.
#[derive(Default)]
struct RowWorkingSize {
    fixed: u64,
    variable: Vec<usize>,
}

impl RowWorkingSize {
    fn new(columns: &[SeriesSize]) -> Self {
        // Keep the same conservative materialization and nested-take bound used
        // for merge admission. It also covers the batch selection indices.
        let mut fixed = 264_u64.saturating_add((columns.len() as u64).saturating_mul(128));
        let mut variable = Vec::new();
        for (index, column) in columns.iter().enumerate() {
            match column.constant_row_bytes() {
                Some(bytes) => fixed = fixed.saturating_add(bytes),
                None => variable.push(index),
            }
        }
        Self {
            fixed: fixed.saturating_mul(3),
            variable,
        }
    }

    fn bytes(&self, columns: &[SeriesSize], row: usize) -> DaftResult<u64> {
        self.variable.iter().try_fold(self.fixed, |bytes, &index| {
            let column = &columns[index];
            Ok(bytes
                .saturating_add(column.bytes(row, 1)?.saturating_mul(3))
                .saturating_add(column.take_workspace_bytes(row, 1)?))
        })
    }
}

pub(crate) enum SortState {
    Building {
        pending: RunBuilder,
        memory_runs: Vec<MemoryRun>,
        spill_runs: Vec<SpillRun>,
        schema: Option<SchemaRef>,
    },
    Done,
}

impl SortState {
    fn building_mut(
        &mut self,
    ) -> (
        &mut Vec<MemoryRun>,
        &mut Vec<SpillRun>,
        &mut Option<SchemaRef>,
    ) {
        match self {
            Self::Building {
                memory_runs,
                spill_runs,
                schema,
                ..
            } => (memory_runs, spill_runs, schema),
            Self::Done => panic!("SortSink should be in Building state"),
        }
    }

    fn pending(&mut self) -> &mut RunBuilder {
        match self {
            Self::Building { pending, .. } => pending,
            Self::Done => panic!("SortSink should be in Building state"),
        }
    }

    fn flush_pending(&mut self, params: &SortParams) -> DaftResult<()> {
        let run = std::mem::take(self.pending()).finish(params)?;
        if let Some(run) = run {
            self.building_mut().0.push(run);
        }
        Ok(())
    }

    fn reclaim_bytes(&self) -> u64 {
        match self {
            Self::Building {
                pending,
                memory_runs,
                ..
            } => memory_runs
                .iter()
                .fold(pending.reclaim_bytes(), |bytes, run| {
                    bytes.saturating_add(run.memory.bytes())
                }),
            Self::Done => 0,
        }
    }

    fn finish(
        &mut self,
        params: &SortParams,
    ) -> DaftResult<(Vec<MemoryRun>, Vec<SpillRun>, Option<SchemaRef>)> {
        self.flush_pending(params)?;
        Ok(match std::mem::replace(self, Self::Done) {
            Self::Building {
                memory_runs,
                spill_runs,
                schema,
                ..
            } => (memory_runs, spill_runs, schema),
            Self::Done => panic!("SortSink should be in Building state"),
        })
    }
}

struct SortParams {
    sort_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
}

impl SortParams {
    fn comparator_pair_bytes(&self, schema: &SchemaRef) -> DaftResult<u64> {
        self.sort_by.iter().try_fold(256_u64, |bytes, key| {
            // Field conversion preserves the storage layout of logical types
            // (e.g. UUID and embedding), as Series::to_arrow does for comparison.
            let field = key.inner().to_field(schema)?.to_arrow()?;
            Ok(bytes.saturating_add(comparator_type_bytes(field.data_type())))
        })
    }
}

fn comparator_type_bytes(dtype: &arrow_schema::DataType) -> u64 {
    use arrow_schema::DataType;

    // Each type node can own a boxed closure, two nullable array/buffer views
    // and a Vec entry. Nested comparators recursively own their child closures.
    // This includes the nullable float path, which captures entire arrays.
    let children = match dtype {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => comparator_type_bytes(field.data_type()),
        DataType::Struct(fields) => fields.iter().fold(0_u64, |bytes, field| {
            bytes.saturating_add(comparator_type_bytes(field.data_type()))
        }),
        DataType::Union(fields, _) => fields.iter().fold(0_u64, |bytes, (_, field)| {
            bytes.saturating_add(comparator_type_bytes(field.data_type()))
        }),
        DataType::Dictionary(_, value) => comparator_type_bytes(value),
        _ => 0,
    };
    512_u64.saturating_add(children)
}

fn comparator_matrix_bytes(run_count: usize, pair_bytes: u64) -> u64 {
    if run_count <= 1 {
        return 0;
    }
    let count = run_count as u64;
    count
        .saturating_pow(2)
        .saturating_mul(pair_bytes)
        .saturating_add(count.saturating_mul(64))
}

pub(crate) fn key_allocates(expr: &Expr) -> bool {
    match expr {
        Expr::Column(Column::Bound(_)) => false,
        Expr::Alias(child, _) => key_allocates(child.as_ref()),
        _ => true,
    }
}
pub struct SortSink {
    params: Arc<SortParams>,
}

impl SortSink {
    /// Computed keys must be projected into input columns by the pipeline builder.
    pub fn new(sort_by: Vec<BoundExpr>, descending: Vec<bool>, nulls_first: Vec<bool>) -> Self {
        debug_assert!(sort_by.iter().all(|key| !key_allocates(key.inner())));
        Self {
            params: Arc::new(SortParams {
                sort_by,
                descending,
                nulls_first,
            }),
        }
    }
}

async fn spill_runs(
    memory_runs: &mut Vec<MemoryRun>,
    spill_files: &mut Vec<SpillRun>,
    target: u64,
    scope: &SpillScopeId,
    spawner: &ExecutionTaskSpawner,
) -> DaftResult<u64> {
    let mut released = 0;
    while released < target {
        let Some(run) = memory_runs.pop() else { break };
        // Retain the original run until the new file has been committed successfully.
        let spilled = write_memory_run(&run, scope, spawner).await?;
        released += run.memory.bytes();
        spill_files.push(spilled);
    }
    Ok(released)
}

enum RunSource {
    Memory(Option<MemoryRun>),
    Spill {
        reader: Option<SpillBatchReader>,
        files: VecDeque<SpillFile>,
    },
}

struct MergeCursor {
    source: RunSource,
    batch: Option<RecordBatch>,
    memory: Option<MemoryPermit>,
    keys: Option<Vec<Series>>,
    arrays: Vec<SeriesSize>,
    row_size: RowWorkingSize,
    row: usize,
}

impl MergeCursor {
    fn memory(run: MemoryRun) -> Self {
        Self {
            source: RunSource::Memory(Some(run)),
            batch: None,
            memory: None,
            keys: None,
            arrays: Vec::new(),
            row_size: RowWorkingSize::default(),
            row: 0,
        }
    }

    fn spill(run: SpillRun) -> Self {
        Self {
            source: RunSource::Spill {
                reader: None,
                files: run.files,
            },
            batch: None,
            memory: None,
            keys: None,
            arrays: Vec::new(),
            row_size: RowWorkingSize::default(),
            row: 0,
        }
    }

    async fn load_next(
        &mut self,
        params: &SortParams,
        spawner: &ExecutionTaskSpawner,
    ) -> DaftResult<bool> {
        self.batch = None;
        self.memory = None;
        self.keys = None;
        self.arrays.clear();
        self.row = 0;
        let next = match &mut self.source {
            RunSource::Memory(run) => run.take().and_then(|run| {
                run.partition
                    .record_batches()
                    .first()
                    .cloned()
                    .map(|batch| (batch, run.memory))
            }),
            RunSource::Spill { reader, files } => loop {
                if reader.is_none() {
                    let Some(file) = files.pop_front() else {
                        return Ok(false);
                    };
                    *reader = Some(
                        spawner
                            .spill_manager
                            .open_micropartitions(&file, |bytes| {
                                reserve_merge_memory(spawner, bytes)
                            })
                            .await
                            .map_err(|error| DaftError::ComputeError(error.to_string()))?,
                    );
                }
                let current = reader.take().unwrap();
                if let Some((batch, memory, next_reader)) = current
                    .next_batch(|bytes| reserve_merge_memory(spawner, bytes))
                    .await
                    .map_err(|error| DaftError::ComputeError(error.to_string()))?
                {
                    *reader = Some(next_reader);
                    break Some((batch, memory));
                }
            },
        };
        let Some((batch, memory)) = next else {
            return Ok(false);
        };
        if batch.is_empty() {
            return Ok(false);
        }
        let keys = evaluate_keys(&batch, params)?;
        self.arrays = batch
            .as_materialized_series()
            .into_iter()
            .map(SeriesSize::new)
            .collect::<DaftResult<Vec<_>>>()?;
        self.batch = Some(batch);
        self.row_size = RowWorkingSize::new(&self.arrays);
        self.memory = Some(memory);
        self.keys = Some(keys);
        Ok(true)
    }

    fn exhausted(&self) -> bool {
        self.batch
            .as_ref()
            .is_none_or(|batch| self.row == batch.len())
    }

    fn row_working_bytes(&self) -> DaftResult<u64> {
        // Include slice descriptors and indices as well as copied Arrow data. This also
        // bounds batches made entirely of nulls or other zero-payload values.
        self.row_size.bytes(&self.arrays, self.row)
    }
}

fn evaluate_keys(batch: &RecordBatch, params: &SortParams) -> DaftResult<Vec<Series>> {
    let keys = batch.eval_expression_list(&params.sort_by)?;
    Ok(keys.as_materialized_series().into_iter().cloned().collect())
}

async fn reserve_merge_memory(
    spawner: &ExecutionTaskSpawner,
    bytes: u64,
) -> DaftResult<MemoryPermit> {
    spawner.try_reserve_memory(bytes)?.ok_or_else(|| DaftError::ComputeError(format!(
        "Sort merge cannot reserve {bytes} bytes of working memory; reduce sort concurrency or increase the memory limit"
    )))
}

type RowComparator = Box<dyn Fn(usize, usize) -> Ordering + Send + Sync>;

struct CursorComparators {
    matrix: Vec<Vec<Option<RowComparator>>>,
}

impl CursorComparators {
    fn new(cursors: &[MergeCursor], params: &SortParams) -> DaftResult<Self> {
        let mut matrix = Vec::with_capacity(cursors.len());
        for left in cursors {
            let mut row = Vec::with_capacity(cursors.len());
            for right in cursors {
                let comparator = match (&left.keys, &right.keys) {
                    (Some(left), Some(right)) => Some(build_multi_array_bicompare(
                        left,
                        right,
                        &params.descending,
                        &params.nulls_first,
                    )?),
                    _ => None,
                };
                row.push(comparator);
            }
            matrix.push(row);
        }
        Ok(Self { matrix })
    }

    fn compare(&self, cursors: &[MergeCursor], left: usize, right: usize) -> Ordering {
        self.matrix[left][right]
            .as_ref()
            .expect("active sort cursors must have a comparator")(
            cursors[left].row,
            cursors[right].row,
        )
    }
}

struct MergeTournament {
    base: usize,
    nodes: Vec<Option<usize>>,
}

impl MergeTournament {
    fn new(cursors: &[MergeCursor], comparators: &CursorComparators) -> Self {
        let base = cursors.len().max(1).next_power_of_two();
        let mut tree = Self {
            base,
            nodes: vec![None; 2 * base],
        };
        for index in 0..cursors.len() {
            tree.update(index, cursors, comparators);
        }
        tree
    }

    fn winner(&self) -> Option<usize> {
        self.nodes[1]
    }

    fn update(&mut self, index: usize, cursors: &[MergeCursor], comparators: &CursorComparators) {
        let mut node = self.base + index;
        self.nodes[node] = (!cursors[index].exhausted()).then_some(index);
        while node > 1 {
            node /= 2;
            self.nodes[node] = match (self.nodes[2 * node], self.nodes[2 * node + 1]) {
                (Some(left), Some(right)) => Some(
                    if comparators.compare(cursors, left, right) == Ordering::Greater {
                        right
                    } else {
                        left
                    },
                ),
                (left, right) => left.or(right),
            };
        }
    }
}

type SortStream = Pin<Box<dyn Stream<Item = DaftResult<MicroPartition>> + Send>>;

const TARGET_MERGE_BATCH_BYTES: u64 = 8 * 1024 * 1024;
async fn write_memory_run(
    run: &MemoryRun,
    scope: &SpillScopeId,
    spawner: &ExecutionTaskSpawner,
) -> DaftResult<SpillRun> {
    let writer = SpillStreamWriter::new(
        spawner.spill_manager.clone(),
        scope.clone(),
        run.partition.schema(),
    );
    let (files, read_bytes) = writer
        .append(run.partition.clone())
        .await
        .map_err(|error| DaftError::ComputeError(error.to_string()))?
        .finish()
        .await
        .map_err(|error| DaftError::ComputeError(error.to_string()))?;
    Ok(SpillRun {
        files,
        read_bytes,
        max_row_working_bytes: run.max_row_working_bytes()?,
    })
}

enum MergeRun {
    Memory(MemoryRun),
    Spill(SpillRun),
}

impl MergeRun {
    fn max_row_working_bytes(&self) -> DaftResult<u64> {
        match self {
            Self::Memory(run) => run.max_row_working_bytes(),
            Self::Spill(run) => Ok(run.max_row_working_bytes),
        }
    }
    fn working_bytes(&self) -> u64 {
        match self {
            // Keys are views of already-accounted run columns.
            Self::Memory(_) => 0_u64,
            Self::Spill(run) => run.read_bytes,
        }
        .saturating_add(64 * 1024)
    }
}

struct MergeAdmission {
    run_count: usize,
    pool: Arc<daft_memory::MemoryPool>,
    workspace_bytes: u64,
    max_row_working_bytes: u64,
    comparator_bytes: u64,
}

fn max_row_working_bytes(runs: &[MergeRun]) -> DaftResult<u64> {
    runs.iter()
        .try_fold(0, |bytes, run| Ok(bytes.max(run.max_row_working_bytes()?)))
}

fn input_working_bytes(runs: &[MergeRun], comparator_bytes: u64) -> u64 {
    runs.iter().fold(comparator_bytes, |bytes, run| {
        bytes.saturating_add(run.working_bytes())
    })
}

fn try_admit_merge(
    runs: &[MergeRun],
    comparator_pair_bytes: u64,
    spawner: &ExecutionTaskSpawner,
) -> DaftResult<Option<MergeAdmission>> {
    // Limit comparator metadata and file descriptors as well as buffer memory. The actual
    // fan-in below is determined by admission against all ancestor memory pools.
    const MAX_MERGE_FAN_IN: usize = 64;
    for run_count in (runs.len().min(2)..=runs.len().min(MAX_MERGE_FAN_IN)).rev() {
        let comparator_bytes = comparator_matrix_bytes(run_count, comparator_pair_bytes);
        let inputs = input_working_bytes(&runs[..run_count], comparator_bytes);
        let max_row_working_bytes = max_row_working_bytes(&runs[..run_count])?;
        let minimum_workspace = (64 * 1024).max(max_row_working_bytes);
        let mut workspace_bytes = (TARGET_MERGE_BATCH_BYTES * 3).max(minimum_workspace);
        loop {
            let bytes = inputs.saturating_add(workspace_bytes);
            if bytes <= spawner.memory_limit_bytes()
                && let Some(permit) = spawner.try_reserve_memory(bytes)?
            {
                return Ok(Some(MergeAdmission {
                    run_count,
                    pool: permit.into_pool("sort-merge"),
                    workspace_bytes,
                    max_row_working_bytes,
                    comparator_bytes,
                }));
            }
            if workspace_bytes == minimum_workspace {
                break;
            }
            workspace_bytes = (workspace_bytes / 2).max(minimum_workspace);
        }
    }
    Ok(None)
}

#[cfg(test)]
async fn merge_files(
    runs: Vec<MergeRun>,
    schema: SchemaRef,
    params: Arc<SortParams>,
    spawner: ExecutionTaskSpawner,
) -> DaftResult<SortStream> {
    let admission = try_admit_merge(&runs, params.comparator_pair_bytes(&schema)?, &spawner)?
        .expect("test merge can be admitted");
    assert_eq!(admission.run_count, runs.len());
    merge_admitted(runs, schema, params, spawner, admission).await
}

async fn merge_admitted(
    runs: Vec<MergeRun>,
    schema: SchemaRef,
    params: Arc<SortParams>,
    parent_spawner: ExecutionTaskSpawner,
    admission: MergeAdmission,
) -> DaftResult<SortStream> {
    let spawner = parent_spawner.with_memory_pool(admission.pool);
    // Reserve output separately before loading inputs. Capacity released by a short input
    // frame stays in this prepaid pool, so later larger frames cannot be starved by peers.
    let workspace = reserve_merge_memory(&spawner, admission.workspace_bytes).await?;
    let mut cursors = Vec::with_capacity(runs.len());
    for run in runs {
        let mut cursor = match run {
            MergeRun::Memory(run) => MergeCursor::memory(run),
            MergeRun::Spill(run) => MergeCursor::spill(run),
        };
        cursor.load_next(&params, &spawner).await?;
        cursors.push(cursor);
    }
    if cursors.len() == 1 {
        return Ok(output::single_run_output(
            cursors.pop().unwrap(),
            schema,
            params,
            spawner,
            workspace,
        ));
    }
    let comparator_memory = reserve_merge_memory(&spawner, admission.comparator_bytes).await?;
    let mut comparators = CursorComparators::new(&cursors, &params)?;
    let mut tournament = MergeTournament::new(&cursors, &comparators);
    let working_bytes = workspace.bytes();
    let default_output_bytes = working_bytes / 3;
    Ok(Box::pin(async_stream::try_stream! {
        let _workspace = workspace;
        let _comparator_memory = comparator_memory;
        loop {
            let output_bytes = default_output_bytes;
            let starts = cursors.iter().map(|cursor| cursor.row).collect::<Vec<_>>();
            let mut selection = Vec::new();
            let mut bytes = 0_u64;
            while bytes < working_bytes {
                let Some(index) = tournament.winner() else { break };
                let row_working_bytes = cursors[index].row_working_bytes()?;
                if bytes.saturating_add(row_working_bytes) > working_bytes {
                    if bytes == 0 {
                        Err(DaftError::ComputeError("Sort row exceeds its admitted working set".to_string()))?;
                    }
                    break;
                }
                bytes += row_working_bytes;
                selection.push(index as u64);
                cursors[index].row += 1;
                tournament.update(index, &cursors, &comparators);
                if cursors[index].exhausted() { break }
            }
            if selection.is_empty() { break }
            let owned = output::materialize_selection(&cursors, &starts, selection)?;
            let owned_bytes = batch_bytes(&owned)?;
            if owned_bytes > output_bytes {
                Err(DaftError::ComputeError(format!(
                    "Sort output exceeds its materialization budget: {owned_bytes} > {output_bytes}"
                )))?;
            }
            yield MicroPartition::new_loaded(schema.clone(), Arc::new(vec![owned]), None);
            // The output reservation covers materialization and remains alive while the
            // partition is waiting to be accepted by the downstream channel. Pipeline data is
            // not owned by this operator after that handoff.
            if cursors.iter().any(|cursor| cursor.batch.is_some() && cursor.exhausted()) {
                // Comparators retain input buffers. Drop them before replacing a frame
                // and releasing its permit, but reuse them across other output batches.
                drop(comparators);
                for cursor in &mut cursors {
                    if cursor.exhausted() { cursor.load_next(&params, &spawner).await?; }
                }
                comparators = CursorComparators::new(&cursors, &params)?;
                tournament = MergeTournament::new(&cursors, &comparators);
            }
        }
    }))
}

impl BlockingSink for SortSink {
    type State = SortState;

    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _stats: Arc<Self::Stats>,
        scope: SpillScopeId,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.params.clone();
        let spawner = spawner.clone();
        spawner
            .clone()
            .spawn(
                async move {
                    run_builder::sink_input(input, &mut state, &params, &scope, &spawner).await?;
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        scope: SpillScopeId,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.params.clone();
        let spawner = spawner.clone();
        spawner
            .clone()
            .spawn(
                async move {
                    let mut memory_runs = Vec::new();
                    let mut spill_files = Vec::new();
                    let mut schema = None;
                    for mut state in states {
                        let (mut runs, mut files, state_schema) = state.finish(&params)?;
                        memory_runs.append(&mut runs);
                        spill_files.append(&mut files);
                        schema = schema.or(state_schema);
                    }
                    if memory_runs.is_empty() && spill_files.is_empty() {
                        let partitions = schema
                            .map(|schema| vec![MicroPartition::empty(Some(schema))])
                            .unwrap_or_default();
                        return Ok(BlockingSinkOutput::partitions(partitions));
                    }
                    let schema = schema.expect("non-empty sort has a schema");
                    let comparator_pair_bytes = params.comparator_pair_bytes(&schema)?;
                    let mut merge_runs = memory_runs
                        .into_iter()
                        .map(MergeRun::Memory)
                        .chain(spill_files.into_iter().map(MergeRun::Spill))
                        .collect::<Vec<_>>();
                    loop {
                        let admission =
                            match try_admit_merge(&merge_runs, comparator_pair_bytes, &spawner)? {
                                Some(admission) => admission,
                                None => {
                                    // Release retained inputs and replace them with bounded read frames
                                    // before trying again. Each iteration releases one complete run.
                                    if let Some(index) = merge_runs
                                        .iter()
                                        .position(|run| matches!(run, MergeRun::Memory(_)))
                                    {
                                        let MergeRun::Memory(run) = merge_runs.remove(index) else {
                                            unreachable!()
                                        };
                                        let spilled =
                                            write_memory_run(&run, &scope, &spawner).await?;
                                        drop(run);
                                        merge_runs.push(MergeRun::Spill(spilled));
                                        continue;
                                    }
                                    // No in-memory runs remain here. It is safe to request recovery
                                    // and wait: this sort does not retain the memory it needs released.
                                    let run_count = merge_runs.len().min(2);
                                    let max_row_working_bytes =
                                        max_row_working_bytes(&merge_runs[..run_count])?;
                                    let workspace_bytes = (64 * 1024).max(max_row_working_bytes);
                                    let comparator_bytes =
                                        comparator_matrix_bytes(run_count, comparator_pair_bytes);
                                    let bytes = input_working_bytes(
                                        &merge_runs[..run_count],
                                        comparator_bytes,
                                    )
                                    .saturating_add(workspace_bytes);
                                    let permit = spawner.reserve_memory(bytes).await?;
                                    MergeAdmission {
                                        run_count,
                                        pool: permit.into_pool("sort-merge"),
                                        workspace_bytes,
                                        max_row_working_bytes,
                                        comparator_bytes,
                                    }
                                }
                            };
                        if admission.run_count == merge_runs.len() {
                            // Advertise the prepaid capacity that stays live for the whole
                            // merge, not input reservations that may be released as runs end.
                            let reclaim_bytes = admission.pool.limit_bytes();
                            let stream = merge_admitted(
                                merge_runs,
                                schema.clone(),
                                params,
                                spawner.clone(),
                                admission,
                            )
                            .await?;
                            return Ok(BlockingSinkOutput::Partitions(output::reclaim_output(
                                stream,
                                schema,
                                scope,
                                spawner,
                                reclaim_bytes,
                            )));
                        }
                        let max_row_working_bytes = admission.max_row_working_bytes;
                        let group = merge_runs.drain(..admission.run_count).collect();
                        let mut stream = merge_admitted(
                            group,
                            schema.clone(),
                            params.clone(),
                            spawner.clone(),
                            admission,
                        )
                        .await?;
                        let mut writer = SpillStreamWriter::new(
                            spawner.spill_manager.clone(),
                            scope.clone(),
                            schema.clone(),
                        );
                        while let Some(partition) = stream.next().await {
                            writer = writer
                                .append(partition?)
                                .await
                                .map_err(|error| DaftError::ComputeError(error.to_string()))?;
                        }
                        let (files, read_bytes) = writer
                            .finish()
                            .await
                            .map_err(|error| DaftError::ComputeError(error.to_string()))?;
                        merge_runs.push(MergeRun::Spill(SpillRun {
                            files,
                            read_bytes,
                            max_row_working_bytes,
                        }));
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn reclaim_bytes(&self, states: &[Self::State]) -> u64 {
        states.iter().map(SortState::reclaim_bytes).sum()
    }

    fn release_memory(
        &self,
        mut states: Vec<Self::State>,
        target: u64,
        scope: SpillScopeId,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkReleaseResult<Self::State> {
        let params = self.params.clone();
        let spawner = spawner.clone();
        spawner
            .clone()
            .spawn(
                async move {
                    let mut released = 0;
                    for state in &mut states {
                        if released >= target {
                            break;
                        }
                        let before = state.reclaim_bytes();
                        state.flush_pending(&params)?;
                        let returned_workspace = before.saturating_sub(state.reclaim_bytes());
                        let needed = target
                            .saturating_sub(released)
                            .saturating_sub(returned_workspace);
                        let (runs, files, _) = state.building_mut();
                        spill_runs(runs, files, needed, &scope, &spawner).await?;
                        released += before.saturating_sub(state.reclaim_bytes());
                    }
                    Ok((states, released))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Sort".into()
    }
    fn op_type(&self) -> NodeType {
        NodeType::Sort
    }
    fn multiline_display(&self) -> Vec<String> {
        assert!(!self.params.sort_by.is_empty());
        let pairs = self
            .params
            .sort_by
            .iter()
            .zip(&self.params.descending)
            .zip(&self.params.nulls_first)
            .map(|((key, descending), nulls_first)| {
                format!(
                    "({}, {}, {})",
                    key,
                    if *descending {
                        "descending"
                    } else {
                        "ascending"
                    },
                    if *nulls_first {
                        "nulls first"
                    } else {
                        "nulls last"
                    }
                )
            })
            .join(", ");
        vec![format!("Sort: Sort by = {pairs}")]
    }
    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(SortState::Building {
            pending: RunBuilder::default(),
            memory_runs: Vec::new(),
            spill_runs: Vec::new(),
            schema: None,
        })
    }
    fn max_concurrency(&self) -> usize {
        1
    }
}
