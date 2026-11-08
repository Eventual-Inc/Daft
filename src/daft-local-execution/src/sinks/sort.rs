use std::{cmp::Ordering, collections::VecDeque, sync::Arc};

use arrow::{
    compute::SortOptions,
    row::{RowConverter, Rows, SortField},
};
use arrow_array::{ArrayRef, RecordBatch as ArrowRecordBatch};
use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch as DaftRecordBatch;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    spill::{SpillManager, SpillReader, SpillRun},
};

pub(crate) enum SortState {
    Building {
        parts: Vec<MicroPartition>,
        retained_bytes: usize,
        runs: Vec<SpillRun>,
    },
    Done,
}

impl SortState {
    fn spill_current(
        parts: &mut Vec<MicroPartition>,
        retained_bytes: &mut usize,
        runs: &mut Vec<SpillRun>,
        params: &SortParams,
    ) -> DaftResult<()> {
        if parts.is_empty() {
            return Ok(());
        }
        let manager = params
            .spill_manager
            .as_ref()
            .expect("spill_current requires a spill manager");
        let concated = MicroPartition::concat(std::mem::take(parts))?;
        let sorted = concated.sort(&params.sort_by, &params.descending, &params.nulls_first)?;
        if !sorted.is_empty() {
            runs.push(manager.write_partition(&sorted)?);
        }
        *retained_bytes = 0;
        Ok(())
    }

    fn push_bounded_chunk(
        parts: &mut Vec<MicroPartition>,
        retained_bytes: &mut usize,
        runs: &mut Vec<SpillRun>,
        chunk: MicroPartition,
        params: &SortParams,
        limit: usize,
    ) -> DaftResult<()> {
        let chunk_bytes = chunk.size_bytes();
        if chunk_bytes > limit {
            return Err(DaftError::ComputeError(
                "PGDAFT_SPILL_ROW_EXCEEDS_LIMIT".to_string(),
            ));
        }
        let next_bytes = retained_bytes.checked_add(chunk_bytes).ok_or_else(|| {
            DaftError::ComputeError(
                "PGDAFT_SORT_MEMORY_LIMIT: retained byte count overflow".to_string(),
            )
        })?;
        if next_bytes > limit {
            Self::spill_current(parts, retained_bytes, runs, params)?;
        }
        *retained_bytes = retained_bytes.checked_add(chunk_bytes).ok_or_else(|| {
            DaftError::ComputeError(
                "PGDAFT_SORT_MEMORY_LIMIT: retained byte count overflow".to_string(),
            )
        })?;
        parts.push(chunk);
        Ok(())
    }

    fn push(&mut self, part: MicroPartition, params: &SortParams) -> DaftResult<()> {
        let Self::Building {
            parts,
            retained_bytes,
            runs,
        } = self
        else {
            panic!("SortSink should be in Building state");
        };

        let Some(limit) = params.retained_limit_bytes else {
            *retained_bytes = retained_bytes
                .checked_add(part.size_bytes())
                .ok_or_else(|| {
                    DaftError::ComputeError(
                        "PGDAFT_SORT_MEMORY_LIMIT: retained byte count overflow".to_string(),
                    )
                })?;
            parts.push(part);
            return Ok(());
        };

        if params.spill_manager.is_none() {
            let next_bytes = retained_bytes
                .checked_add(part.size_bytes())
                .ok_or_else(|| {
                    DaftError::ComputeError(
                        "PGDAFT_SORT_MEMORY_LIMIT: retained byte count overflow".to_string(),
                    )
                })?;
            if next_bytes > limit {
                return Err(DaftError::ComputeError(format!(
                    "PGDAFT_SORT_MEMORY_LIMIT: retained {next_bytes} bytes exceeds {limit} bytes; spill is not enabled"
                )));
            }
            parts.push(part);
            *retained_bytes = next_bytes;
            return Ok(());
        }

        if part.is_empty() {
            return Ok(());
        }
        let total_rows = part.len();
        let total_bytes = part.size_bytes();
        let mut offset = 0;
        while offset < total_rows {
            let remaining = total_rows - offset;
            let mut rows = if total_bytes == 0 {
                remaining
            } else {
                (((remaining as u128) * (limit as u128)) / (total_bytes as u128))
                    .max(1)
                    .min(remaining as u128) as usize
            };
            let chunk = loop {
                let candidate = part.slice(offset, offset + rows)?;
                if candidate.size_bytes() <= limit {
                    break candidate;
                }
                if rows == 1 {
                    return Err(DaftError::ComputeError(
                        "PGDAFT_SPILL_ROW_EXCEEDS_LIMIT".to_string(),
                    ));
                }
                rows = (rows / 2).max(1);
            };
            Self::push_bounded_chunk(parts, retained_bytes, runs, chunk, params, limit)?;
            offset += rows;
        }
        Ok(())
    }

    fn finalize(&mut self) -> (Vec<MicroPartition>, Vec<SpillRun>) {
        let res = if let Self::Building { parts, runs, .. } = self {
            (std::mem::take(parts), std::mem::take(runs))
        } else {
            panic!("SortSink should be in Building state");
        };
        *self = Self::Done;
        res
    }
}

struct SortParams {
    sort_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    retained_limit_bytes: Option<usize>,
    spill_manager: Option<Arc<SpillManager>>,
}

pub struct SortSink {
    params: Arc<SortParams>,
}

impl SortSink {
    pub(crate) fn new(
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        retained_limit_bytes: Option<usize>,
        spill_manager: Option<Arc<SpillManager>>,
    ) -> Self {
        Self {
            params: Arc::new(SortParams {
                sort_by,
                descending,
                nulls_first,
                retained_limit_bytes,
                spill_manager,
            }),
        }
    }
}

fn sort_key_arrays(batch: &ArrowRecordBatch, sort_by: &[BoundExpr]) -> DaftResult<Vec<ArrayRef>> {
    let batch = DaftRecordBatch::try_from(batch)?;
    let keys = batch.eval_expression_list(sort_by)?;
    keys.columns()
        .iter()
        .map(|column| column.as_materialized_series().to_arrow())
        .collect()
}

struct MergeCursor {
    reader: SpillReader,
    batch: Option<ArrowRecordBatch>,
    rows: Option<Rows>,
    index: usize,
}

impl MergeCursor {
    fn new(
        reader: SpillReader,
        first: ArrowRecordBatch,
        converter: &RowConverter,
        sort_by: &[BoundExpr],
    ) -> DaftResult<Self> {
        let rows = converter.convert_columns(&sort_key_arrays(&first, sort_by)?)?;
        Ok(Self {
            reader,
            batch: Some(first),
            rows: Some(rows),
            index: 0,
        })
    }

    fn load_next(&mut self, converter: &RowConverter, sort_by: &[BoundExpr]) -> DaftResult<()> {
        loop {
            let Some(batch) = self.reader.next_batch()? else {
                self.batch = None;
                self.rows = None;
                self.index = 0;
                return Ok(());
            };
            if batch.num_rows() == 0 {
                continue;
            }
            self.rows = Some(converter.convert_columns(&sort_key_arrays(&batch, sort_by)?)?);
            self.batch = Some(batch);
            self.index = 0;
            return Ok(());
        }
    }

    fn advance_batch_if_needed(
        &mut self,
        converter: &RowConverter,
        sort_by: &[BoundExpr],
    ) -> DaftResult<()> {
        if self
            .batch
            .as_ref()
            .is_some_and(|batch| self.index == batch.num_rows())
        {
            self.load_next(converter, sort_by)?;
        }
        Ok(())
    }
}

fn first_nonempty(reader: &mut SpillReader) -> DaftResult<Option<ArrowRecordBatch>> {
    while let Some(batch) = reader.next_batch()? {
        if batch.num_rows() > 0 {
            return Ok(Some(batch));
        }
    }
    Ok(None)
}

fn cursor_cmp(left: &MergeCursor, right: &MergeCursor) -> Ordering {
    left.rows
        .as_ref()
        .expect("left merge rows must be live")
        .row(left.index)
        .cmp(
            &right
                .rows
                .as_ref()
                .expect("right merge rows must be live")
                .row(right.index),
        )
}

fn drain_cursor(
    cursor: &mut MergeCursor,
    writer: &mut crate::spill::SpillWriter,
) -> DaftResult<()> {
    if let Some(batch) = cursor.batch.take() {
        let remaining = batch.num_rows() - cursor.index;
        if remaining > 0 {
            writer.write(&batch.slice(cursor.index, remaining))?;
        }
    }
    while let Some(batch) = cursor.reader.next_batch()? {
        if batch.num_rows() > 0 {
            writer.write(&batch)?;
        }
    }
    Ok(())
}

fn merge_runs(left: SpillRun, right: SpillRun, params: &SortParams) -> DaftResult<SpillRun> {
    let manager = params
        .spill_manager
        .as_ref()
        .expect("merge_runs requires a spill manager");
    let mut left_reader = left.open()?;
    let mut right_reader = right.open()?;
    let schema = left_reader.schema();
    if right_reader.schema().as_ref() != schema.as_ref() {
        return Err(DaftError::SchemaMismatch(
            "PGDAFT_SPILL_SCHEMA_MISMATCH".to_string(),
        ));
    }
    let left_first = first_nonempty(&mut left_reader)?;
    let right_first = first_nonempty(&mut right_reader)?;
    let first = left_first
        .as_ref()
        .or(right_first.as_ref())
        .ok_or_else(|| DaftError::ComputeError("PGDAFT_SPILL_EMPTY_RUN".to_string()))?;
    let first_keys = sort_key_arrays(first, &params.sort_by)?;
    let fields = first_keys
        .iter()
        .zip(params.descending.iter())
        .zip(params.nulls_first.iter())
        .map(|((array, descending), nulls_first)| {
            SortField::new_with_options(
                array.data_type().clone(),
                SortOptions {
                    descending: *descending,
                    nulls_first: *nulls_first,
                },
            )
        })
        .collect();
    let converter = RowConverter::new(fields)?;
    let mut writer = manager.create_writer(schema.as_ref())?;

    match (left_first, right_first) {
        (Some(left_first), Some(right_first)) => {
            let mut left = MergeCursor::new(left_reader, left_first, &converter, &params.sort_by)?;
            let mut right =
                MergeCursor::new(right_reader, right_first, &converter, &params.sort_by)?;
            while left.batch.is_some() && right.batch.is_some() {
                if cursor_cmp(&left, &right) != Ordering::Greater {
                    let start = left.index;
                    let batch_len = left.batch.as_ref().unwrap().num_rows();
                    while left.index < batch_len && cursor_cmp(&left, &right) != Ordering::Greater {
                        left.index += 1;
                    }
                    writer.write(
                        &left
                            .batch
                            .as_ref()
                            .unwrap()
                            .slice(start, left.index - start),
                    )?;
                    left.advance_batch_if_needed(&converter, &params.sort_by)?;
                } else {
                    let start = right.index;
                    let batch_len = right.batch.as_ref().unwrap().num_rows();
                    while right.index < batch_len && cursor_cmp(&left, &right) == Ordering::Greater
                    {
                        right.index += 1;
                    }
                    writer.write(
                        &right
                            .batch
                            .as_ref()
                            .unwrap()
                            .slice(start, right.index - start),
                    )?;
                    right.advance_batch_if_needed(&converter, &params.sort_by)?;
                }
            }
            drain_cursor(&mut left, &mut writer)?;
            drain_cursor(&mut right, &mut writer)?;
        }
        (Some(left_first), None) => {
            let mut left = MergeCursor::new(left_reader, left_first, &converter, &params.sort_by)?;
            drain_cursor(&mut left, &mut writer)?;
        }
        (None, Some(right_first)) => {
            let mut right =
                MergeCursor::new(right_reader, right_first, &converter, &params.sort_by)?;
            drain_cursor(&mut right, &mut writer)?;
        }
        (None, None) => unreachable!(),
    }
    writer.finish()
}

impl BlockingSink for SortSink {
    type State = SortState;

    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        match state.push(input, &self.params) {
            Ok(()) => Ok(state).into(),
            Err(error) => Err(error).into(),
        }
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let mut parts = Vec::new();
                    let mut runs = VecDeque::new();
                    for mut state in states {
                        let (state_parts, state_runs) = state.finalize();
                        parts.extend(state_parts);
                        runs.extend(state_runs);
                    }
                    if runs.is_empty() {
                        if parts.is_empty() {
                            return Ok(BlockingSinkOutput::Partitions(Vec::new()));
                        }
                        let concated = MicroPartition::concat(parts)?;
                        let sorted = concated.sort(
                            &params.sort_by,
                            &params.descending,
                            &params.nulls_first,
                        )?;
                        return Ok(BlockingSinkOutput::Partitions(vec![sorted]));
                    }

                    if !parts.is_empty() {
                        let concated = MicroPartition::concat(parts)?;
                        let sorted = concated.sort(
                            &params.sort_by,
                            &params.descending,
                            &params.nulls_first,
                        )?;
                        if !sorted.is_empty() {
                            runs.push_back(
                                params
                                    .spill_manager
                                    .as_ref()
                                    .expect("spill run requires manager")
                                    .write_partition(&sorted)?,
                            );
                        }
                    }

                    while runs.len() > 1 {
                        let left = runs.pop_front().unwrap();
                        let right = runs.pop_front().unwrap();
                        runs.push_back(merge_runs(left, right, &params)?);
                    }
                    Ok(BlockingSinkOutput::Spilled(runs.pop_front().unwrap()))
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
        let mut lines = vec![];
        assert!(!self.params.sort_by.is_empty());
        let pairs = self
            .params
            .sort_by
            .iter()
            .zip(self.params.descending.iter())
            .zip(self.params.nulls_first.iter())
            .map(|((sb, d), nf)| {
                format!(
                    "({}, {}, {})",
                    sb,
                    if *d { "descending" } else { "ascending" },
                    if *nf { "nulls first" } else { "nulls last" }
                )
            })
            .join(", ");
        lines.push(format!("Sort: Sort by = {}", pairs));
        lines
    }

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(SortState::Building {
            parts: Vec::new(),
            retained_bytes: 0,
            runs: Vec::new(),
        })
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}
