use super::*;

const TARGET_RUN_BYTES: u64 = 128 * 1024 * 1024;

pub(super) async fn sink_input(
    input: MicroPartition,
    state: &mut SortState,
    params: &SortParams,
    scope: &SpillScopeId,
    spawner: &ExecutionTaskSpawner,
) -> DaftResult<()> {
    let schema = state.building_mut().2;
    if schema
        .as_ref()
        .is_some_and(|schema| *schema != input.schema())
    {
        return Err(DaftError::SchemaMismatch(
            "Sort input schemas must match".to_string(),
        ));
    }
    *schema = Some(input.schema());
    let target = RunBuilder::target_bytes(spawner.memory_limit_bytes());
    for batch in input.record_batches() {
        let columns = batch
            .as_materialized_series()
            .into_iter()
            .map(SeriesSize::new)
            .collect::<DaftResult<Vec<_>>>()?;
        let mut start = 0;
        while start < batch.len() {
            let remaining = target.saturating_sub(state.pending().bytes());
            let rows = select_rows(&columns, start, batch.len() - start, remaining)?;
            let bytes = selection_working_bytes(&columns, start, rows)?;
            if state.pending().bytes() > 0 && bytes > remaining {
                state.flush_pending(params)?;
                continue;
            }
            // Prepay retained input, concatenation, sorted output and take scratch space.
            // Finishing/reclaiming this buffer must not acquire ordinary memory again.
            let requested = bytes;
            let mut flushed_for_admission = false;
            let memory = match spawner.try_reserve_memory(requested)? {
                Some(memory) => memory,
                None => {
                    state.flush_pending(params)?;
                    flushed_for_admission = true;
                    match spawner.try_reserve_memory(requested)? {
                        Some(memory) => memory,
                        None => {
                            let (runs, files, _) = state.building_mut();
                            spill_runs(runs, files, u64::MAX, scope, spawner).await?;
                            // No retained input buffer or in-memory Sort run remains while waiting.
                            spawner.reserve_memory(requested).await?
                        }
                    }
                }
            };
            let finish_now = bytes >= target
                || (!flushed_for_admission
                    && (start + rows < batch.len()
                        || state.pending().bytes().saturating_add(bytes) >= target));
            let selected = batch.slice(start, start + rows)?;
            let selected = if finish_now {
                // This view is consumed by sorting before returning from sink_input.
                // Avoid copying a full run once here and then again during sorting.
                selected
            } else {
                // A retained slice must not pin an arbitrarily large upstream buffer.
                let indices = UInt64Array::from_values("", 0..rows as u64);
                selected.take(&indices)?
            };
            state.pending().push(selected, memory, bytes);
            if finish_now {
                state.flush_pending(params)?;
            }
            start += rows;
        }
    }
    Ok(())
}

/// Owns input across morsels and prepays the workspace needed to turn it into a
/// sorted run. Finishing a run never waits for another ordinary memory allocation.
#[derive(Default)]
pub(crate) struct RunBuilder {
    batches: Vec<RecordBatch>,
    memory: Option<MemoryPermit>,
    // Prepaid materialization cost, including nested take indices, not just payload.
    bytes: u64,
}

impl RunBuilder {
    pub(super) fn target_bytes(limit: u64) -> u64 {
        TARGET_RUN_BYTES.min((limit / 16).max(1)).saturating_mul(3)
    }

    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(super) fn reclaim_bytes(&self) -> u64 {
        self.memory.as_ref().map_or(0, MemoryPermit::bytes)
    }

    pub(super) fn push(&mut self, batch: RecordBatch, memory: MemoryPermit, bytes: u64) {
        if let Some(current) = &mut self.memory {
            current.merge(memory);
        } else {
            self.memory = Some(memory);
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.batches.push(batch);
    }

    pub(super) fn finish(self, params: &SortParams) -> DaftResult<Option<MemoryRun>> {
        let Self {
            batches, memory, ..
        } = self;
        let Some(mut memory) = memory else {
            debug_assert!(batches.is_empty());
            return Ok(None);
        };
        let schema = batches[0].schema.clone();
        let input = MicroPartition::new_loaded(schema, Arc::new(batches), None);
        let sorted = input.sort(&params.sort_by, &params.descending, &params.nulls_first)?;
        let sorted_bytes =
            partition_bytes(&sorted)?.saturating_add(batch_overhead(sorted.schema().len()));
        if sorted_bytes > memory.bytes() {
            return Err(DaftError::ComputeError(format!(
                "sorted run requires {sorted_bytes} bytes, exceeding its {} byte reservation",
                memory.bytes(),
            )));
        }
        // Drop all input buffers before returning the prepaid workspace to competitors.
        drop(input);
        memory.shrink_to(sorted_bytes);
        Ok(Some(MemoryRun {
            partition: sorted,
            memory,
            max_row_working_bytes: OnceLock::new(),
        }))
    }
}

fn batch_overhead(columns: usize) -> u64 {
    256_u64.saturating_add((columns as u64).saturating_mul(128))
}

/// Includes row indices and batch descriptors, even for zero-payload columns.
pub(super) fn selection_bytes(
    columns: &[SeriesSize],
    start: usize,
    rows: usize,
) -> DaftResult<u64> {
    columns.iter().try_fold(
        batch_overhead(columns.len()).saturating_add((rows as u64).saturating_mul(8)),
        |bytes, column| Ok(bytes.saturating_add(column.bytes(start, rows)?)),
    )
}

/// Retained input, concatenation and output may coexist. Nested take indices
/// are additional scratch space and must not be inferred from the payload size.
pub(super) fn selection_working_bytes(
    columns: &[SeriesSize],
    start: usize,
    rows: usize,
) -> DaftResult<u64> {
    columns.iter().try_fold(
        selection_bytes(columns, start, rows)?.saturating_mul(3),
        |bytes, column| Ok(bytes.saturating_add(column.take_workspace_bytes(start, rows)?)),
    )
}

pub(super) fn select_rows(
    columns: &[SeriesSize],
    start: usize,
    remaining: usize,
    target: u64,
) -> DaftResult<usize> {
    let mut low = 1;
    let mut high = remaining;
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        if selection_working_bytes(columns, start, mid)? <= target {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    Ok(low)
}
