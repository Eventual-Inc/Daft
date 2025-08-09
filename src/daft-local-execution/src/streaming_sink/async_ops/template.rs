//! Template for async streaming sink implementations.
//!
//! General process for async operations:
//!
//! Required Methods / Properties:
//! - execute_batch_size(): Min # of rows to return during execute
//! - finalize_batch_size(): Min # of rows to return during finalize
//! - num_active_tasks(): # of actively running operations
//! - accept_more_input(): Whether to accept more input during StreamingSink::execute
//! - process_input(): Process new input rows
//! - poll_finished(): Poll for finished tasks
//!
//! On StreamingSink::execute
//! 1. Start the async task per row of input
//! 2. Poll for finished tasks up to execute_batch_size()
//! 3. If accept_more_input() == false, return HasMoreOutput to loop again
//! 4. Once accept_more_input() == true, return NeedMoreInput to continue
//!
//! On StreamingSink::finalize
//! 1. Poll for finished tasks up to finalize_batch_size()
//! 2. If num_active_tasks() >= 0, return HasMoreOutput to loop again
//! 3. Once num_active_tasks() == 0, return Finished

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, LazyLock,
};

use common_error::DaftResult;
use common_metrics::{snapshot, Stat, StatSnapshotSend};
use common_runtime::NUM_CPUS;
use daft_core::{
    prelude::{BooleanArray, SchemaRef, UInt64Array},
    series::{IntoSeries, Series},
};
use daft_dsl::expr::BoundColumn;
use daft_micropartition::{partitioning::Partition, MicroPartition};
use daft_recordbatch::RecordBatch;

use crate::{
    runtime_stats::{RuntimeStats, CPU_US_KEY, ROWS_EMITTED_KEY, ROWS_RECEIVED_KEY},
    streaming_sink::base::StreamingSinkState,
};

pub trait AsyncOpState {
    /// Min # of rows to return during StreamingSink::execute
    fn execute_batch_size(&self) -> usize;
    /// Min # of rows to return during StreamingSink::finalize
    fn finalize_batch_size(&self) -> usize;
    /// # of actively running operations
    fn num_active_tasks(&self) -> usize;
    /// Whether to accept more input during StreamingSink::execute
    fn accept_more_input(&self) -> bool;
    /// Start a task
    fn process_input(&mut self, input: Arc<MicroPartition>) -> DaftResult<()>;
    /// Poll for finished tasks
    async fn poll_finished(
        &mut self,
        num_rows: usize,
        stats: &AsyncOpRuntimeStatsBuilder,
    ) -> DaftResult<(Vec<u64>, Series)>;
}

/// Helper function for building the output of an async row-wise streaming sink
/// given the input, output row idxs, and output value column to append
pub fn build_output(
    all_inputs: Arc<MicroPartition>,
    output_row_idxs: Vec<u64>,
    output_values: Series,
    output_schema: SchemaRef,
) -> DaftResult<RecordBatch> {
    if output_row_idxs.is_empty() {
        return Ok(RecordBatch::empty(Some(output_schema)));
    }

    let output_row_idxs = UInt64Array::from(("idxs", output_row_idxs)).into_series();
    let original_rows = all_inputs.take(&output_row_idxs)?;
    let output = original_rows.append_column(output_values)?;
    Ok(output)
}

pub struct AsyncOpRuntimeStatsBuilder {
    rows_received: AtomicU64,
    rows_emitted: AtomicU64,
    num_download_failed: AtomicU64,
    cpu_us: AtomicU64,
}

impl AsyncOpRuntimeStatsBuilder {
    pub fn new() -> Self {
        Self {
            rows_received: AtomicU64::new(0),
            rows_emitted: AtomicU64::new(0),
            num_download_failed: AtomicU64::new(0),
            cpu_us: AtomicU64::new(0),
        }
    }

    pub fn inc_num_failed(&self) {
        self.num_download_failed.fetch_add(1, Ordering::Relaxed);
    }
}

impl RuntimeStats for AsyncOpRuntimeStatsBuilder {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
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

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshotSend {
        snapshot![
            CPU_US_KEY; Stat::Count(self.cpu_us.load(ordering)),
            ROWS_RECEIVED_KEY; Stat::Count(self.rows_received.load(ordering)),
            ROWS_EMITTED_KEY; Stat::Count(self.rows_emitted.load(ordering)),
            "failed downloads"; Stat::Count(self.num_download_failed.load(ordering)),
        ]
    }
}

pub static IN_FLIGHT_SCALE_FACTOR: LazyLock<usize> = LazyLock::new(|| 4 * *NUM_CPUS);

pub struct AsyncSinkState<T: AsyncOpState> {
    // Arguments
    passthrough_columns: Vec<BoundColumn>,
    output_schema: SchemaRef,
    output_column: String,
    // Max size of saved inputs before we start pruning rows
    input_size_bytes_buffer: usize,

    // Self State
    // Save input columns to pass through to output
    saved_inputs: Arc<MicroPartition>,
    // True when row is not finished downloading.
    // Note: unfinished_inputs.len() == all_inputs.num_rows()
    unfinished_inputs: Vec<bool>,
    // Map idx from in_flight_downloads to row in all_inputs
    // Used to filter out finished rows from all_inputs while keeping in_flight_downloads in sync
    // Note in_flight_inputs_idx.len() == submitted_downloads
    in_flight_inputs_idx: Vec<usize>,

    // Operator State
    inner_state: T,
}

impl<T: AsyncOpState> AsyncSinkState<T> {
    pub fn new(
        passthrough_columns: Vec<BoundColumn>,
        save_schema: SchemaRef,
        output_schema: SchemaRef,
        output_column: String,
        input_size_bytes_buffer: usize,
        inner_state: T,
    ) -> Self {
        Self {
            passthrough_columns,
            output_schema,
            output_column,
            input_size_bytes_buffer,

            saved_inputs: Arc::new(MicroPartition::empty(Some(save_schema))),
            unfinished_inputs: Vec::new(),
            in_flight_inputs_idx: Vec::new(),

            inner_state,
        }
    }

    pub fn accept_more_input(&self) -> bool {
        self.inner_state.accept_more_input()
    }

    fn input_size(&self) -> usize {
        // Expect that all inputs are loaded
        self.saved_inputs.size_bytes().unwrap().unwrap_or(0)
    }

    pub fn num_active_tasks(&self) -> usize {
        self.inner_state.num_active_tasks()
    }

    /// Start downloads for given URLs
    pub fn process_input(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        if input.is_empty() {
            return Ok(());
        }

        self.inner_state.process_input(input.clone())?;

        // Add to row tracking data structures
        let curr_num_rows = self.saved_inputs.num_rows()?;
        self.in_flight_inputs_idx
            .extend(curr_num_rows..(curr_num_rows + input.num_rows()?));
        self.unfinished_inputs.extend(vec![true; input.num_rows()?]);
        self.saved_inputs = Arc::new(MicroPartition::concat([
            self.saved_inputs.clone(),
            Arc::new(input.select_columns(&self.passthrough_columns)?),
        ])?);

        Ok(())
    }

    pub async fn poll_finished(
        &mut self,
        finished: bool,
        stats: &AsyncOpRuntimeStatsBuilder,
    ) -> DaftResult<RecordBatch> {
        let exp_capacity = if finished {
            self.inner_state.finalize_batch_size()
        } else {
            self.inner_state.execute_batch_size()
        };

        let (completed_idxs, completed_contents) =
            self.inner_state.poll_finished(exp_capacity, stats).await?;

        let completed_idxs: Vec<_> = completed_idxs
            .iter()
            .map(|idx| self.in_flight_inputs_idx[*idx as usize] as u64)
            .collect();
        let completed_contents = completed_contents.rename(self.output_column.as_str());

        // Mark finished rows as finished
        for idx in &completed_idxs {
            self.unfinished_inputs[*idx as usize] = false;
        }

        let output = build_output(
            self.saved_inputs.clone(),
            completed_idxs,
            completed_contents,
            self.output_schema.clone(),
        )?;

        // Mark finished rows and cleanup
        if self.input_size() > self.input_size_bytes_buffer {
            let unfinished_inputs =
                BooleanArray::from(("mask", self.unfinished_inputs.as_slice())).into_series();
            self.saved_inputs = Arc::new(self.saved_inputs.mask_filter(&unfinished_inputs)?);

            // Update the in_flight_inputs_idx with the new locations in all_inputs
            let mut cnt = 0;
            for (idx, val) in self.unfinished_inputs.iter().enumerate() {
                if *val {
                    self.in_flight_inputs_idx[idx] = cnt;
                    cnt += 1;
                }
            }
            self.unfinished_inputs = vec![true; self.saved_inputs.num_rows()?];
        }

        Ok(output)
    }
}

impl<T: AsyncOpState> StreamingSinkState for AsyncSinkState<T>
where
    T: Send + Sync + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
