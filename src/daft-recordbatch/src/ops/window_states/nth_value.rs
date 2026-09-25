use arrow::array::NullBufferBuilder;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use super::WindowAggStateOps;

/// Window state for `nth_value(expr, n)` over a sliding/expanding frame.
///
/// Returns the n-th value (1-based) within the current window frame in
/// the ORDER BY order of the underlying RecordBatch. When `ignore_nulls`
/// is true, null values are skipped and the n-th non-null value is
/// returned. If the frame contains fewer than `n` qualifying rows, the
/// output for that row is NULL.
pub struct NthValueWindowState {
    source: Series,
    n: usize,
    ignore_nulls: bool,
    // Sliding window of indices currently inside the frame, in order.
    // Only stores indices that satisfy the `ignore_nulls` filter.
    indices: std::collections::VecDeque<usize>,
    result_idxs: Vec<u64>,
    null_builder: NullBufferBuilder,
}

impl NthValueWindowState {
    pub fn new(source: &Series, total_length: usize, n: usize, ignore_nulls: bool) -> Self {
        Self {
            source: source.clone(),
            n,
            ignore_nulls,
            indices: std::collections::VecDeque::new(),
            result_idxs: Vec::with_capacity(total_length),
            null_builder: NullBufferBuilder::new(total_length),
        }
    }
}

impl WindowAggStateOps for NthValueWindowState {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.ignore_nulls && !self.source.is_valid(i) {
                continue;
            }
            self.indices.push_back(i);
        }
        Ok(())
    }

    fn remove(&mut self, _start_idx: usize, end_idx: usize) -> DaftResult<()> {
        while let Some(&front) = self.indices.front()
            && front < end_idx
        {
            self.indices.pop_front();
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        // n is 1-based.
        if self.indices.len() < self.n {
            self.null_builder.append_null();
            self.result_idxs.push(0);
            return Ok(());
        }
        let idx = *self.indices.get(self.n - 1).ok_or_else(|| {
            DaftError::ComputeError("nth_value: deque index out of range".to_string())
        })?;
        self.result_idxs.push(idx as u64);
        if self.source.is_valid(idx) {
            self.null_builder.append_non_null();
        } else {
            self.null_builder.append_null();
        }
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        let result = self.source.take(&DataArray::<UInt64Type>::from_vec(
            "",
            self.result_idxs.clone(),
        ))?;
        result.with_nulls(self.null_builder.finish_cloned())
    }
}
