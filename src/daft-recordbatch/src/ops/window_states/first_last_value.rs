use std::collections::VecDeque;

use arrow::array::NullBufferBuilder;
use common_error::DaftResult;
use daft_core::prelude::*;

use super::WindowAggStateOps;

pub struct FirstValueWindowState {
    source: Series,
    ignore_nulls: bool,
    deque: VecDeque<usize>,
    result_idxs: Vec<u64>,
    null_builder: NullBufferBuilder,
}

impl FirstValueWindowState {
    pub fn new(source: &Series, total_length: usize, ignore_nulls: bool) -> Self {
        Self {
            source: source.clone(),
            ignore_nulls,
            deque: VecDeque::new(),
            result_idxs: Vec::with_capacity(total_length),
            null_builder: NullBufferBuilder::new(total_length),
        }
    }
}

impl WindowAggStateOps for FirstValueWindowState {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.ignore_nulls && !self.source.is_valid(i) {
                continue;
            }
            self.deque.push_back(i);
        }
        Ok(())
    }

    fn remove(&mut self, _start_idx: usize, end_idx: usize) -> DaftResult<()> {
        while let Some(&front) = self.deque.front()
            && front < end_idx
        {
            self.deque.pop_front();
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        if self.deque.is_empty() {
            self.null_builder.append_null();
            self.result_idxs.push(0);
            return Ok(());
        }
        let idx = *self.deque.front().unwrap();
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

pub struct LastValueWindowState {
    source: Series,
    ignore_nulls: bool,
    last_idx: Option<usize>,
    result_idxs: Vec<u64>,
    null_builder: NullBufferBuilder,
}

impl LastValueWindowState {
    pub fn new(source: &Series, total_length: usize, ignore_nulls: bool) -> Self {
        Self {
            source: source.clone(),
            ignore_nulls,
            last_idx: None,
            result_idxs: Vec::with_capacity(total_length),
            null_builder: NullBufferBuilder::new(total_length),
        }
    }
}

impl WindowAggStateOps for LastValueWindowState {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        if !self.ignore_nulls {
            self.last_idx = Some(end_idx - 1);
            return Ok(());
        }
        for i in (start_idx..end_idx).rev() {
            if self.source.is_valid(i) {
                self.last_idx = Some(i);
                break;
            }
        }
        Ok(())
    }

    fn remove(&mut self, _start_idx: usize, end_idx: usize) -> DaftResult<()> {
        if let Some(last) = self.last_idx
            && last < end_idx
        {
            self.last_idx = None;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        if self.last_idx.is_none() {
            self.null_builder.append_null();
            self.result_idxs.push(0);
            return Ok(());
        }
        let idx = self.last_idx.unwrap();
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
