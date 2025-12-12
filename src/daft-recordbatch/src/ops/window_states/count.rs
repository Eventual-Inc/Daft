use common_error::DaftResult;
use daft_arrow::buffer::NullBuffer;
use daft_core::{count_mode::CountMode, prelude::*};

use super::WindowAggStateOps;

pub struct CountWindowState {
    source_validity: Option<NullBuffer>,
    valid_count: usize,
    total_count: usize,
    count_vec: Vec<u64>,
    count_mode: CountMode,
}

impl CountWindowState {
    pub fn new(source: &Series, total_length: usize, count_mode: CountMode) -> Self {
        let source_bitmap = source.validity().cloned();

        Self {
            source_validity: source_bitmap,
            valid_count: 0,
            total_count: 0,
            count_vec: Vec::with_capacity(total_length),
            count_mode,
        }
    }
}

impl WindowAggStateOps for CountWindowState {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        self.total_count += end_idx - start_idx;
        if matches!(self.count_mode, CountMode::Valid | CountMode::Null) {
            for i in start_idx..end_idx {
                if self.source_validity.is_none()
                    || self.source_validity.as_ref().unwrap().is_valid(i)
                {
                    self.valid_count += 1;
                }
            }
        }
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        self.total_count -= end_idx - start_idx;
        if matches!(self.count_mode, CountMode::Valid | CountMode::Null) {
            for i in start_idx..end_idx {
                if self.source_validity.is_none()
                    || self.source_validity.as_ref().unwrap().is_valid(i)
                {
                    self.valid_count -= 1;
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        let count = match self.count_mode {
            CountMode::All => self.total_count,
            CountMode::Valid => self.valid_count,
            CountMode::Null => self.total_count - self.valid_count,
        };
        self.count_vec.push(count as u64);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        Ok(DataArray::<UInt64Type>::from(("", self.count_vec.clone())).into_series())
    }
}
