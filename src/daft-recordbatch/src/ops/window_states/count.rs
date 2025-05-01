use arrow2::bitmap::Bitmap;
use common_error::DaftResult;
use daft_core::{count_mode::CountMode, prelude::*};

use super::WindowAggStateOps;

pub struct CountWindowState<'a> {
    source: Bitmap,
    valid_count: usize,
    total_count: usize,
    count_vec: Vec<u64>,
    count_mode: CountMode,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> CountWindowState<'a> {
    pub fn new(source: &'a Series, total_length: usize) -> Self {
        Self::with_count_mode(source, total_length, CountMode::Valid)
    }

    pub fn with_count_mode(source: &'a Series, total_length: usize, count_mode: CountMode) -> Self {
        let source_bitmap = source
            .validity()
            .cloned()
            .unwrap_or_else(|| Bitmap::new_constant(true, source.len()));
        Self {
            source: source_bitmap,
            valid_count: 0,
            total_count: 0,
            count_vec: Vec::with_capacity(total_length),
            count_mode,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a> WindowAggStateOps<'a> for CountWindowState<'a> {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        self.total_count += end_idx - start_idx;
        if matches!(self.count_mode, CountMode::Valid | CountMode::Null) {
            for i in start_idx..end_idx {
                if self.source.get_bit(i) {
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
                if self.source.get_bit(i) {
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
