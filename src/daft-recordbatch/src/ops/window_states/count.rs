use arrow2::bitmap::Bitmap;
use common_error::DaftResult;
use daft_core::prelude::*;

use super::WindowAggStateOps;

pub struct CountWindowState<'a> {
    source: Bitmap,
    count: usize,
    count_vec: Vec<u64>,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> CountWindowState<'a> {
    pub fn new(source: &'a Series, total_length: usize) -> Self {
        let source_bitmap = source
            .validity()
            .cloned()
            .unwrap_or_else(|| Bitmap::new_constant(true, source.len()));
        Self {
            source: source_bitmap,
            count: 0,
            count_vec: Vec::with_capacity(total_length),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a> WindowAggStateOps<'a> for CountWindowState<'a> {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.get_bit(i) {
                self.count += 1;
            }
        }
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.get_bit(i) {
                self.count -= 1;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.count_vec.push(self.count as u64);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        Ok(DataArray::<UInt64Type>::from(("", self.count_vec.clone())).into_series())
    }
}
