use common_error::DaftResult;
use daft_core::prelude::*;
use num_traits::Zero;

use super::WindowAggStateOps;

pub struct SumWindowStateInner<T>
where
    T: DaftNumericType,
{
    source: DataArray<T>,
    sum: T::Native,
    sum_vec: Vec<T::Native>,
}

impl<T> SumWindowStateInner<T>
where
    T: DaftNumericType,
{
    pub fn new(source: &Series, total_length: usize) -> Self {
        let source_array = source.downcast::<DataArray<T>>().unwrap().clone();
        Self {
            source: source_array,
            sum: T::Native::zero(),
            sum_vec: Vec::with_capacity(total_length),
        }
    }
}

impl<T> WindowAggStateOps for SumWindowStateInner<T>
where
    T: DaftNumericType,
    DataArray<T>: IntoSeries,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.sum = self.sum + self.source.get(i).unwrap();
            }
        }
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.sum = self.sum - self.source.get(i).unwrap();
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.sum_vec.push(self.sum);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        Ok(DataArray::<T>::from(("", self.sum_vec.clone())).into_series())
    }
}
