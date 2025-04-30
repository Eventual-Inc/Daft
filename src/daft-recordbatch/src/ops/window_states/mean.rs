use common_error::DaftResult;
use daft_core::prelude::*;

use super::{CountWindowStateInner, SumWindowStateInner, WindowAggStateOps};

pub struct MeanWindowStateInner<T>
where
    T: DaftNumericType,
{
    sum: SumWindowStateInner<T>,
    count: CountWindowStateInner,
}

impl<T> MeanWindowStateInner<T>
where
    T: DaftNumericType,
{
    pub fn new(source: &Series, total_length: usize) -> Self {
        Self {
            sum: SumWindowStateInner::<T>::new(source, total_length),
            count: CountWindowStateInner::new(source, total_length),
        }
    }
}

impl<T> WindowAggStateOps for MeanWindowStateInner<T>
where
    T: DaftNumericType,
    DataArray<T>: IntoSeries,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        self.sum.add(start_idx, end_idx)?;
        self.count.add(start_idx, end_idx)?;
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        self.sum.remove(start_idx, end_idx)?;
        self.count.remove(start_idx, end_idx)?;
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.sum.evaluate()?;
        self.count.evaluate()?;
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        let sum_series = self.sum.build()?;
        let count_series = self.count.build()?;

        Ok((sum_series / count_series).unwrap())
    }
}
