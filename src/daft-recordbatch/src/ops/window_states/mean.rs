use std::ops::{AddAssign, SubAssign};

use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{try_mean_aggregation_supertype, DaftPrimitiveType},
    prelude::*,
};
use num_traits::Zero;

use super::WindowAggStateOps;
use crate::ops::window_states::{CountWindowState, SumWindowState};

pub struct MeanWindowState<T>
where
    T: DaftPrimitiveType,
    T::Native: Zero + AddAssign + SubAssign + Copy,
{
    sum: SumWindowState<T>,
    count: CountWindowState,
}

impl<T> MeanWindowState<T>
where
    T: DaftPrimitiveType,
    T::Native: Zero + AddAssign + SubAssign + Copy,
{
    pub fn new(source: &Series, total_length: usize) -> Self {
        Self {
            sum: SumWindowState::<T>::new(source, total_length),
            count: CountWindowState::new(source, total_length, CountMode::Valid),
        }
    }
}

impl<T> WindowAggStateOps for MeanWindowState<T>
where
    T: DaftPrimitiveType,
    DataArray<T>: IntoSeries,
    T::Native: Zero + AddAssign + SubAssign + Copy,
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

pub fn create_for_type(
    source: &Series,
    total_length: usize,
) -> DaftResult<Option<Box<dyn WindowAggStateOps>>> {
    let target_type = try_mean_aggregation_supertype(source.data_type())?;
    match target_type {
        DataType::Float64 => {
            let casted = source.cast(&DataType::Float64)?;
            Ok(Some(Box::new(MeanWindowState::<Float64Type>::new(
                &casted,
                total_length,
            ))))
        }
        DataType::Decimal128(_, _) => {
            let casted = source.cast(&target_type)?;
            Ok(Some(Box::new(MeanWindowState::<Decimal128Type>::new(
                &casted,
                total_length,
            ))))
        }
        dt => Err(DaftError::TypeError(format!(
            "Cannot run Mean over type {}",
            dt
        ))),
    }
}
