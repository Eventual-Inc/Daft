use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use num_traits::Zero;

use super::WindowAggStateOps;

pub struct SumWindowState<T>
where
    T: DaftNumericType,
{
    source: DataArray<T>,
    sum: T::Native,
    sum_vec: Vec<T::Native>,
}

impl<T> SumWindowState<T>
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

impl<T> WindowAggStateOps for SumWindowState<T>
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

pub fn create_for_type(
    source: &Series,
    total_length: usize,
) -> DaftResult<Box<dyn WindowAggStateOps>> {
    match source.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let casted = source.cast(&DataType::Int64)?;
            Ok(Box::new(SumWindowState::<Int64Type>::new(
                &casted,
                total_length,
            )))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let casted = source.cast(&DataType::UInt64)?;
            Ok(Box::new(SumWindowState::<UInt64Type>::new(
                &casted,
                total_length,
            )))
        }
        DataType::Float32 => Ok(Box::new(SumWindowState::<Float32Type>::new(
            source,
            total_length,
        ))),
        DataType::Float64 => Ok(Box::new(SumWindowState::<Float64Type>::new(
            source,
            total_length,
        ))),
        dt => Err(DaftError::TypeError(format!(
            "Cannot run Sum over type {}",
            dt
        ))),
    }
}
