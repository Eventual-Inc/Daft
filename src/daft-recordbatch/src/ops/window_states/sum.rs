use std::ops::{Add, Sub};

use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{try_sum_supertype, DaftPrimitiveType},
    prelude::*,
};
use num_traits::Zero;

use super::WindowAggStateOps;

pub struct SumWindowState<T>
where
    T: DaftPrimitiveType,
    T::Native: Zero + Add<Output = T::Native> + Sub<Output = T::Native> + Copy,
{
    source: DataArray<T>,
    sum: T::Native,
    sum_vec: Vec<T::Native>,
}

impl<T> SumWindowState<T>
where
    T: DaftPrimitiveType,
    T::Native: Zero + Add<Output = T::Native> + Sub<Output = T::Native> + Copy,
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
    T: DaftPrimitiveType,
    T::Native: Zero + Add<Output = T::Native> + Sub<Output = T::Native> + Copy,
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
        let field = self.source.field().clone();
        let arrow_array = Box::new(arrow2::array::PrimitiveArray::from_vec(
            self.sum_vec.clone(),
        ));
        Ok(DataArray::<T>::new(field.into(), arrow_array)?.into_series())
    }
}

pub fn create_for_type(
    source: &Series,
    total_length: usize,
) -> DaftResult<Option<Box<dyn WindowAggStateOps>>> {
    match source.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let casted = source.cast(&DataType::Int64)?;
            Ok(Some(Box::new(SumWindowState::<Int64Type>::new(
                &casted,
                total_length,
            ))))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let casted = source.cast(&DataType::UInt64)?;
            Ok(Some(Box::new(SumWindowState::<UInt64Type>::new(
                &casted,
                total_length,
            ))))
        }
        DataType::Float32 => Ok(Some(Box::new(SumWindowState::<Float32Type>::new(
            source,
            total_length,
        )))),
        DataType::Float64 => Ok(Some(Box::new(SumWindowState::<Float64Type>::new(
            source,
            total_length,
        )))),
        DataType::Decimal128(_, _) => {
            let target_type = try_sum_supertype(source.data_type())?;
            let casted = source.cast(&target_type)?;
            Ok(Some(Box::new(SumWindowState::<Decimal128Type>::new(
                &casted,
                total_length,
            ))))
        }
        dt => Err(DaftError::TypeError(format!(
            "Cannot run Sum over type {}",
            dt
        ))),
    }
}
