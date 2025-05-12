use std::ops::{AddAssign, SubAssign};

use arrow2::bitmap::MutableBitmap;
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::DaftIsNan,
    datatypes::{try_sum_supertype, DaftPrimitiveType},
    prelude::*,
};
use num_traits::{FromPrimitive, Zero};

use super::WindowAggStateOps;
use crate::RecordBatch;

pub struct SumWindowState<T>
where
    T: DaftPrimitiveType,
    T::Native: Zero + AddAssign + SubAssign + Copy,
{
    source: DataArray<T>,
    is_nan: Option<DataArray<BooleanType>>,
    sum: T::Native,
    sum_vec: Vec<T::Native>,
    valid_count: usize,
    nan_count: usize,
    validity: MutableBitmap,
}

impl<T> SumWindowState<T>
where
    T: DaftPrimitiveType,
    T::Native: Zero + AddAssign + SubAssign + Copy,
{
    pub fn new(source: &Series, total_length: usize) -> Self {
        let source_array = source.downcast::<DataArray<T>>().unwrap().clone();
        let is_nan = match source.data_type() {
            DataType::Float32 => {
                let float_array = source.downcast::<DataArray<Float32Type>>().unwrap();
                Some(DaftIsNan::is_nan(float_array).unwrap())
            }
            DataType::Float64 => {
                let float_array = source.downcast::<DataArray<Float64Type>>().unwrap();
                Some(DaftIsNan::is_nan(float_array).unwrap())
            }
            _ => None,
        };
        Self {
            source: source_array,
            is_nan,
            sum: T::Native::zero(),
            sum_vec: Vec::with_capacity(total_length),
            valid_count: 0,
            nan_count: 0,
            validity: MutableBitmap::with_capacity(total_length),
        }
    }
}

impl<T> WindowAggStateOps for SumWindowState<T>
where
    T: DaftPrimitiveType,
    T::Native: Zero + AddAssign + SubAssign + Copy + FromPrimitive,
    DataArray<T>: IntoSeries,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                let value = self.source.get(i).unwrap();
                if let Some(is_nan) = &self.is_nan
                    && is_nan.get(i).unwrap()
                {
                    self.nan_count += 1;
                } else {
                    self.sum += value;
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

        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                let value = self.source.get(i).unwrap();
                if let Some(is_nan) = &self.is_nan
                    && is_nan.get(i).unwrap()
                {
                    self.nan_count -= 1;
                } else {
                    self.sum -= value;
                    self.valid_count -= 1;
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        if self.nan_count > 0 {
            self.sum_vec
                .push(T::Native::from_f64(f64::NAN).unwrap_or_else(T::Native::zero));
        } else {
            self.sum_vec.push(self.sum);
        }
        self.validity.push(self.valid_count > 0);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        let field = self.source.field().clone();
        let arrow_array = Box::new(arrow2::array::PrimitiveArray::from_vec(
            self.sum_vec.clone(),
        ));
        DataArray::<T>::new(field.into(), arrow_array)?
            .into_series()
            .with_validity(Some(self.validity.clone().into()))
    }
}

pub fn create_for_type(
    sources: &RecordBatch,
    total_length: usize,
) -> DaftResult<Option<Box<dyn WindowAggStateOps>>> {
    let [source] = sources.columns() else {
        unreachable!("sum should only have one input")
    };

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
