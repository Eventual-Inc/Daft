use std::{
    marker::PhantomData,
    ops::{Add, Sub},
};

use arrow2::bitmap::MutableBitmap;
use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::DaftPrimitiveType, prelude::*};
use num_traits::Zero;

use super::WindowAggStateOps;

pub struct SumWindowState<'a, T>
where
    T: DaftPrimitiveType,
    T::Native: Zero + Add<Output = T::Native> + Sub<Output = T::Native> + Copy,
{
    source: DataArray<T>,
    sum: T::Native,
    sum_vec: Vec<T::Native>,
    valid_count: usize,
    validity: MutableBitmap,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, T> SumWindowState<'a, T>
where
    T: DaftPrimitiveType,
    T::Native: Zero + Add<Output = T::Native> + Sub<Output = T::Native> + Copy,
{
    pub fn new(source: &'a Series, total_length: usize) -> DaftResult<Self> {
        let source_array = source.downcast::<DataArray<T>>().cloned().or_else(
            |_| -> Result<DataArray<T>, DaftError> {
                let target_type = T::get_dtype();
                source
                    .cast(&target_type)
                    .and_then(|casted| casted.downcast::<DataArray<T>>().cloned())
            },
        )?; // unsure whether or_else would ever trigger

        Ok(Self {
            source: source_array,
            sum: T::Native::zero(),
            sum_vec: Vec::with_capacity(total_length),
            valid_count: 0,
            validity: MutableBitmap::with_capacity(total_length),
            _phantom: PhantomData,
        })
    }
}

impl<'a, T> WindowAggStateOps<'a> for SumWindowState<'a, T>
where
    T: DaftPrimitiveType,
    T::Native: Zero + Add<Output = T::Native> + Sub<Output = T::Native> + Copy,
    DataArray<T>: IntoSeries,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        // if end_idx <= start_idx {
        //     return Err(DaftError::ValueError(
        //         "end_idx must be greater than start_idx".into(),
        //     ));
        // }
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.sum = self.sum + self.source.get(i).unwrap();
                self.valid_count += 1;
            }
        }
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        // if end_idx <= start_idx {
        //     return Err(DaftError::ValueError(
        //         "end_idx must be greater than start_idx".into(),
        //     ));
        // }
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.sum = self.sum - self.source.get(i).unwrap();
                self.valid_count -= 1;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.sum_vec.push(self.sum);
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

pub fn create_for_type<'a>(
    source: &'a Series,
    total_length: usize,
) -> DaftResult<Option<Box<dyn WindowAggStateOps<'a> + 'a>>> {
    match source.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(Some(Box::new(
            SumWindowState::<'a, Int64Type>::new(source, total_length)?,
        ))),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => Ok(Some(
            Box::new(SumWindowState::<'a, UInt64Type>::new(source, total_length)?),
        )),
        DataType::Float32 => Ok(Some(Box::new(SumWindowState::<'a, Float32Type>::new(
            source,
            total_length,
        )?))),
        DataType::Float64 => Ok(Some(Box::new(SumWindowState::<'a, Float64Type>::new(
            source,
            total_length,
        )?))),
        DataType::Decimal128(_, _) => Ok(Some(Box::new(
            SumWindowState::<'a, Decimal128Type>::new(source, total_length)?,
        ))),
        dt => Err(DaftError::TypeError(format!(
            "Cannot run Sum over type {}",
            dt
        ))),
    }
}
