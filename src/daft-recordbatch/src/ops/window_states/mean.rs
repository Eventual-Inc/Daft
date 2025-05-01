use std::marker::PhantomData;

use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::DaftPrimitiveType, prelude::*};

use super::WindowAggStateOps;
use crate::ops::window_states::{CountWindowState, SumWindowState};

pub struct MeanWindowState<'a, T>
where
    T: DaftPrimitiveType,
{
    sum: SumWindowState<'a, T>,
    count: CountWindowState<'a>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, T> MeanWindowState<'a, T>
where
    T: DaftPrimitiveType,
{
    pub fn new(source: &'a Series, total_length: usize) -> DaftResult<Self> {
        Ok(Self {
            sum: SumWindowState::<'a, T>::new(source, total_length)?,
            count: CountWindowState::new(source, total_length),
            _phantom: PhantomData,
        })
    }
}

impl<'a, T> WindowAggStateOps<'a> for MeanWindowState<'a, T>
where
    T: DaftPrimitiveType,
    DataArray<T>: IntoSeries,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        self.sum.add(start_idx, end_idx)?;
        self.count.add(start_idx, end_idx)?;
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

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

        sum_series / count_series
    }
}

pub fn create_for_type<'a>(
    source: &'a Series,
    total_length: usize,
) -> DaftResult<Option<Box<dyn WindowAggStateOps<'a> + 'a>>> {
    match source.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(Some(Box::new(
            MeanWindowState::<'a, Int64Type>::new(source, total_length)?,
        ))),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(Some(Box::new(MeanWindowState::<'a, UInt64Type>::new(
                source,
                total_length,
            )?)))
        }
        DataType::Float32 => Ok(Some(Box::new(MeanWindowState::<'a, Float32Type>::new(
            source,
            total_length,
        )?))),
        DataType::Float64 => Ok(Some(Box::new(MeanWindowState::<'a, Float64Type>::new(
            source,
            total_length,
        )?))),
        DataType::Decimal128(_, _) => Ok(Some(Box::new(
            MeanWindowState::<'a, Decimal128Type>::new(source, total_length)?,
        ))),
        dt => Err(DaftError::TypeError(format!(
            "Cannot run Mean over type {}",
            dt
        ))),
    }
}
