use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use super::WindowAggStateOps;
use crate::ops::window_states::{CountWindowState, SumWindowState};

pub struct MeanWindowState<T>
where
    T: DaftNumericType,
{
    sum: SumWindowState<T>,
    count: CountWindowState,
}

impl<T> MeanWindowState<T>
where
    T: DaftNumericType,
{
    pub fn new(source: &Series, total_length: usize) -> Self {
        Self {
            sum: SumWindowState::<T>::new(source, total_length),
            count: CountWindowState::new(source, total_length),
        }
    }
}

impl<T> WindowAggStateOps for MeanWindowState<T>
where
    T: DaftNumericType,
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

        self.sum.add(start_idx, end_idx)?;
        self.count.add(start_idx, end_idx)?;
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
) -> DaftResult<Box<dyn WindowAggStateOps>> {
    match source.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let casted = source.cast(&DataType::Int64)?;
            Ok(Box::new(MeanWindowState::<Int64Type>::new(
                &casted,
                total_length,
            )))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let casted = source.cast(&DataType::UInt64)?;
            Ok(Box::new(MeanWindowState::<UInt64Type>::new(
                &casted,
                total_length,
            )))
        }
        DataType::Float32 => Ok(Box::new(MeanWindowState::<Float32Type>::new(
            source,
            total_length,
        ))),
        DataType::Float64 => Ok(Box::new(MeanWindowState::<Float64Type>::new(
            source,
            total_length,
        ))),
        dt => Err(DaftError::TypeError(format!(
            "Cannot run Mean over type {}",
            dt
        ))),
    }
}
