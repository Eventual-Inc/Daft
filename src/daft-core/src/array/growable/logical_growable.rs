use std::{marker::PhantomData, sync::Arc};

use common_error::DaftResult;

use crate::{
    datatypes::{logical::LogicalArray, DaftDataType, DaftLogicalType, Field},
    DataType, IntoSeries, Series,
};

use super::Growable;

pub struct LogicalGrowable<L: DaftLogicalType>
where
    LogicalArray<L>: IntoSeries,
{
    name: String,
    dtype: DataType,
    physical_growable: Box<dyn Growable>,
    _phantom: PhantomData<L>,
}

impl<'a, L: DaftLogicalType> LogicalGrowable<L>
where
    LogicalArray<L>: IntoSeries,
{
    pub fn new(name: String, dtype: &DataType, physical_growable: Box<dyn Growable>) -> Self {
        Self {
            name,
            dtype: dtype.clone(),
            physical_growable,
            _phantom: PhantomData,
        }
    }
}

impl<'a, L: DaftLogicalType> Growable for LogicalGrowable<L>
where
    LogicalArray<L>: IntoSeries,
{
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.physical_growable.extend(index, start, len);
    }

    fn add_nulls(&mut self, additional: usize) {
        self.physical_growable.add_nulls(additional)
    }

    fn build(&mut self) -> DaftResult<Series> {
        let physical_series = self.physical_growable.build()?;
        let arr = LogicalArray::<L>::new(
            Field::new(self.name, self.dtype),
            *physical_series.downcast::<<L::PhysicalType as DaftDataType>::ArrayType>()?,
        );
        Ok(arr.into_series())
    }
}
