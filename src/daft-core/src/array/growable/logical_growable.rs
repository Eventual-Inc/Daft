use std::marker::PhantomData;

use common_error::DaftResult;

use crate::{
    datatypes::{logical::LogicalArray, DaftLogicalType, Field, DaftDataType},
    DataType, IntoSeries, Series,
};

use super::Growable;

pub struct LogicalGrowable<'a, L: DaftLogicalType>
where
    LogicalArray<L>: IntoSeries,
{
    name: String,
    dtype: DataType,
    physical_growable: Box<dyn Growable + 'a>,
    _phantom: PhantomData<L>,
}

impl<'a, L: DaftLogicalType> LogicalGrowable<'a, L>
where
    LogicalArray<L>: IntoSeries,
{
    pub fn new(name: String, dtype: &DataType, physical_growable: Box<dyn Growable + 'a>) -> Self {
        Self {
            name,
            dtype: dtype.clone(),
            physical_growable,
            _phantom: PhantomData,
        }
    }
}

impl<'a, L: DaftLogicalType> Growable for LogicalGrowable<'a, L>
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
            Field::new(self.name.clone(), self.dtype.clone()),
            // TODO: is it possible to avoid the clone here with a .downcast_move() -> DataArray?
            // I don't think so because going from Series (multiple owners) to DataArray (single owner)
            // isn't safe to move.
            physical_series.downcast::<<L::PhysicalType as DaftDataType>::ArrayType>()?.clone(),
        );
        Ok(arr.into_series())
    }
}
