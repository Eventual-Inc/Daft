use std::marker::PhantomData;

use common_error::DaftResult;

use crate::{
    datatypes::{logical::LogicalArray, DaftDataType, DaftLogicalType, Field},
    DataType, IntoSeries,
};

use super::Growable;

pub struct LogicalGrowable<'a, L: DaftLogicalType>
where
    LogicalArray<L>: IntoSeries,
{
    name: String,
    dtype: DataType,
    physical_growable: Box<dyn Growable<<L::PhysicalType as DaftDataType>::ArrayType> + 'a>,
    _phantom: PhantomData<L>,
}

impl<'a, L: DaftLogicalType> LogicalGrowable<'a, L>
where
    LogicalArray<L>: IntoSeries,
{
    pub fn new(
        name: String,
        dtype: &DataType,
        physical_growable: Box<dyn Growable<<L::PhysicalType as DaftDataType>::ArrayType> + 'a>,
    ) -> Self {
        Self {
            name,
            dtype: dtype.clone(),
            physical_growable,
            _phantom: PhantomData,
        }
    }
}

impl<'a, L: DaftLogicalType> Growable<LogicalArray<L>> for LogicalGrowable<'a, L>
where
    LogicalArray<L>: IntoSeries,
{
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.physical_growable.extend(index, start, len);
    }
    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        self.physical_growable.add_nulls(additional)
    }
    #[inline]
    fn build(&mut self) -> DaftResult<LogicalArray<L>> {
        let physical_arr = self.physical_growable.build()?;
        let arr = LogicalArray::<L>::new(
            Field::new(self.name.clone(), self.dtype.clone()),
            physical_arr,
        );
        Ok(arr)
    }
}
