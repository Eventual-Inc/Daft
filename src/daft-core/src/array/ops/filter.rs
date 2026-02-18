use common_error::DaftResult;

use super::{as_arrow::AsArrow, from_arrow::FromArrow};
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{BooleanArray, DaftArrowBackedType},
};

impl<T> DataArray<T>
where
    T: DaftArrowBackedType,
{
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow::compute::filter(self.to_arrow().as_ref(), &mask.as_arrow()?)?;

        Self::from_arrow(self.field().clone(), result)
    }
}

impl ListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let filtered = arrow::compute::filter(self.to_arrow()?.as_ref(), &mask.as_arrow()?)?;
        Self::from_arrow(self.field().clone(), filtered)
    }
}

impl FixedSizeListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let filtered = arrow::compute::filter(self.to_arrow()?.as_ref(), &mask.as_arrow()?)?;
        Self::from_arrow(self.field().clone(), filtered)
    }
}

impl StructArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let filtered = arrow::compute::filter(self.to_arrow()?.as_ref(), &mask.as_arrow()?)?;
        Self::from_arrow(self.field().clone(), filtered)
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let filtered = arrow::compute::filter(self.to_arrow()?.as_ref(), &mask.as_arrow()?)?;
        Self::from_arrow(self.field().clone(), filtered)
    }
}
