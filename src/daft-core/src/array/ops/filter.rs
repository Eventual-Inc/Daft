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
        let result = daft_arrow::compute::filter::filter(self.data(), mask.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl ListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let arrow_arr = self.to_arrow();
        let filtered = daft_arrow::compute::filter::filter(arrow_arr.as_ref(), mask.as_arrow())?;
        Self::from_arrow(self.field().clone().into(), filtered)
    }
}

impl FixedSizeListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let arrow_arr = self.to_arrow();
        let filtered = daft_arrow::compute::filter::filter(arrow_arr.as_ref(), mask.as_arrow())?;
        Self::from_arrow(self.field().clone().into(), filtered)
    }
}

impl StructArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let arrow_arr = self.to_arrow();
        let filtered = daft_arrow::compute::filter::filter(arrow_arr.as_ref(), mask.as_arrow())?;
        Self::from_arrow(self.field().clone().into(), filtered)
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let arrow_arr = self.to_arrow()?;
        let filtered = daft_arrow::compute::filter::filter(arrow_arr.as_ref(), mask.as_arrow())?;
        Self::from_arrow(self.field().clone().into(), filtered)
    }
}
