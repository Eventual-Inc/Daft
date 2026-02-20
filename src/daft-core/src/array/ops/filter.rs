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
        use super::full::FullNull;
        use crate::array::growable::{Growable, GrowableArray};

        let keep_bitmap = match mask.nulls() {
            None => mask.to_bitmap(),
            Some(nulls) => &mask.to_bitmap() & nulls.inner(),
        };

        let num_invalid = keep_bitmap.len() - keep_bitmap.count_set_bits();
        if num_invalid == 0 {
            return Ok(self.clone());
        } else if num_invalid == mask.len() {
            return Ok(Self::empty(self.name(), self.data_type()));
        }

        let mut growable = Self::make_growable(
            self.name(),
            self.data_type(),
            vec![self],
            false,
            keep_bitmap.count_set_bits(),
        );

        for (start, end) in keep_bitmap.set_slices() {
            growable.extend(0, start, end - start);
        }

        Ok(growable.build()?.downcast::<Self>()?.clone())
    }
}
