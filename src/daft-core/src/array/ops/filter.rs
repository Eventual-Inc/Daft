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

// TODO(desmond): Migrate this to arrow-rs after migrating growable internals. We can't use
// arrow::compute::filter here because PythonArray::to_arrow() pickles Python objects into bytes,
// and from_arrow() unpickles them back into *new* objects, breaking Python object identity.
#[cfg(feature = "python")]
impl PythonArray {
    #[allow(deprecated, reason = "arrow2->arrow migration")]
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        use std::borrow::Cow;

        use daft_arrow::bitmap::utils::SlicesIterator;

        use super::full::FullNull;
        use crate::array::growable::{Growable, GrowableArray};

        let keep_bitmap = match mask.as_arrow2().validity() {
            None => Cow::Borrowed(mask.as_arrow2().values()),
            Some(nulls) => Cow::Owned(mask.as_arrow2().values() & nulls),
        };

        let num_invalid = keep_bitmap.as_ref().unset_bits();
        if num_invalid == 0 {
            return Ok(self.clone());
        } else if num_invalid == mask.len() {
            return Ok(Self::empty(self.name(), self.data_type()));
        }

        let slice_iter = SlicesIterator::new(keep_bitmap.as_ref());
        let mut growable = Self::make_growable(
            self.name(),
            self.data_type(),
            vec![self],
            false,
            slice_iter.slots(),
        );

        for (start_keep, len_keep) in slice_iter {
            growable.extend(0, start_keep, len_keep);
        }

        Ok(growable.build()?.downcast::<Self>()?.clone())
    }
}
