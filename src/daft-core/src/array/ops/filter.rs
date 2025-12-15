use std::borrow::Cow;

use common_error::DaftResult;
use daft_arrow::bitmap::utils::SlicesIterator;

use super::{as_arrow::AsArrow, full::FullNull};
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{
        DataArray, FixedSizeListArray, ListArray, StructArray,
        growable::{Growable, GrowableArray},
    },
    datatypes::{BooleanArray, DaftArrayType, DaftArrowBackedType, DataType},
};

impl<T> DataArray<T>
where
    T: DaftArrowBackedType,
{
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = daft_arrow::compute::filter::filter(self.data(), mask.as_arrow2())?;
        Self::try_from((self.field.clone(), result))
    }
}

fn generic_filter<Arr>(
    arr: &Arr,
    mask: &BooleanArray,
    arr_name: &str,
    arr_dtype: &DataType,
) -> DaftResult<Arr>
where
    Arr: FullNull + Clone + GrowableArray + DaftArrayType,
{
    let keep_bitmap = match mask.as_arrow2().validity() {
        None => Cow::Borrowed(mask.as_arrow2().values()),
        Some(validity) => Cow::Owned(mask.as_arrow2().values() & validity),
    };

    let num_invalid = keep_bitmap.as_ref().unset_bits();
    if num_invalid == 0 {
        return Ok(arr.clone());
    } else if num_invalid == mask.len() {
        return Ok(Arr::empty(arr_name, arr_dtype));
    }

    let slice_iter = SlicesIterator::new(keep_bitmap.as_ref());
    let mut growable =
        Arr::make_growable(arr_name, arr_dtype, vec![arr], false, slice_iter.slots());

    for (start_keep, len_keep) in slice_iter {
        growable.extend(0, start_keep, len_keep);
    }

    Ok(growable.build()?.downcast::<Arr>()?.clone())
}

impl ListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        generic_filter(self, mask, self.name(), self.data_type())
    }
}

impl FixedSizeListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        generic_filter(self, mask, self.name(), self.data_type())
    }
}

impl StructArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        generic_filter(self, mask, self.name(), self.data_type())
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        generic_filter(self, mask, self.name(), self.data_type())
    }
}
