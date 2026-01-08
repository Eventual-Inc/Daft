use common_error::DaftResult;

use crate::{
    array::DataArray,
    datatypes::{DaftArrowBackedType, UInt64Array},
    kernels::search_sorted,
    prelude::FromArrow,
};

impl<T> DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    pub fn search_sorted(&self, keys: &Self, descending: bool) -> DaftResult<UInt64Array> {
        let array =
            search_sorted::search_sorted(self.data.as_ref(), keys.data.as_ref(), descending)?;

        UInt64Array::from_arrow2(self.field.clone(), Box::new(array))
    }
}
