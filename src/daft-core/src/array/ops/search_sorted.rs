use std::sync::Arc;

use common_error::DaftResult;

use crate::{
    array::DataArray,
    datatypes::{DaftArrowBackedType, DataType, Field, UInt64Array},
    kernels::search_sorted,
};

impl<T> DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    pub fn search_sorted(&self, keys: &Self, descending: bool) -> DaftResult<UInt64Array> {
        let array = search_sorted::search_sorted(
            self.to_arrow().as_ref(),
            keys.to_arrow().as_ref(),
            descending,
        )?;

        DataArray::from_arrow(
            Field::new(self.field.name.as_str(), DataType::UInt64),
            Arc::new(array),
        )
    }
}
