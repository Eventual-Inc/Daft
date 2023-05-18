use crate::{
    array::DataArray,
    datatypes::{DaftArrowBackedType, UInt64Array},
    error::DaftResult,
    kernels::search_sorted,
};

impl<T> DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    pub fn search_sorted(&self, keys: &Self, descending: bool) -> DaftResult<UInt64Array> {
        let array =
            search_sorted::search_sorted(self.data.as_ref(), keys.data.as_ref(), descending)?;

        Ok(DataArray::from((self.name(), Box::new(array))))
    }
}
