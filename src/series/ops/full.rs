use crate::array::BaseArray;
use crate::{datatypes::DataType, error::DaftResult, series::Series, with_match_daft_types};

impl Series {
    pub fn empty(name: &str, datatype: &DataType) -> DaftResult<Self> {
        with_match_daft_types!(datatype, |$T| {
            Ok(DataArray::<$T>::empty(name, datatype).into_series())
        })
    }
}
