use crate::{series::IntoSeries, with_match_daft_types, DataType, Series};

impl Series {
    pub fn empty(name: &str, dtype: &DataType) -> Self {
        with_match_daft_types!(dtype, |$T| {
            <$T as DaftDataType>::ArrayType::empty(name, dtype).into_series()
        })
    }
}
