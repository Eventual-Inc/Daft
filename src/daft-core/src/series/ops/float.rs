use crate::{series::Series, with_match_float_and_null_daft_types};

use common_error::DaftResult;

use crate::series::array_impl::IntoSeries;

impl Series {
    pub fn is_nan(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftIsNan;
        with_match_float_and_null_daft_types!(self.data_type(), |$T| {
            Ok(DaftIsNan::is_nan(self.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
        })
    }
}
