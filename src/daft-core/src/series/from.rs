use std::sync::Arc;

use crate::{
    datatypes::{DataType, Field},
    with_match_daft_types,
};
use common_error::{DaftError, DaftResult};

use super::Series;

use crate::array::ops::from_arrow::FromArrow;
use crate::series::array_impl::IntoSeries;

impl TryFrom<(&str, Box<dyn arrow2::array::Array>)> for Series {
    type Error = DaftError;

    fn try_from(item: (&str, Box<dyn arrow2::array::Array>)) -> DaftResult<Self> {
        let (name, array) = item;
        let source_arrow_type = array.data_type();
        let dtype: DataType = source_arrow_type.into();
        let field = Arc::new(Field::new(name, dtype.clone()));
        with_match_daft_types!(dtype, |$T| {
            Ok(<$T as DaftDataType>::ArrayType::from_arrow(&field, array)?.into_series())
        })
    }
}
