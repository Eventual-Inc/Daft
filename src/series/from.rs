use crate::{
    datatypes::{DataType, Field},
    error::{DaftError, DaftResult},
    with_match_arrow_daft_types,
};

use super::Series;

use crate::array::BaseArray;

impl TryFrom<(&str, Box<dyn arrow2::array::Array>)> for Series {
    type Error = DaftError;

    fn try_from(item: (&str, Box<dyn arrow2::array::Array>)) -> DaftResult<Self> {
        let (name, array) = item;
        let self_arrow_type = array.data_type();
        let dtype: DataType = self_arrow_type.into();
        let field = Field::new(name, dtype.clone());
        Ok(
            with_match_arrow_daft_types!(dtype, |$T| DataArray::<$T>::new(field.into(), array.into())?.into_series()),
        )
    }
}
