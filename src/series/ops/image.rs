use crate::datatypes::DataType;

use crate::{
    error::{DaftError, DaftResult},
    series::Series,
};

impl Series {
    pub fn image_decode(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            DataType::FixedShapeImage(..) => todo!("not impelemented"),
            DataType::Image(..) => Err(DaftError::ValueError(format!(
                "Can't decode bytes into image type without a well-defined image mode and shape, but got: {}", dtype
            ))),
            _ => Err(DaftError::ValueError(format!(
                "Can't decode bytes into a non-image dtype, but got: {}",
                dtype
            ))),
        }
    }
}
