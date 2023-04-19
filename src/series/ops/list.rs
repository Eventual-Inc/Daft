use crate::array::BaseArray;
use crate::datatypes::DataType;
use crate::error::DaftError;
use crate::with_match_arrow_daft_types;
use crate::{error::DaftResult, series::Series};

impl Series {
    pub fn explode(&self) -> DaftResult<(Series, Series)> {
        use DataType::*;
        match self.data_type() {
            List(child) | FixedSizeList(child, ..) => with_match_arrow_daft_types!(
            child.dtype,
            |$T| {
                let (exploded, repeat_idx) = self.list()?.explode::<$T>()?;
                Ok((exploded.into_series(), repeat_idx.into_series()))
            }),
            dt => Err(DaftError::TypeError(format!(
                "explode not implemented for {}",
                dt
            ))),
        }
    }
}
