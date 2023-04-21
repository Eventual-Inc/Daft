use crate::array::BaseArray;
use crate::datatypes::DataType;
use crate::error::DaftError;
use crate::{error::DaftResult, series::Series};

impl Series {
    pub fn explode(&self) -> DaftResult<(Series, Series)> {
        use DataType::*;
        match self.data_type() {
            List(_) => {
                let (exploded, repeat_idx) = self.list()?.explode()?;
                Ok((exploded, repeat_idx.into_series()))
            }
            FixedSizeList(..) => {
                let (exploded, repeat_idx) = self.fixed_size_list()?.explode()?;
                Ok((exploded, repeat_idx.into_series()))
            }
            dt => Err(DaftError::TypeError(format!(
                "explode not implemented for {}",
                dt
            ))),
        }
    }
}
