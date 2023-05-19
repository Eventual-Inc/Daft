use crate::datatypes::{DataType, UInt64Array};
use crate::error::DaftError;
use crate::{error::DaftResult, series::Series};

impl Series {
    pub fn explode(&self) -> DaftResult<Series> {
        use DataType::*;
        match self.data_type() {
            List(_) => self.list()?.explode(),
            FixedSizeList(..) => self.fixed_size_list()?.explode(),
            dt => Err(DaftError::TypeError(format!(
                "explode not implemented for {}",
                dt
            ))),
        }
    }

    pub fn arr_lengths(&self) -> DaftResult<UInt64Array> {
        use DataType::*;

        let p = self.as_physical()?;
        match p.data_type() {
            List(_) => p.list()?.lengths(),
            FixedSizeList(..) => p.fixed_size_list()?.lengths(),
            dt => Err(DaftError::TypeError(format!(
                "lengths not implemented for {}",
                dt
            ))),
        }
    }
}
