use common_error::{DaftError, DaftResult};

use super::DataType;

/// Get the data type that the sum of a column of the given data type should be casted to.
pub fn try_sum_supertype(dtype: &DataType) -> DaftResult<DataType> {
    use DataType::*;
    match dtype {
        Int8 | Int16 | Int32 | Int64 => Ok(Int64),
        UInt8 | UInt16 | UInt32 | UInt64 => Ok(UInt64),
        Float32 => Ok(Float32),
        Float64 => Ok(Float64),
        other => Err(DaftError::TypeError(format!(
            "Invalid argument to sum supertype: {}",
            other
        ))),
    }
}

/// Get the data type that the mean of a column of the given data type should be casted to.
pub fn try_mean_supertype(dtype: &DataType) -> DaftResult<DataType> {
    use DataType::*;
    if dtype.is_numeric() {
        Ok(Float64)
    } else {
        Err(DaftError::TypeError(format!(
            "Invalid argument to mean supertype: {}",
            dtype
        )))
    }
}
