use std::cmp::min;

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
        // 38 is the maximum precision for Decimal128, while 19 is the max increase based on 2^64 rows
        Decimal128(a, b) => Ok(Decimal128(min(38, *a + 19), *b)),
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
