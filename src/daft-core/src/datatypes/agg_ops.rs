use std::cmp::min;

use common_error::{DaftError, DaftResult};

use super::DataType;

/// Get the data type that the sum of a column of the given data type should be casted to.
pub fn try_sum_supertype(dtype: &DataType) -> DaftResult<DataType> {
    match dtype {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(DataType::Int64),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(DataType::UInt64)
        }
        DataType::Float32 => Ok(DataType::Float32),
        DataType::Float64 => Ok(DataType::Float64),
        // 38 is the maximum precision for Decimal128, while 19 is the max increase based on 2^64 rows
        DataType::Decimal128(a, b) => Ok(DataType::Decimal128(min(38, *a + 19), *b)),
        other => Err(DaftError::TypeError(format!(
            "Invalid argument to sum supertype: {}",
            other
        ))),
    }
}

/// Get the data type that the mean of a column of the given data type should be casted to.
pub fn try_mean_stddev_aggregation_supertype(dtype: &DataType) -> DaftResult<DataType> {
    if dtype.is_numeric() {
        Ok(DataType::Float64)
    } else {
        Err(DaftError::TypeError(format!(
            "Invalid argument to mean supertype: {}",
            dtype
        )))
    }
}
