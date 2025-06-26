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
        DataType::Decimal128(p, s) => Ok(DataType::Decimal128(min(38, *p + 19), *s)),
        other => Err(DaftError::TypeError(format!(
            "Invalid argument to sum supertype: {}",
            other
        ))),
    }
}

/// Get the data type that the mean of a column of the given data type should be casted to.
pub fn try_mean_aggregation_supertype(dtype: &DataType) -> DaftResult<DataType> {
    match dtype {
        d if d.is_numeric() => Ok(DataType::Float64),
        DataType::Decimal128(p, s) => {
            // 38 is the maximum precision for Decimal128, while 19 is the max increase based on 2^64 rows
            let p_prime = std::cmp::min(38, *p + 19);

            let s_max = std::cmp::min(p_prime, s + 4);

            if !(1..=38).contains(&p_prime) {
                Err(DaftError::TypeError(
                    format!("Cannot infer supertypes for mean on type: {} result precision: {p_prime} exceed bounds of [1, 38]", dtype)
                ))
            } else if s_max > 38 {
                Err(DaftError::TypeError(
                    format!("Cannot infer supertypes for mean on type: {} result scale: {s_max} exceed bounds of [0, 38]", dtype)
                ))
            } else if s_max > p_prime {
                Err(DaftError::TypeError(
                    format!("Cannot infer supertypes for mean on type: {} result scale: {s_max} exceed precision {p_prime}", dtype)
                ))
            } else {
                Ok(DataType::Decimal128(p_prime, s_max))
            }
        }
        _ => Err(DaftError::TypeError(format!(
            "Mean is not supported for: {}",
            dtype
        ))),
    }
}

/// Get the data type that the stddev of a column of the given data type should be casted to.
pub fn try_stddev_aggregation_supertype(dtype: &DataType) -> DaftResult<DataType> {
    match dtype {
        d if d.is_numeric() => Ok(DataType::Float64),
        DataType::Decimal128(..) => Ok(DataType::Float64),
        _ => Err(DaftError::TypeError(format!(
            "StdDev is not supported for: {}",
            dtype
        ))),
    }
}

/// Get the data type that the skew of a column of the given data type should be casted to.
pub fn try_skew_aggregation_supertype(dtype: &DataType) -> DaftResult<DataType> {
    match dtype {
        d if d.is_numeric() => Ok(DataType::Float64),
        DataType::Decimal128(..) => Ok(DataType::Float64),
        _ => Err(DaftError::TypeError(format!(
            "Skew is not supported for: {}",
            dtype
        ))),
    }
}
