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
        DataType::Decimal128(_, s) => Ok(DataType::Decimal128(38, *s)),
        other => Err(DaftError::TypeError(format!(
            "Invalid argument to sum supertype: {}",
            other
        ))),
    }
}

/// Get the data type that the product of a column of the given data type should be casted to.
pub fn try_product_supertype(dtype: &DataType) -> DaftResult<DataType> {
    match dtype {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(DataType::Int64),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(DataType::UInt64)
        }
        DataType::Float32 => Ok(DataType::Float32),
        DataType::Float64 => Ok(DataType::Float64),
        DataType::Decimal128(_, s) => Ok(DataType::Decimal128(38, *s)),
        other => Err(DaftError::TypeError(format!(
            "Invalid argument to product supertype: {}",
            other
        ))),
    }
}

/// Get the data type that the mean of a column of the given data type should be casted to.
pub fn try_mean_aggregation_supertype(dtype: &DataType) -> DaftResult<DataType> {
    match dtype {
        d if d.is_numeric() => Ok(DataType::Float64),
        DataType::Decimal128(_, s) => {
            const P_PRIME: usize = 38;

            let s_max = std::cmp::min(P_PRIME, s + 4);

            if s_max > 38 {
                Err(DaftError::TypeError(format!(
                    "Cannot infer supertypes for mean on type: {} result scale: {s_max} exceed bounds of [0, 38]",
                    dtype
                )))
            } else if s_max > P_PRIME {
                Err(DaftError::TypeError(format!(
                    "Cannot infer supertypes for mean on type: {} result scale: {s_max} exceed precision {P_PRIME}",
                    dtype
                )))
            } else {
                Ok(DataType::Decimal128(P_PRIME, s_max))
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
