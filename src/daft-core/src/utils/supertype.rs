use common_error::{DaftError, DaftResult};

use crate::datatypes::{DataType, TimeUnit};

// TODO: Deprecate this logic soon!

fn get_time_units(tu_l: &TimeUnit, tu_r: &TimeUnit) -> TimeUnit {
    match (tu_l, tu_r) {
        (TimeUnit::Nanoseconds, TimeUnit::Microseconds) => TimeUnit::Microseconds,
        (_, TimeUnit::Milliseconds) => TimeUnit::Milliseconds,
        _ => *tu_l,
    }
}

/// Computes the supertype of two types.
pub fn try_get_supertype(l: &DataType, r: &DataType) -> DaftResult<DataType> {
    match get_supertype(l, r) {
        Some(dt) => Ok(dt),
        None => Err(DaftError::TypeError(format!(
            "could not determine supertype of {l:?} and {r:?}"
        ))),
    }
}

/// Computes the supertype of a collection.
pub fn try_get_collection_supertype<'a, I>(types: I) -> DaftResult<DataType>
where
    I: IntoIterator<Item = &'a DataType>,
{
    let mut dtype = DataType::Null;
    for type_ in types {
        dtype = try_get_supertype(&dtype, type_)?;
    }
    Ok(dtype)
}

#[must_use]
pub fn get_supertype(l: &DataType, r: &DataType) -> Option<DataType> {
    fn inner(l: &DataType, r: &DataType) -> Option<DataType> {
        if l == r {
            return Some(l.clone());
        }

        match (l, r) {
            #[cfg(feature = "python")]
            // The supertype of anything and DataType::Python is DataType::Python.
            (_, DataType::Python) => Some(DataType::Python),

            (DataType::Int8, DataType::Boolean) => Some(DataType::Int8),
            (DataType::Int8, DataType::Int16) => Some(DataType::Int16),
            (DataType::Int8, DataType::Int32) => Some(DataType::Int32),
            (DataType::Int8, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int8, DataType::UInt8) => Some(DataType::Int16),
            (DataType::Int8, DataType::UInt16) => Some(DataType::Int32),
            (DataType::Int8, DataType::UInt32) => Some(DataType::Int64),
            (DataType::Int8, DataType::UInt64) => Some(DataType::Float64), // Follow numpy
            (DataType::Int8, DataType::Float32) => Some(DataType::Float32),
            (DataType::Int8, DataType::Float64) => Some(DataType::Float64),

            (DataType::Int16, DataType::Boolean) => Some(DataType::Int16),
            (DataType::Int16, DataType::Int8) => Some(DataType::Int16),
            (DataType::Int16, DataType::Int32) => Some(DataType::Int32),
            (DataType::Int16, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int16, DataType::UInt8) => Some(DataType::Int16),
            (DataType::Int16, DataType::UInt16) => Some(DataType::Int32),
            (DataType::Int16, DataType::UInt32) => Some(DataType::Int64),
            (DataType::Int16, DataType::UInt64) => Some(DataType::Float64), // Follow numpy
            (DataType::Int16, DataType::Float32) => Some(DataType::Float32),
            (DataType::Int16, DataType::Float64) => Some(DataType::Float64),

            (DataType::Int32, DataType::Boolean) => Some(DataType::Int32),
            (DataType::Int32, DataType::Int8) => Some(DataType::Int32),
            (DataType::Int32, DataType::Int16) => Some(DataType::Int32),
            (DataType::Int32, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int32, DataType::UInt8) => Some(DataType::Int32),
            (DataType::Int32, DataType::UInt16) => Some(DataType::Int32),
            (DataType::Int32, DataType::UInt32) => Some(DataType::Int64),
            (DataType::Int32, DataType::UInt64) => Some(DataType::Float64),  // Follow numpy
            (DataType::Int32, DataType::Float32) => Some(DataType::Float64), // Follow numpy
            (DataType::Int32, DataType::Float64) => Some(DataType::Float64),

            (DataType::Int64, DataType::Boolean) => Some(DataType::Int64),
            (DataType::Int64, DataType::Int8) => Some(DataType::Int64),
            (DataType::Int64, DataType::Int16) => Some(DataType::Int64),
            (DataType::Int64, DataType::Int32) => Some(DataType::Int64),
            (DataType::Int64, DataType::UInt8) => Some(DataType::Int64),
            (DataType::Int64, DataType::UInt16) => Some(DataType::Int64),
            (DataType::Int64, DataType::UInt32) => Some(DataType::Int64),
            (DataType::Int64, DataType::UInt64) => Some(DataType::Float64),  // Follow numpy
            (DataType::Int64, DataType::Float32) => Some(DataType::Float64), // Follow numpy
            (DataType::Int64, DataType::Float64) => Some(DataType::Float64),

            (DataType::UInt16, DataType::UInt8) => Some(DataType::UInt16),
            (DataType::UInt16, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::UInt16, DataType::UInt64) => Some(DataType::UInt64),

            (DataType::UInt8, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::UInt8, DataType::UInt64) => Some(DataType::UInt64),
            (DataType::UInt32, DataType::UInt64) => Some(DataType::UInt64),

            (DataType::Boolean, DataType::UInt8) => Some(DataType::UInt8),
            (DataType::Boolean, DataType::UInt16) => Some(DataType::UInt16),
            (DataType::Boolean, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::Boolean, DataType::UInt64) => Some(DataType::UInt64),

            (DataType::Float32, DataType::UInt8) => Some(DataType::Float32),
            (DataType::Float32, DataType::UInt16) => Some(DataType::Float32),
            (DataType::Float32, DataType::UInt32) => Some(DataType::Float64),
            (DataType::Float32, DataType::UInt64) => Some(DataType::Float64),

            (DataType::Float64, DataType::UInt8) => Some(DataType::Float64),
            (DataType::Float64, DataType::UInt16) => Some(DataType::Float64),
            (DataType::Float64, DataType::UInt32) => Some(DataType::Float64),
            (DataType::Float64, DataType::UInt64) => Some(DataType::Float64),

            (DataType::Float64, DataType::Float32) => Some(DataType::Float64),

            (DataType::Date, DataType::UInt8) => Some(DataType::Int64),
            (DataType::Date, DataType::UInt16) => Some(DataType::Int64),
            (DataType::Date, DataType::UInt32) => Some(DataType::Int64),
            (DataType::Date, DataType::UInt64) => Some(DataType::Int64),
            (DataType::Date, DataType::Int8) => Some(DataType::Int32),
            (DataType::Date, DataType::Int16) => Some(DataType::Int32),
            (DataType::Date, DataType::Int32) => Some(DataType::Int32),
            (DataType::Date, DataType::Int64) => Some(DataType::Int64),
            (DataType::Date, DataType::Float32) => Some(DataType::Float32),
            (DataType::Date, DataType::Float64) => Some(DataType::Float64),
            (DataType::Date, DataType::Timestamp(tu, tz)) => Some(DataType::Timestamp(*tu, tz.clone())),

            (DataType::Timestamp(_, _), DataType::UInt32) => Some(DataType::Int64),
            (DataType::Timestamp(_, _), DataType::UInt64) => Some(DataType::Int64),
            (DataType::Timestamp(_, _), DataType::Int32) => Some(DataType::Int64),
            (DataType::Timestamp(_, _), DataType::Int64) => Some(DataType::Int64),
            (DataType::Timestamp(_, _), DataType::Float32) => Some(DataType::Float64),
            (DataType::Timestamp(_, _), DataType::Float64) => Some(DataType::Float64),
            (DataType::Timestamp(tu, tz), DataType::Date) => Some(DataType::Timestamp(*tu, tz.clone())),

            (DataType::Duration(_), DataType::UInt32) => Some(DataType::Int64),
            (DataType::Duration(_), DataType::UInt64) => Some(DataType::Int64),
            (DataType::Duration(_), DataType::Int32) => Some(DataType::Int64),
            (DataType::Duration(_), DataType::Int64) => Some(DataType::Int64),
            (DataType::Duration(_), DataType::Float32) => Some(DataType::Float64),
            (DataType::Duration(_), DataType::Float64) => Some(DataType::Float64),

            (DataType::Time(_), DataType::Int32) => Some(DataType::Int64),
            (DataType::Time(_), DataType::Int64) => Some(DataType::Int64),
            (DataType::Time(_), DataType::Float32) => Some(DataType::Float64),
            (DataType::Time(_), DataType::Float64) => Some(DataType::Float64),

            (DataType::Duration(lu), DataType::Timestamp(ru, Some(tz))) | (DataType::Timestamp(lu, Some(tz)), DataType::Duration(ru)) => {
                if tz.is_empty() {
                    Some(DataType::Timestamp(get_time_units(lu, ru), None))
                } else {
                    Some(DataType::Timestamp(get_time_units(lu, ru), Some(tz.clone())))
                }
            }
            (DataType::Duration(lu), DataType::Timestamp(ru, None)) | (DataType::Timestamp(lu, None), DataType::Duration(ru)) => {
                Some(DataType::Timestamp(get_time_units(lu, ru), None))
            }
            (DataType::Duration(_), DataType::Date) | (DataType::Date, DataType::Duration(_)) => Some(DataType::Date),
            (DataType::Duration(lu), DataType::Duration(ru)) => Some(DataType::Duration(get_time_units(lu, ru))),

            // Some() timezones that are non equal
            // we cast from more precision to higher precision as that always fits with occasional loss of precision
            (DataType::Timestamp(tu_l, Some(tz_l)), DataType::Timestamp(tu_r, Some(tz_r)))
                if !tz_l.is_empty()
                    && !tz_r.is_empty() && tz_l != tz_r =>
            {
                let tu = get_time_units(tu_l, tu_r);
                Some(DataType::Timestamp(tu, Some("UTC".to_string())))
            }
            // None and Some("<tz>") timezones
            // we cast from more precision to higher precision as that always fits with occasional loss of precision
            (DataType::Timestamp(tu_l, tz_l), DataType::Timestamp(tu_r, tz_r)) if
                // both are none
                tz_l.is_none() && tz_r.is_none()
                // both have the same time zone
                || (tz_l.is_some() && (tz_l == tz_r)) => {
                let tu = get_time_units(tu_l, tu_r);
                Some(DataType::Timestamp(tu, tz_r.clone()))
            }

            //TODO(sammy): add time, struct related dtypes
            (DataType::Boolean, DataType::Float32) => Some(DataType::Float32),
            (DataType::Boolean, DataType::Float64) => Some(DataType::Float64),
            (DataType::List(inner_left_dtype), DataType::List(inner_right_dtype)) => {
                let inner_st = get_supertype(inner_left_dtype.as_ref(), inner_right_dtype.as_ref())?;
                Some(DataType::List(Box::new(inner_st)))
            }
            // TODO(Colin): Add support for getting supertype for two maps once StructArray supports such a cast.
            // (Map(inner_left_dtype), Map(inner_right_dtype)) => {
            //     let inner_st = get_supertype(inner_left_dtype.as_ref(), inner_right_dtype.as_ref())?;
            //     Some(DataType::Map(Box::new(inner_st)))
            // }
            // TODO(Clark): Add support for getting supertype for two fixed size lists once Arrow2 supports such a cast.
            // (FixedSizeList(inner_left_field, inner_left_size), FixedSizeList(inner_right_field, inner_right_size)) if inner_left_size == inner_right_size => {
            //     let inner_st = inner(&inner_left_field.dtype, &inner_right_field.dtype)?;
            //     Some(DataType::FixedSizeList(Box::new(Field::new(inner_left_field.name.clone(), inner_st)), *inner_left_size))
            // }
            // TODO(Clark): Add support for getting supertype for a fixed size list and a list once Arrow2 supports such a cast.
            // (FixedSizeList(inner_left_field, _inner_left_size), List(inner_right_field)) => {
            //     let inner_st = get_supertype(&inner_left_field.dtype, &inner_right_field.dtype)?;
            //     Some(DataType::List(Box::new(Field::new(inner_left_field.name.clone(), inner_st))))
            // }

            // every known type can be casted to a string except binary
            (dt, DataType::Utf8) if !matches!(&dt, &DataType::Binary | &DataType::FixedSizeBinary(_) | &DataType::List(_)) => Some(DataType::Utf8),
            (dt, DataType::Null) => Some(dt.clone()), // Drop DataType::Null Type


            _ => None,
        }
    }
    inner(l, r).or_else(|| inner(r, l))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_bad_arrow_type() -> DaftResult<()> {
        let result = get_supertype(&DataType::Utf8, &DataType::Binary);
        assert_eq!(result, None);
        Ok(())
    }
}
