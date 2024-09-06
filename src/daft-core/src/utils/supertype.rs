use crate::datatypes::DataType;
use crate::datatypes::TimeUnit;
use common_error::DaftError;
use common_error::DaftResult;

// TODO: Deprecate this logic soon!

fn get_time_units(tu_l: &TimeUnit, tu_r: &TimeUnit) -> TimeUnit {
    use TimeUnit::*;
    match (tu_l, tu_r) {
        (Nanoseconds, Microseconds) => Microseconds,
        (_, Milliseconds) => Milliseconds,
        _ => *tu_l,
    }
}

pub fn try_get_supertype(l: &DataType, r: &DataType) -> DaftResult<DataType> {
    match get_supertype(l, r) {
        Some(dt) => Ok(dt),
        None => Err(DaftError::TypeError(format!(
            "could not determine supertype of {l:?} and {r:?}"
        ))),
    }
}

pub fn get_supertype(l: &DataType, r: &DataType) -> Option<DataType> {
    fn inner(l: &DataType, r: &DataType) -> Option<DataType> {
        use DataType::*;

        if l == r {
            return Some(l.clone());
        }

        match (l, r) {
            #[cfg(feature = "python")]
            // The supertype of anything and Python is Python.
            (_, Python) => Some(Python),

            (Int8, Boolean) => Some(Int8),
            (Int8, Int16) => Some(Int16),
            (Int8, Int32) => Some(Int32),
            (Int8, Int64) => Some(Int64),
            (Int8, UInt8) => Some(Int16),
            (Int8, UInt16) => Some(Int32),
            (Int8, UInt32) => Some(Int64),
            (Int8, UInt64) => Some(Float64), // Follow numpy
            (Int8, Float32) => Some(Float32),
            (Int8, Float64) => Some(Float64),

            (Int16, Boolean) => Some(Int16),
            (Int16, Int8) => Some(Int16),
            (Int16, Int32) => Some(Int32),
            (Int16, Int64) => Some(Int64),
            (Int16, UInt8) => Some(Int16),
            (Int16, UInt16) => Some(Int32),
            (Int16, UInt32) => Some(Int64),
            (Int16, UInt64) => Some(Float64), // Follow numpy
            (Int16, Float32) => Some(Float32),
            (Int16, Float64) => Some(Float64),

            (Int32, Boolean) => Some(Int32),
            (Int32, Int8) => Some(Int32),
            (Int32, Int16) => Some(Int32),
            (Int32, Int64) => Some(Int64),
            (Int32, UInt8) => Some(Int32),
            (Int32, UInt16) => Some(Int32),
            (Int32, UInt32) => Some(Int64),
            (Int32, UInt64) => Some(Float64),  // Follow numpy
            (Int32, Float32) => Some(Float64), // Follow numpy
            (Int32, Float64) => Some(Float64),

            (Int64, Boolean) => Some(Int64),
            (Int64, Int8) => Some(Int64),
            (Int64, Int16) => Some(Int64),
            (Int64, Int32) => Some(Int64),
            (Int64, UInt8) => Some(Int64),
            (Int64, UInt16) => Some(Int64),
            (Int64, UInt32) => Some(Int64),
            (Int64, UInt64) => Some(Float64),  // Follow numpy
            (Int64, Float32) => Some(Float64), // Follow numpy
            (Int64, Float64) => Some(Float64),

            (UInt16, UInt8) => Some(UInt16),
            (UInt16, UInt32) => Some(UInt32),
            (UInt16, UInt64) => Some(UInt64),

            (UInt8, UInt32) => Some(UInt32),
            (UInt8, UInt64) => Some(UInt64),
            (UInt32, UInt64) => Some(UInt64),

            (Boolean, UInt8) => Some(UInt8),
            (Boolean, UInt16) => Some(UInt16),
            (Boolean, UInt32) => Some(UInt32),
            (Boolean, UInt64) => Some(UInt64),

            (Float32, UInt8) => Some(Float32),
            (Float32, UInt16) => Some(Float32),
            (Float32, UInt32) => Some(Float64),
            (Float32, UInt64) => Some(Float64),

            (Float64, UInt8) => Some(Float64),
            (Float64, UInt16) => Some(Float64),
            (Float64, UInt32) => Some(Float64),
            (Float64, UInt64) => Some(Float64),

            (Float64, Float32) => Some(Float64),

            (Date, UInt8) => Some(Int64),
            (Date, UInt16) => Some(Int64),
            (Date, UInt32) => Some(Int64),
            (Date, UInt64) => Some(Int64),
            (Date, Int8) => Some(Int32),
            (Date, Int16) => Some(Int32),
            (Date, Int32) => Some(Int32),
            (Date, Int64) => Some(Int64),
            (Date, Float32) => Some(Float32),
            (Date, Float64) => Some(Float64),
            (Date, Timestamp(tu, tz)) => Some(Timestamp(*tu, tz.clone())),

            (Timestamp(_, _), UInt32) => Some(Int64),
            (Timestamp(_, _), UInt64) => Some(Int64),
            (Timestamp(_, _), Int32) => Some(Int64),
            (Timestamp(_, _), Int64) => Some(Int64),
            (Timestamp(_, _), Float32) => Some(Float64),
            (Timestamp(_, _), Float64) => Some(Float64),
            (Timestamp(tu, tz), Date) => Some(Timestamp(*tu, tz.clone())),

            (Duration(_), UInt32) => Some(Int64),
            (Duration(_), UInt64) => Some(Int64),
            (Duration(_), Int32) => Some(Int64),
            (Duration(_), Int64) => Some(Int64),
            (Duration(_), Float32) => Some(Float64),
            (Duration(_), Float64) => Some(Float64),

            (Time(_), Int32) => Some(Int64),
            (Time(_), Int64) => Some(Int64),
            (Time(_), Float32) => Some(Float64),
            (Time(_), Float64) => Some(Float64),

            (Duration(lu), Timestamp(ru, Some(tz))) | (Timestamp(lu, Some(tz)), Duration(ru)) => {
                if tz.is_empty() {
                    Some(Timestamp(get_time_units(lu, ru), None))
                } else {
                    Some(Timestamp(get_time_units(lu, ru), Some(tz.clone())))
                }
            }
            (Duration(lu), Timestamp(ru, None)) | (Timestamp(lu, None), Duration(ru)) => {
                Some(Timestamp(get_time_units(lu, ru), None))
            }
            (Duration(_), Date) | (Date, Duration(_)) => Some(Date),
            (Duration(lu), Duration(ru)) => Some(Duration(get_time_units(lu, ru))),

            // Some() timezones that are non equal
            // we cast from more precision to higher precision as that always fits with occasional loss of precision
            (Timestamp(tu_l, Some(tz_l)), Timestamp(tu_r, Some(tz_r)))
                if !tz_l.is_empty()
                    && !tz_r.is_empty() && tz_l != tz_r =>
            {
                let tu = get_time_units(tu_l, tu_r);
                Some(Timestamp(tu, Some("UTC".to_string())))
            }
            // None and Some("<tz>") timezones
            // we cast from more precision to higher precision as that always fits with occasional loss of precision
            (Timestamp(tu_l, tz_l), Timestamp(tu_r, tz_r)) if
                // both are none
                tz_l.is_none() && tz_r.is_none()
                // both have the same time zone
                || (tz_l.is_some() && (tz_l == tz_r)) => {
                let tu = get_time_units(tu_l, tu_r);
                Some(Timestamp(tu, tz_r.clone()))
            }

            //TODO(sammy): add time, struct related dtypes
            (Boolean, Float32) => Some(Float32),
            (Boolean, Float64) => Some(Float64),
            (List(inner_left_dtype), List(inner_right_dtype)) => {
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
            (dt, Utf8) if !matches!(&dt, &Binary | &FixedSizeBinary(_)) => Some(Utf8),
            (dt, Null) => Some(dt.clone()), // Drop Null Type


            _ => None,
        }
    }
    match inner(l, r) {
        Some(dt) => Some(dt),
        None => inner(r, l),
    }
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
