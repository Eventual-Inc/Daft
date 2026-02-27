use std::{borrow::Borrow, fmt::Write};

use arrow::{
    array::{
        ArrayRef, OffsetSizeTrait,
        builder::{
            ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, GenericListBuilder,
            GenericStringBuilder, NullBuilder, PrimitiveBuilder, StructBuilder,
        },
    },
    datatypes::{
        ArrowPrimitiveType, DataType, Date32Type, Date64Type, DurationMicrosecondType,
        DurationMillisecondType, DurationNanosecondType, DurationSecondType, Field, Float16Type,
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, IntervalUnit,
        IntervalYearMonthType, Schema, Time32MillisecondType, Time32SecondType,
        Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
        UInt16Type, UInt32Type, UInt64Type,
    },
    error::ArrowError,
    temporal_conversions,
};
use chrono::{Datelike, Timelike};
use daft_decoding::deserialize::{
    deserialize_datetime, deserialize_naive_date, deserialize_naive_datetime,
};
use indexmap::IndexMap;
use num_traits::NumCast;
use simd_json::StaticNode;

use crate::deserializer::Value as BorrowedValue;
const JSON_NULL_VALUE: BorrowedValue = BorrowedValue::Static(StaticNode::Null);

/// Deserialize chunk of JSON records into a chunk of Arrow arrays.
pub fn deserialize_records<'a, A: Borrow<BorrowedValue<'a>>>(
    records: &[A],
    schema: &Schema,
) -> Result<Vec<ArrayRef>, ArrowError> {
    // Allocate mutable arrays.
    let fields: Vec<_> = schema.fields().iter().collect();
    let mut builders: IndexMap<&str, Box<dyn ArrayBuilder>> = fields
        .iter()
        .map(|f| (f.name().as_str(), allocate_array(f, records.len())))
        .collect();
    for record in records {
        match record.borrow() {
            BorrowedValue::Object(record) => {
                for (i, (key, arr)) in builders.iter_mut().enumerate() {
                    let dtype = fields[i].data_type();
                    if let Some(value) = record.get(&**key) {
                        deserialize_into(arr, dtype, &[value]);
                    } else {
                        deserialize_into(arr, dtype, &[&JSON_NULL_VALUE]);
                    }
                }
            }
            _ => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Each line in a newline-delimited JSON file must be a JSON object, but got: {:?}",
                    record.borrow()
                )));
            }
        }
    }

    Ok(builders
        .into_values()
        .map(|mut builder| builder.finish())
        .collect())
}

pub fn allocate_array(f: &Field, length: usize) -> Box<dyn ArrayBuilder> {
    match f.data_type() {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Int8 => Box::new(PrimitiveBuilder::<Int8Type>::with_capacity(length)),
        DataType::Int16 => Box::new(PrimitiveBuilder::<Int16Type>::with_capacity(length)),
        DataType::Int32 => Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(length)),
        DataType::Date32 => Box::new(PrimitiveBuilder::<Date32Type>::with_capacity(length)),
        DataType::Time32(TimeUnit::Second) => {
            Box::new(PrimitiveBuilder::<Time32SecondType>::with_capacity(length))
        }
        DataType::Time32(TimeUnit::Millisecond) => Box::new(PrimitiveBuilder::<
            Time32MillisecondType,
        >::with_capacity(length)),
        DataType::Interval(IntervalUnit::YearMonth) => Box::new(PrimitiveBuilder::<
            IntervalYearMonthType,
        >::with_capacity(length)),
        DataType::Int64 => Box::new(PrimitiveBuilder::<Int64Type>::with_capacity(length)),
        DataType::Date64 => Box::new(PrimitiveBuilder::<Date64Type>::with_capacity(length)),
        DataType::Time64(TimeUnit::Microsecond) => Box::new(PrimitiveBuilder::<
            Time64MicrosecondType,
        >::with_capacity(length)),
        DataType::Time64(TimeUnit::Nanosecond) => Box::new(
            PrimitiveBuilder::<Time64NanosecondType>::with_capacity(length),
        ),
        DataType::Duration(tu) => match tu {
            TimeUnit::Second => Box::new(PrimitiveBuilder::<DurationSecondType>::with_capacity(
                length,
            )),
            TimeUnit::Millisecond => Box::new(
                PrimitiveBuilder::<DurationMillisecondType>::with_capacity(length),
            ),
            TimeUnit::Microsecond => Box::new(
                PrimitiveBuilder::<DurationMicrosecondType>::with_capacity(length),
            ),
            TimeUnit::Nanosecond => Box::new(
                PrimitiveBuilder::<DurationNanosecondType>::with_capacity(length),
            ),
        },
        dt @ DataType::Timestamp(tu, _) => match tu {
            TimeUnit::Second => Box::new(
                PrimitiveBuilder::<TimestampSecondType>::with_capacity(length)
                    .with_data_type(dt.clone()),
            ),
            TimeUnit::Millisecond => Box::new(
                PrimitiveBuilder::<TimestampMillisecondType>::with_capacity(length)
                    .with_data_type(dt.clone()),
            ),
            TimeUnit::Microsecond => Box::new(
                PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(length)
                    .with_data_type(dt.clone()),
            ),
            TimeUnit::Nanosecond => Box::new(
                PrimitiveBuilder::<TimestampNanosecondType>::with_capacity(length)
                    .with_data_type(dt.clone()),
            ),
        },
        DataType::UInt8 => Box::new(PrimitiveBuilder::<UInt8Type>::with_capacity(length)),
        DataType::UInt16 => Box::new(PrimitiveBuilder::<UInt16Type>::with_capacity(length)),
        DataType::UInt32 => Box::new(PrimitiveBuilder::<UInt32Type>::with_capacity(length)),
        DataType::UInt64 => Box::new(PrimitiveBuilder::<UInt64Type>::with_capacity(length)),
        DataType::Float16 => Box::new(PrimitiveBuilder::<Float16Type>::with_capacity(length)),
        DataType::Float32 => Box::new(PrimitiveBuilder::<Float32Type>::with_capacity(length)),
        DataType::Float64 => Box::new(PrimitiveBuilder::<Float64Type>::with_capacity(length)),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(length)),
        DataType::Utf8 => Box::new(GenericStringBuilder::<i32>::with_capacity(length, 1024)),
        DataType::LargeUtf8 => Box::new(GenericStringBuilder::<i64>::with_capacity(length, 1024)),
        DataType::FixedSizeList(inner, size) => {
            let inner_builder = allocate_array(inner.as_ref(), length);
            Box::new(FixedSizeListBuilder::with_capacity(
                inner_builder,
                *size,
                length,
            ))
        }
        DataType::List(inner) => {
            let inner_builder = allocate_array(inner.as_ref(), length);
            Box::new(
                GenericListBuilder::<i32, _>::with_capacity(inner_builder, length)
                    .with_field(inner.clone()),
            )
        }
        DataType::LargeList(inner) => {
            let inner_builder = allocate_array(inner.as_ref(), length);
            Box::new(
                GenericListBuilder::<i64, _>::with_capacity(inner_builder, length)
                    .with_field(inner.clone()),
            )
        }
        DataType::Struct(fields) => Box::new(StructBuilder::from_fields(fields.clone(), length)),
        dt => todo!("Dtype not supported: {:?}", dt),
    }
}

/// Deserialize `rows` by extending them into the given `target`.
pub fn deserialize_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut Box<dyn ArrayBuilder>,
    dtype: &DataType,
    rows: &[A],
) {
    match dtype {
        DataType::Null => {
            let target = target.as_any_mut().downcast_mut::<NullBuilder>().unwrap();
            for _ in 0..rows.len() {
                target.append_null();
            }
        }
        DataType::Boolean => {
            generic_deserialize_into(target, rows, deserialize_boolean_into);
        }
        DataType::Float32 => deserialize_primitive_into::<_, Float32Type>(target, rows),
        DataType::Float64 => deserialize_primitive_into::<_, Float64Type>(target, rows),
        DataType::Float16 => deserialize_primitive_into::<_, Float16Type>(target, rows),
        DataType::Int8 => deserialize_primitive_into::<_, Int8Type>(target, rows),
        DataType::Int16 => deserialize_primitive_into::<_, Int16Type>(target, rows),
        DataType::Int32 => deserialize_primitive_into::<_, Int32Type>(target, rows),
        DataType::Interval(IntervalUnit::YearMonth) => {
            deserialize_primitive_into::<_, IntervalYearMonthType>(target, rows);
        }
        DataType::Date32 => {
            deserialize_date_into::<_, Date32Type>(target, dtype, rows);
        }
        DataType::Time32(TimeUnit::Second) => {
            deserialize_date_into::<_, Time32SecondType>(target, dtype, rows);
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            deserialize_date_into::<_, Time32MillisecondType>(target, dtype, rows);
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            unimplemented!("There is no natural representation of DayTime in JSON.")
        }
        DataType::Int64 => deserialize_primitive_into::<_, Int64Type>(target, rows),
        DataType::Duration(TimeUnit::Second) => {
            deserialize_primitive_into::<_, DurationSecondType>(target, rows);
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            deserialize_primitive_into::<_, DurationMillisecondType>(target, rows);
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            deserialize_primitive_into::<_, DurationMicrosecondType>(target, rows);
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            deserialize_primitive_into::<_, DurationNanosecondType>(target, rows);
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            deserialize_datetime_into::<_, TimestampSecondType>(target, dtype, rows);
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            deserialize_datetime_into::<_, TimestampMillisecondType>(target, dtype, rows);
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            deserialize_datetime_into::<_, TimestampMicrosecondType>(target, dtype, rows);
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            deserialize_datetime_into::<_, TimestampNanosecondType>(target, dtype, rows);
        }
        DataType::Date64 => {
            deserialize_datetime_into::<_, Date64Type>(target, dtype, rows);
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            deserialize_datetime_into::<_, Time64MicrosecondType>(target, dtype, rows);
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            deserialize_datetime_into::<_, Time64NanosecondType>(target, dtype, rows);
        }
        DataType::UInt8 => deserialize_primitive_into::<_, UInt8Type>(target, rows),
        DataType::UInt16 => deserialize_primitive_into::<_, UInt16Type>(target, rows),
        DataType::UInt32 => deserialize_primitive_into::<_, UInt32Type>(target, rows),
        DataType::UInt64 => deserialize_primitive_into::<_, UInt64Type>(target, rows),
        DataType::Utf8 => generic_deserialize_into::<_, GenericStringBuilder<i32>>(
            target,
            rows,
            deserialize_utf8_into,
        ),
        DataType::LargeUtf8 => generic_deserialize_into::<_, GenericStringBuilder<i64>>(
            target,
            rows,
            deserialize_utf8_into,
        ),
        DataType::FixedSizeList(inner, size) => {
            deserialize_fixed_size_list_into(target, inner.data_type(), *size, rows);
        }
        DataType::List(inner) => {
            deserialize_list_into::<i32, _>(target, inner.data_type(), rows);
        }
        DataType::LargeList(inner) => {
            deserialize_list_into::<i64, _>(target, inner.data_type(), rows);
        }
        DataType::Struct(fields) => {
            deserialize_struct_into(target, fields, rows);
        }
        // TODO(Clark): Add support for decimal type.
        // TODO(Clark): Add support for binary and large binary types.
        dt => {
            todo!("Dtype not supported: {:?}", dt)
        }
    }
}

fn deserialize_primitive_into<'a, A: Borrow<BorrowedValue<'a>>, T: ArrowPrimitiveType>(
    target: &mut Box<dyn ArrayBuilder>,
    rows: &[A],
) where
    T::Native: NumCast,
{
    let target = target
        .as_any_mut()
        .downcast_mut::<PrimitiveBuilder<T>>()
        .unwrap();

    for row in rows {
        let value = match row.borrow() {
            BorrowedValue::Static(StaticNode::I64(v)) => NumCast::from(*v),
            BorrowedValue::Static(StaticNode::U64(v)) => NumCast::from(*v),
            BorrowedValue::Static(StaticNode::F64(v)) => NumCast::from(*v),
            BorrowedValue::Static(StaticNode::Bool(v)) => NumCast::from(*v as u8),
            _ => None,
        };
        target.append_option(value);
    }
}

fn generic_deserialize_into<'a, A: Borrow<BorrowedValue<'a>>, M: 'static>(
    target: &mut Box<dyn ArrayBuilder>,
    rows: &[A],
    deserialize_into: fn(&mut M, &[A]),
) {
    deserialize_into(target.as_any_mut().downcast_mut::<M>().unwrap(), rows);
}

fn deserialize_utf8_into<'a, O: OffsetSizeTrait, A: Borrow<BorrowedValue<'a>>>(
    target: &mut GenericStringBuilder<O>,
    rows: &[A],
) {
    let mut scratch = String::new();

    for row in rows {
        match row.borrow() {
            BorrowedValue::String(v) => target.append_value(v.as_ref()),
            BorrowedValue::Static(StaticNode::Bool(v)) => {
                target.append_value(if *v { "true" } else { "false" });
            }
            BorrowedValue::Static(node) if !matches!(node, StaticNode::Null) => {
                write!(scratch, "{node}").unwrap();
                target.append_value(scratch.as_str());
                scratch.clear();
            }
            _ => target.append_null(),
        }
    }
}

fn deserialize_boolean_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut BooleanBuilder,
    rows: &[A],
) {
    for row in rows {
        match row.borrow() {
            BorrowedValue::Static(StaticNode::Bool(v)) => target.append_value(*v),
            _ => target.append_null(),
        }
    }
}

fn get_factor_from_timeunit(time_unit: &TimeUnit) -> u32 {
    match time_unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

fn deserialize_date_into<'a, A: Borrow<BorrowedValue<'a>>, T: ArrowPrimitiveType<Native = i32>>(
    target: &mut Box<dyn ArrayBuilder>,
    dtype: &DataType,
    rows: &[A],
) {
    let target = target
        .as_any_mut()
        .downcast_mut::<PrimitiveBuilder<T>>()
        .unwrap();
    let mut last_fmt_idx = 0;

    for row in rows {
        let value = match row.borrow() {
            BorrowedValue::Static(StaticNode::I64(i)) => i32::try_from(*i).ok(),
            BorrowedValue::Static(StaticNode::U64(i)) => i32::try_from(*i).ok(),
            BorrowedValue::String(v) => match dtype {
                DataType::Time32(tu) => {
                    let factor = get_factor_from_timeunit(tu);
                    v.parse::<chrono::NaiveTime>().ok().map(|x| {
                        (x.hour() * 3_600 * factor
                            + x.minute() * 60 * factor
                            + x.second() * factor
                            + x.nanosecond() / (1_000_000_000 / factor))
                            as i32
                    })
                }
                DataType::Date32 => deserialize_naive_date(v, &mut last_fmt_idx)
                    .map(|x| x.num_days_from_ce() - (temporal_conversions::UNIX_EPOCH_DAY as i32)),
                _ => unreachable!(),
            },
            _ => None,
        };
        target.append_option(value);
    }
}

fn deserialize_datetime_into<
    'a,
    A: Borrow<BorrowedValue<'a>>,
    T: ArrowPrimitiveType<Native = i64>,
>(
    target: &mut Box<dyn ArrayBuilder>,
    dtype: &DataType,
    rows: &[A],
) {
    let target = target
        .as_any_mut()
        .downcast_mut::<PrimitiveBuilder<T>>()
        .unwrap();
    let mut last_fmt_idx = 0;

    for row in rows {
        let value = match row.borrow() {
            BorrowedValue::Static(StaticNode::I64(i)) => Some(*i),
            BorrowedValue::Static(StaticNode::U64(i)) => i64::try_from(*i).ok(),
            BorrowedValue::String(v) => match dtype {
                DataType::Time64(tu) => {
                    let factor = get_factor_from_timeunit(tu) as u64;
                    v.parse::<chrono::NaiveTime>().ok().map(|x| {
                        (x.hour() as u64 * 3_600 * factor
                            + x.minute() as u64 * 60 * factor
                            + x.second() as u64 * factor
                            + x.nanosecond() as u64 / (1_000_000_000 / factor))
                            as i64
                    })
                }
                DataType::Date64 => deserialize_naive_datetime(v, &mut last_fmt_idx)
                    .map(|x| x.and_utc().timestamp_millis()),
                DataType::Timestamp(tu, None) => deserialize_naive_datetime(v, &mut last_fmt_idx)
                    .and_then(|dt| match tu {
                        TimeUnit::Second => Some(dt.and_utc().timestamp()),
                        TimeUnit::Millisecond => Some(dt.and_utc().timestamp_millis()),
                        TimeUnit::Microsecond => Some(dt.and_utc().timestamp_micros()),
                        TimeUnit::Nanosecond => dt.and_utc().timestamp_nanos_opt(),
                    }),
                DataType::Timestamp(tu, Some(tz)) => {
                    let tz = if tz.as_ref() == "Z" { "UTC" } else { tz };
                    let tz = daft_schema::time_unit::parse_offset(tz).unwrap();
                    deserialize_datetime(v, &tz, &mut last_fmt_idx).and_then(|dt| match tu {
                        TimeUnit::Second => Some(dt.timestamp()),
                        TimeUnit::Millisecond => Some(dt.timestamp_millis()),
                        TimeUnit::Microsecond => Some(dt.timestamp_micros()),
                        TimeUnit::Nanosecond => dt.timestamp_nanos_opt(),
                    })
                }
                _ => unreachable!(),
            },
            _ => None,
        };
        target.append_option(value);
    }
}

fn deserialize_list_into<'a, O: OffsetSizeTrait, A: Borrow<BorrowedValue<'a>>>(
    target: &mut Box<dyn ArrayBuilder>,
    inner_dtype: &DataType,
    rows: &[A],
) {
    let target = target
        .as_any_mut()
        .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
        .unwrap();

    for row in rows {
        match row.borrow() {
            BorrowedValue::Array(values) => {
                deserialize_into(target.values(), inner_dtype, values);
                target.append(true);
            }
            _ => {
                target.append(false);
            }
        }
    }
}

fn deserialize_fixed_size_list_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut Box<dyn ArrayBuilder>,
    inner_dtype: &DataType,
    size: i32,
    rows: &[A],
) {
    let target = target
        .as_any_mut()
        .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>()
        .unwrap();

    let null_values: Vec<_> = std::iter::repeat_n(&JSON_NULL_VALUE, size as usize).collect();

    for row in rows {
        match row.borrow() {
            BorrowedValue::Array(value) => {
                if value.len() == size as usize {
                    deserialize_into(target.values(), inner_dtype, value);
                    target.append(true);
                } else {
                    // TODO(Clark): Return an error instead of dropping incorrectly sized lists.
                    // Push placeholder nulls to maintain alignment, then mark as null.
                    deserialize_into(target.values(), inner_dtype, &null_values);
                    target.append(false);
                }
            }
            _ => {
                // Push placeholder nulls to maintain alignment, then mark as null.
                deserialize_into(target.values(), inner_dtype, &null_values);
                target.append(false);
            }
        }
    }
}

fn deserialize_struct_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut Box<dyn ArrayBuilder>,
    fields: &arrow::datatypes::Fields,
    rows: &[A],
) {
    let target = target.as_any_mut().downcast_mut::<StructBuilder>().unwrap();

    // Build a map from struct field name -> accumulated JSON values.
    let mut values: IndexMap<&str, Vec<&BorrowedValue<'a>>> = fields
        .iter()
        .map(|field| (field.name().as_str(), vec![]))
        .collect();

    for row in rows {
        match row.borrow() {
            BorrowedValue::Object(value) => {
                values.iter_mut().for_each(|(s, inner)| {
                    inner.push(value.get(*s).unwrap_or(&JSON_NULL_VALUE));
                });
                target.append(true);
            }
            _ => {
                values
                    .iter_mut()
                    .for_each(|(_, inner)| inner.push(&JSON_NULL_VALUE));
                target.append(false);
            }
        }
    }

    // Then deserialize each field's JSON values buffer to the appropriate Arrow array.
    //
    // Column ordering invariant - this assumes that values and target.field_builders_mut()
    // have aligned columns; we can assume this because:
    // - field_builders_mut() is guaranteed to have the same column ordering as the fields,
    // - values is an ordered map, whose ordering is tied to fields.
    values
        .into_values()
        .zip(fields.iter())
        .zip(target.field_builders_mut().iter_mut())
        .for_each(|((col_values, field), col_builder)| {
            deserialize_into(col_builder, field.data_type(), col_values.as_slice());
        });
}
