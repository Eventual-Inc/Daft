#![allow(deprecated, reason = "arrow2 migration")]
use std::{borrow::Borrow, fmt::Write, sync::Arc};

use arrow_array::{
    Array, ArrowPrimitiveType, OffsetSizeTrait,
    builder::{
        ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, GenericListBuilder,
        GenericStringBuilder, Int32Builder, LargeListBuilder, ListBuilder, NullBuilder,
        PrimitiveBuilder, StructBuilder, TimestampNanosecondBuilder, make_builder,
    },
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_schema::{DataType, IntervalUnit, Schema, TimeUnit};
use chrono::{Datelike, Timelike};
use daft_arrow::{
    error::{Error, Result},
    temporal_conversions,
};
use daft_decoding::deserialize::{
    deserialize_datetime, deserialize_naive_date, deserialize_naive_datetime,
    get_factor_from_timeunit,
};
use indexmap::IndexMap;
use num_traits::NumCast;
use simd_json::StaticNode;

use crate::deserializer::Value as BorrowedValue;
const JSON_NULL_VALUE: BorrowedValue = BorrowedValue::Static(StaticNode::Null);

/// Deserialize chunk of JSON records into a chunk of Arrow2 arrays.
pub fn deserialize_records<'a, A: Borrow<BorrowedValue<'a>>>(
    records: &[A],
    schema: &Schema,
) -> Result<Vec<Arc<dyn Array>>> {
    // Allocate array builders
    let mut results = schema
        .fields
        .iter()
        .map(|f| {
            (
                f.name().as_str(),
                (f.data_type(), make_builder(f.data_type(), records.len())),
            )
        })
        .collect::<IndexMap<_, _>>();

    for record in records {
        match record.borrow() {
            BorrowedValue::Object(record) => {
                for (key, (dtype, arr)) in &mut results {
                    if let Some(value) = record.get(&**key) {
                        deserialize_into(arr, dtype, &[value]);
                    } else {
                        push_null(arr, dtype);
                    }
                }
            }
            _ => {
                return Err(Error::ExternalFormat(format!(
                    "Each line in a newline-delimited JSON file must be a JSON object, but got: {:?}",
                    record.borrow()
                )));
            }
        }
    }

    Ok(results
        .into_values()
        .map(|(_, mut ma)| ma.finish())
        .collect())
}

/// Deserialize `rows` by extending them into the given `target`
pub fn deserialize_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut Box<dyn ArrayBuilder>,
    dtype: &DataType,
    rows: &[A],
) {
    match dtype {
        DataType::Null => {
            let target = target.as_any_mut().downcast_mut::<NullBuilder>().unwrap();
            // TODO(Clark): Return an error if any of rows are not Value::Null.
            for _ in 0..rows.len() {
                target.append_null();
            }
        }
        DataType::Boolean => {
            generic_deserialize_into(target, dtype, rows, deserialize_boolean_into);
        }
        DataType::Float32 => deserialize_primitive_into::<_, Float32Type>(target, rows),
        DataType::Float64 => deserialize_primitive_into::<_, Float64Type>(target, rows),
        DataType::Int8 => deserialize_primitive_into::<_, Int8Type>(target, rows),
        DataType::Int16 => deserialize_primitive_into::<_, Int16Type>(target, rows),
        DataType::Int32 | DataType::Interval(IntervalUnit::YearMonth) => {
            deserialize_primitive_into::<_, Int32Type>(target, rows);
        }
        DataType::Date32 | DataType::Time32(_) => deserialize_date_into(target, dtype, rows),
        DataType::Interval(IntervalUnit::DayTime) => {
            unimplemented!("There is no natural representation of DayTime in JSON.")
        }
        DataType::Int64 | DataType::Duration(_) => {
            deserialize_primitive_into::<_, Int64Type>(target, rows);
        }
        DataType::Timestamp(..) | DataType::Date64 | DataType::Time64(_) => {
            deserialize_datetime_into(target, dtype, rows);
        }
        DataType::UInt8 => deserialize_primitive_into::<_, UInt8Type>(target, rows),
        DataType::UInt16 => deserialize_primitive_into::<_, UInt16Type>(target, rows),
        DataType::UInt32 => deserialize_primitive_into::<_, UInt32Type>(target, rows),
        DataType::UInt64 => deserialize_primitive_into::<_, UInt64Type>(target, rows),
        DataType::Utf8 => generic_deserialize_into::<_, GenericStringBuilder<i32>>(
            target,
            dtype,
            rows,
            deserialize_utf8_into,
        ),
        DataType::LargeUtf8 => generic_deserialize_into::<_, GenericStringBuilder<i64>>(
            target,
            dtype,
            rows,
            deserialize_utf8_into,
        ),
        DataType::FixedSizeList(inner_field, _) => {
            generic_deserialize_into(
                target,
                inner_field.data_type(),
                rows,
                deserialize_fixed_size_list_into,
            );
        }
        DataType::List(inner_field) => deserialize_list_into(
            target
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap(),
            inner_field.data_type(),
            rows,
        ),
        DataType::LargeList(inner_field) => deserialize_list_into(
            target
                .as_any_mut()
                .downcast_mut::<LargeListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap(),
            inner_field.data_type(),
            rows,
        ),
        DataType::Struct(_) => {
            generic_deserialize_into::<_, StructBuilder>(
                target,
                dtype,
                rows,
                deserialize_struct_into,
            );
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

    let iter = rows.iter().map(|row| match row.borrow() {
        BorrowedValue::Static(StaticNode::I64(v)) => <T::Native as NumCast>::from(*v),
        BorrowedValue::Static(StaticNode::U64(v)) => <T::Native as NumCast>::from(*v),
        BorrowedValue::Static(StaticNode::F64(v)) => <T::Native as NumCast>::from(*v),
        BorrowedValue::Static(StaticNode::Bool(v)) => <T::Native as NumCast>::from(*v as u8),
        _ => None,
    });
    target.extend(iter);
}

fn generic_deserialize_into<'a, A: Borrow<BorrowedValue<'a>>, M: 'static>(
    target: &mut Box<dyn ArrayBuilder>,
    dtype: &DataType,
    rows: &[A],
    deserialize_into: fn(&mut M, &DataType, &[A]) -> (),
) {
    deserialize_into(
        target.as_any_mut().downcast_mut::<M>().unwrap(),
        dtype,
        rows,
    );
}

fn deserialize_utf8_into<'a, O: OffsetSizeTrait, A: Borrow<BorrowedValue<'a>>>(
    target: &mut GenericStringBuilder<O>,
    _: &DataType,
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
    _: &DataType,
    rows: &[A],
) {
    let iter = rows.iter().map(|row| match row.borrow() {
        BorrowedValue::Static(StaticNode::Bool(v)) => Some(*v),
        _ => None,
    });
    target.extend(iter);
}

fn deserialize_date_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut Box<dyn ArrayBuilder>,
    dtype: &DataType,
    rows: &[A],
) {
    let target = target.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
    let mut last_fmt_idx = 0;

    let iter = rows.iter().map(|row| match row.borrow() {
        BorrowedValue::Static(StaticNode::I64(i)) => i32::try_from(*i).ok(),
        BorrowedValue::Static(StaticNode::U64(i)) => i32::try_from(*i).ok(),
        BorrowedValue::String(v) => match dtype {
            DataType::Time32(tu) => {
                let factor = get_factor_from_timeunit(*tu);
                v.parse::<chrono::NaiveTime>().ok().map(|x| {
                    (x.hour() * 3_600 * factor
                        + x.minute() * 60 * factor
                        + x.second() * factor
                        + x.nanosecond() / (1_000_000_000 / factor)) as i32
                })
            }
            DataType::Date32 => deserialize_naive_date(v, &mut last_fmt_idx)
                .map(|x| x.num_days_from_ce() - (temporal_conversions::UNIX_EPOCH_DAY as i32)),
            _ => unreachable!(),
        },
        _ => None,
    });
    target.extend(iter);
}

fn deserialize_datetime_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut Box<dyn ArrayBuilder>,
    dtype: &DataType,
    rows: &[A],
) {
    let target = target
        .as_any_mut()
        .downcast_mut::<TimestampNanosecondBuilder>()
        .unwrap();
    let mut last_fmt_idx = 0;
    let iter = rows.iter().map(|row| match row.borrow() {
        BorrowedValue::Static(StaticNode::I64(i)) => Some(*i),
        BorrowedValue::Static(StaticNode::U64(i)) => i64::try_from(*i).ok(),
        BorrowedValue::String(v) => match dtype {
            DataType::Time64(tu) => {
                let factor = get_factor_from_timeunit(*tu) as u64;
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
    });
    target.extend(iter);
}

fn deserialize_list_into<'a, O: OffsetSizeTrait, A: Borrow<BorrowedValue<'a>>>(
    target: &mut GenericListBuilder<O, Box<dyn ArrayBuilder>>,
    inner_dtype: &DataType,
    rows: &[A],
) {
    for row in rows {
        match row.borrow() {
            BorrowedValue::Array(value) => {
                deserialize_into(target.values(), inner_dtype, value);
                target.append(true);
            }
            _ => target.append(false),
        }
    }
}

fn deserialize_fixed_size_list_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut FixedSizeListBuilder<Box<dyn ArrayBuilder>>,
    inner_dtype: &DataType,
    rows: &[A],
) {
    for row in rows {
        match row.borrow() {
            BorrowedValue::Array(value) => {
                if value.len() == (target.value_length() as usize) {
                    deserialize_into(target.values(), inner_dtype, value);
                    target.append(true);
                } else {
                    // TODO(Clark): Return an error instead of dropping incorrectly sized lists.
                    target.append(false);
                }
            }
            _ => target.append(false),
        }
    }
}

fn deserialize_struct_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut StructBuilder,
    dtype: &DataType,
    rows: &[A],
) {
    let DataType::Struct(fields) = dtype else {
        unreachable!();
    };

    // Build a map from struct field -> JSON values.
    let mut values = fields
        .into_iter()
        .map(|field| (field.name(), vec![]))
        .collect::<IndexMap<_, _>>();
    for row in rows {
        match row.borrow() {
            BorrowedValue::Object(value) => {
                values.iter_mut().for_each(|(s, inner)| {
                    inner.push(value.get(s.as_str()).unwrap_or(&JSON_NULL_VALUE));
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
    // Column ordering invariant - this assumes that values and target.mut_values() have aligned columns;
    // we can assume this because:
    // - target.mut_values() is guaranteed to have the same column ordering as target.data_type().fields,
    // - values is an ordered map, whose ordering is tied to target.data_type().fields.
    for (idx, values) in values.into_values().enumerate() {
        deserialize_into(
            target.field_builder(idx).unwrap(),
            fields[idx].data_type(),
            values.as_slice(),
        );
    }
}

pub fn push_null(target: &mut Box<dyn ArrayBuilder>, dtype: &DataType) {
    match dtype {
        DataType::Null => target
            .as_any_mut()
            .downcast_mut::<NullBuilder>()
            .unwrap()
            .append_null(),
        DataType::Boolean => target
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_null(),
        DataType::Float32 => target
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<Float32Type>>()
            .unwrap()
            .append_null(),
        DataType::Float64 => target
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<Float64Type>>()
            .unwrap()
            .append_null(),
        DataType::Int8 => target
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<Int8Type>>()
            .unwrap()
            .append_null(),
        DataType::Int16 => target
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<Int16Type>>()
            .unwrap()
            .append_null(),
        DataType::Int32 | DataType::Interval(IntervalUnit::YearMonth) => {
            target
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<Int32Type>>()
                .unwrap()
                .append_null();
        }
        DataType::UInt8 => target
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<UInt8Type>>()
            .unwrap()
            .append_null(),
        DataType::UInt16 => target
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<UInt16Type>>()
            .unwrap()
            .append_null(),
        DataType::UInt32 => target
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<UInt32Type>>()
            .unwrap()
            .append_null(),
        DataType::UInt64 => target
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<UInt64Type>>()
            .unwrap()
            .append_null(),
        DataType::Utf8 => target
            .as_any_mut()
            .downcast_mut::<GenericStringBuilder<i32>>()
            .unwrap()
            .append_null(),
        DataType::LargeUtf8 => target
            .as_any_mut()
            .downcast_mut::<GenericStringBuilder<i64>>()
            .unwrap()
            .append_null(),
        DataType::FixedSizeList(_, _) => {
            target
                .as_any_mut()
                .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap()
                .append(false);
        }
        DataType::List(_) => target
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .unwrap()
            .append_null(),
        DataType::LargeList(_) => target
            .as_any_mut()
            .downcast_mut::<LargeListBuilder<Box<dyn ArrayBuilder>>>()
            .unwrap()
            .append_null(),
        DataType::Struct(_) => {
            target
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap()
                .append_null();
        }
        // TODO: Add support for decimal type.
        // TODO: Add support for binary and large binary types.
        dt => {
            todo!("Dtype not supported: {:?}", dt)
        }
    }
}
