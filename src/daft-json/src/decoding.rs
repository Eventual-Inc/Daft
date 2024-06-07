use crate::deserializer::Value as BorrowedValue;
use arrow2::array::{
    Array, MutableArray, MutableBooleanArray, MutableFixedSizeListArray, MutableListArray,
    MutableNullArray, MutablePrimitiveArray, MutableStructArray, MutableUtf8Array,
};
use arrow2::bitmap::MutableBitmap;
use arrow2::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use arrow2::error::{Error, Result};
use arrow2::offset::Offsets;
use arrow2::temporal_conversions;
use arrow2::types::{f16, NativeType, Offset};
use chrono::{Datelike, Timelike};
use daft_decoding::deserialize::{
    deserialize_datetime, deserialize_naive_date, deserialize_naive_datetime,
    get_factor_from_timeunit,
};
use indexmap::IndexMap;
use num_traits::NumCast;
use simd_json::StaticNode;
use std::borrow::Borrow;
use std::fmt::Write;
const JSON_NULL_VALUE: BorrowedValue = BorrowedValue::Static(StaticNode::Null);
/// Deserialize chunk of JSON records into a chunk of Arrow2 arrays.
pub(crate) fn deserialize_records<'a, A: Borrow<BorrowedValue<'a>>>(
    records: &[A],
    schema: &Schema,
    schema_is_projection: bool,
) -> Result<Vec<Box<dyn Array>>> {
    // Allocate mutable arrays.
    let mut results = schema
        .fields
        .iter()
        .map(|f| (f.name.as_str(), allocate_array(f, records.len())))
        .collect::<IndexMap<_, _>>();
    for record in records {
        match record.borrow() {
            BorrowedValue::Object(record) => {
                for (key, value) in record.iter() {
                    let arr = results.get_mut(key.as_ref());
                    if let Some(arr) = arr {
                        deserialize_into(arr, &[value]);
                    } else if !schema_is_projection {
                        // Provided schema is either the full schema or a projection.
                        // If this key isn't in the schema-derived array map AND there was no projection,
                        // we return an error. Otherwise, we drop this key-value pair.
                        return Err(Error::ExternalFormat(format!("unexpected key: '{key}'")));
                    }
                }
            }
            _ => {
                return Err(Error::ExternalFormat(format!(
                "Each line in a newline-delimited JSON file must be a JSON object, but got: {:?}",
                record.borrow()
            )))
            }
        }
    }

    Ok(results.into_values().map(|mut ma| ma.as_box()).collect())
}

pub(crate) fn allocate_array(f: &Field, length: usize) -> Box<dyn MutableArray> {
    match f.data_type() {
        DataType::Null => Box::new(MutableNullArray::new(DataType::Null, 0)),
        DataType::Int8 => Box::new(MutablePrimitiveArray::<i8>::with_capacity(length)),
        DataType::Int16 => Box::new(MutablePrimitiveArray::<i16>::with_capacity(length)),
        dt @ (DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth)) => {
            Box::new(MutablePrimitiveArray::<i32>::with_capacity(length).to(dt.clone()))
        }
        dt @ (DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Timestamp(..)) => {
            Box::new(MutablePrimitiveArray::<i64>::with_capacity(length).to(dt.clone()))
        }
        DataType::UInt8 => Box::new(MutablePrimitiveArray::<u8>::with_capacity(length)),
        DataType::UInt16 => Box::new(MutablePrimitiveArray::<u16>::with_capacity(length)),
        DataType::UInt32 => Box::new(MutablePrimitiveArray::<u32>::with_capacity(length)),
        DataType::UInt64 => Box::new(MutablePrimitiveArray::<u64>::with_capacity(length)),
        DataType::Float16 => Box::new(MutablePrimitiveArray::<f16>::with_capacity(length)),
        DataType::Float32 => Box::new(MutablePrimitiveArray::<f32>::with_capacity(length)),
        DataType::Float64 => Box::new(MutablePrimitiveArray::<f64>::with_capacity(length)),
        DataType::Boolean => Box::new(MutableBooleanArray::with_capacity(length)),
        DataType::Utf8 => Box::new(MutableUtf8Array::<i32>::with_capacity(length)),
        DataType::LargeUtf8 => Box::new(MutableUtf8Array::<i64>::with_capacity(length)),
        DataType::FixedSizeList(inner, size) => Box::new(MutableFixedSizeListArray::new_from(
            allocate_array(inner, length),
            f.data_type().clone(),
            *size,
        )),
        // TODO(Clark): Ensure that these mutable list arrays work correctly and efficiently for arbitrarily nested arrays.
        // TODO(Clark): We have to manually give a non-None bitmap due to a bug in try_extend_from_lengths for
        // mutable list arrays, which will unintentionally drop the validity mask if the bitmap isn't already non-None.
        DataType::List(inner) => Box::new(MutableListArray::new_from_mutable(
            allocate_array(inner, length),
            Offsets::<i32>::with_capacity(length),
            Some(MutableBitmap::with_capacity(length)),
        )),
        DataType::LargeList(inner) => Box::new(MutableListArray::new_from_mutable(
            allocate_array(inner, length),
            Offsets::<i64>::with_capacity(length),
            Some(MutableBitmap::with_capacity(length)),
        )),
        // TODO(Clark): We have to manually give a non-None bitmap due to a bug in MutableStructArray::push(), which will
        // unintentionally drop the first null added to the validity mask if a bitmap hasn't been initialized from the start.
        dt @ DataType::Struct(inner) => Box::new(
            MutableStructArray::try_new(
                dt.clone(),
                inner
                    .iter()
                    .map(|field| allocate_array(field, length))
                    .collect::<Vec<_>>(),
                Some(MutableBitmap::with_capacity(length)),
            )
            .unwrap(),
        ),
        dt => todo!("Dtype not supported: {:?}", dt),
    }
}

/// Deserialize `rows` by extending them into the given `target`
pub(crate) fn deserialize_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut Box<dyn MutableArray>,
    rows: &[A],
) {
    match target.data_type() {
        DataType::Null => {
            // TODO(Clark): Return an error if any of rows are not Value::Null.
            for _ in 0..rows.len() {
                target.push_null()
            }
        }
        DataType::Boolean => generic_deserialize_into(target, rows, deserialize_boolean_into),
        DataType::Float32 => deserialize_primitive_into::<_, f32>(target, rows),
        DataType::Float64 => deserialize_primitive_into::<_, f64>(target, rows),
        DataType::Int8 => deserialize_primitive_into::<_, i8>(target, rows),
        DataType::Int16 => deserialize_primitive_into::<_, i16>(target, rows),
        DataType::Int32 | DataType::Interval(IntervalUnit::YearMonth) => {
            deserialize_primitive_into::<_, i32>(target, rows)
        }
        DataType::Date32 | DataType::Time32(_) => deserialize_date_into(target, rows),
        DataType::Interval(IntervalUnit::DayTime) => {
            unimplemented!("There is no natural representation of DayTime in JSON.")
        }
        DataType::Int64 | DataType::Duration(_) => {
            deserialize_primitive_into::<_, i64>(target, rows)
        }
        DataType::Timestamp(..) | DataType::Date64 | DataType::Time64(_) => {
            deserialize_datetime_into(target, rows)
        }
        DataType::UInt8 => deserialize_primitive_into::<_, u8>(target, rows),
        DataType::UInt16 => deserialize_primitive_into::<_, u16>(target, rows),
        DataType::UInt32 => deserialize_primitive_into::<_, u32>(target, rows),
        DataType::UInt64 => deserialize_primitive_into::<_, u64>(target, rows),
        DataType::Utf8 => generic_deserialize_into::<_, MutableUtf8Array<i32>>(
            target,
            rows,
            deserialize_utf8_into,
        ),
        DataType::LargeUtf8 => generic_deserialize_into::<_, MutableUtf8Array<i64>>(
            target,
            rows,
            deserialize_utf8_into,
        ),
        DataType::FixedSizeList(_, _) => {
            generic_deserialize_into(target, rows, deserialize_fixed_size_list_into)
        }
        DataType::List(_) => deserialize_list_into(
            target
                .as_mut_any()
                .downcast_mut::<MutableListArray<i32, Box<dyn MutableArray>>>()
                .unwrap(),
            rows,
        ),
        DataType::LargeList(_) => deserialize_list_into(
            target
                .as_mut_any()
                .downcast_mut::<MutableListArray<i64, Box<dyn MutableArray>>>()
                .unwrap(),
            rows,
        ),
        DataType::Struct(_) => {
            generic_deserialize_into::<_, MutableStructArray>(target, rows, deserialize_struct_into)
        }
        // TODO(Clark): Add support for decimal type.
        // TODO(Clark): Add support for binary and large binary types.
        dt => {
            todo!("Dtype not supported: {:?}", dt)
        }
    }
}

fn deserialize_primitive_into<'a, A: Borrow<BorrowedValue<'a>>, T: NativeType + NumCast>(
    target: &mut Box<dyn MutableArray>,
    rows: &[A],
) {
    let target = target
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<T>>()
        .unwrap();

    let iter = rows.iter().map(|row| match row.borrow() {
        BorrowedValue::Static(StaticNode::I64(v)) => T::from(*v),
        BorrowedValue::Static(StaticNode::U64(v)) => T::from(*v),
        BorrowedValue::Static(StaticNode::F64(v)) => T::from(*v),
        BorrowedValue::Static(StaticNode::Bool(v)) => T::from(*v as u8),
        _ => None,
    });
    target.extend_trusted_len(iter);
}

fn generic_deserialize_into<'a, A: Borrow<BorrowedValue<'a>>, M: 'static>(
    target: &mut Box<dyn MutableArray>,
    rows: &[A],
    deserialize_into: fn(&mut M, &[A]) -> (),
) {
    deserialize_into(target.as_mut_any().downcast_mut::<M>().unwrap(), rows);
}

fn deserialize_utf8_into<'a, O: Offset, A: Borrow<BorrowedValue<'a>>>(
    target: &mut MutableUtf8Array<O>,
    rows: &[A],
) {
    let mut scratch = String::new();

    for row in rows {
        match row.borrow() {
            BorrowedValue::String(v) => target.push(Some(v.as_ref())),
            BorrowedValue::Static(StaticNode::Bool(v)) => {
                target.push(Some(if *v { "true" } else { "false" }))
            }
            BorrowedValue::Static(node) if !matches!(node, StaticNode::Null) => {
                write!(scratch, "{node}").unwrap();
                target.push(Some(scratch.as_str()));
                scratch.clear();
            }
            _ => target.push_null(),
        }
    }
}

fn deserialize_boolean_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut MutableBooleanArray,
    rows: &[A],
) {
    let iter = rows.iter().map(|row| match row.borrow() {
        BorrowedValue::Static(StaticNode::Bool(v)) => Some(v),
        _ => None,
    });
    target.extend_trusted_len(iter);
}

fn deserialize_date_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut Box<dyn MutableArray>,
    rows: &[A],
) {
    let target = target
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i32>>()
        .unwrap();
    let dtype = target.data_type().clone();
    let mut last_fmt_idx = 0;

    let iter = rows.iter().map(|row| match row.borrow() {
        BorrowedValue::Static(StaticNode::I64(i)) => i32::try_from(*i).ok(),
        BorrowedValue::Static(StaticNode::U64(i)) => i32::try_from(*i).ok(),
        BorrowedValue::String(v) => match dtype {
            DataType::Time32(tu) => {
                let factor = get_factor_from_timeunit(tu);
                v.parse::<chrono::NaiveTime>().ok().map(|x| {
                    (x.hour() * 3_600 * factor
                        + x.minute() * 60 * factor
                        + x.second() * factor
                        + x.nanosecond() / (1_000_000_000 / factor)) as i32
                })
            }
            DataType::Date32 => deserialize_naive_date(v, &mut last_fmt_idx)
                .map(|x| x.num_days_from_ce() - temporal_conversions::EPOCH_DAYS_FROM_CE),
            _ => unreachable!(),
        },
        _ => None,
    });
    target.extend_trusted_len(iter);
}
fn deserialize_datetime_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut Box<dyn MutableArray>,
    rows: &[A],
) {
    let target = target
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i64>>()
        .unwrap();
    let dtype = target.data_type().clone();
    let mut last_fmt_idx = 0;
    let iter = rows.iter().map(|row| match row.borrow() {
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
            DataType::Timestamp(tu, Some(ref tz)) => {
                let tz = if tz == "Z" { "UTC" } else { tz };
                let tz = temporal_conversions::parse_offset(tz).unwrap();
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
    target.extend_trusted_len(iter);
}

fn deserialize_list_into<'a, O: Offset, A: Borrow<BorrowedValue<'a>>>(
    target: &mut MutableListArray<O, Box<dyn MutableArray>>,
    rows: &[A],
) {
    let empty = [];
    let inner: Vec<_> = rows
        .iter()
        .flat_map(|row| match row.borrow() {
            BorrowedValue::Array(value) => value.iter(),
            _ => empty.iter(),
        })
        .collect();

    deserialize_into(target.mut_values(), &inner);

    let lengths = rows.iter().map(|row| match row.borrow() {
        BorrowedValue::Array(value) => Some(value.len()),
        _ => None,
    });

    // NOTE(Clark): A bug in Arrow2 will cause the validity mask to be dropped if it's currently None in target,
    // which will be the case unless we explicitly initialize the mutable array with a bitmap.
    target
        .try_extend_from_lengths(lengths)
        .expect("Offsets overflow");
}

fn deserialize_fixed_size_list_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut MutableFixedSizeListArray<Box<dyn MutableArray>>,
    rows: &[A],
) {
    for row in rows {
        match row.borrow() {
            BorrowedValue::Array(value) => {
                if value.len() == target.size() {
                    deserialize_into(target.mut_values(), value);
                    // Unless alignment is already off, the if above should
                    // prevent this from ever happening.
                    target.try_push_valid().expect("unaligned backing array");
                } else {
                    // TODO(Clark): Return an error instead of dropping incorrectly sized lists.
                    target.push_null();
                }
            }
            _ => target.push_null(),
        }
    }
}

fn deserialize_struct_into<'a, A: Borrow<BorrowedValue<'a>>>(
    target: &mut MutableStructArray,
    rows: &[A],
) {
    let dtype = target.data_type().clone();
    // Build a map from struct field -> JSON values.
    let mut values = match dtype {
        DataType::Struct(fields) => fields
            .into_iter()
            .map(|field| (field.name, vec![]))
            .collect::<IndexMap<_, _>>(),
        _ => unreachable!(),
    };
    rows.iter().for_each(|row| {
        match row.borrow() {
            BorrowedValue::Object(value) => {
                values.iter_mut().for_each(|(s, inner)| {
                    inner.push(value.get(s.as_str()).unwrap_or(&JSON_NULL_VALUE));
                });
                target.push(true);
            }
            _ => {
                values
                    .iter_mut()
                    .for_each(|(_, inner)| inner.push(&JSON_NULL_VALUE));
                target.push(false);
            }
        };
    });
    // Then deserialize each field's JSON values buffer to the appropriate Arrow2 array.
    //
    // Column ordering invariant - this assumes that values and target.mut_values() have aligned columns;
    // we can assume this because:
    // - target.mut_values() is guaranteed to have the same column ordering as target.data_type().fields,
    // - values is an ordered map, whose ordering is tied to target.data_type().fields.
    values
        .into_values()
        .zip(target.mut_values())
        .for_each(|(col_values, col_mut_arr)| deserialize_into(col_mut_arr, col_values.as_slice()));
}
