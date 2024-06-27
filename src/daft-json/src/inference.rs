use std::{borrow::Borrow, collections::HashSet};

use crate::deserializer::{Object, Value as BorrowedValue};
use arrow2::datatypes::{DataType, Field, Metadata, Schema, TimeUnit};
use arrow2::error::{Error, Result};
use indexmap::IndexMap;

use simd_json::StaticNode;

const ITEM_NAME: &str = "item";

/// Infer Arrow2 schema from JSON Value record.
pub(crate) fn infer_records_schema(record: &BorrowedValue) -> Result<Schema> {
    let fields = match record {
        BorrowedValue::Object(record) => record
            .iter()
            .map(|(name, value)| {
                let data_type = infer(value)?;

                Ok(Field {
                    name: name.to_string(),
                    data_type,
                    is_nullable: true,
                    metadata: Metadata::default(),
                })
            })
            .collect::<Result<Vec<_>>>(),
        _ => Err(Error::ExternalFormat(
            "Deserialized JSON value is not an Object record".to_string(),
        )),
    }?;

    Ok(Schema {
        fields,
        metadata: Metadata::default(),
    })
}

/// Infers [`DataType`] from [`Value`].
fn infer(json: &BorrowedValue) -> Result<DataType> {
    Ok(match json {
        BorrowedValue::Static(StaticNode::Bool(_)) => DataType::Boolean,
        BorrowedValue::Static(StaticNode::Null) => DataType::Null,
        BorrowedValue::Static(StaticNode::I64(_)) => DataType::Int64,
        BorrowedValue::Static(StaticNode::U64(_)) => DataType::UInt64,
        BorrowedValue::Static(StaticNode::F64(_)) => DataType::Float64,
        BorrowedValue::String(s) => infer_string(s),
        BorrowedValue::Array(array) => infer_array(array.as_slice())?,
        BorrowedValue::Object(inner) => infer_object(inner)?,
    })
}

fn infer_string(string: &str) -> DataType {
    daft_decoding::inference::infer_string(string)
}

fn infer_object(inner: &Object) -> Result<DataType> {
    let fields = inner
        .iter()
        .map(|(key, value)| infer(value).map(|dt| (key, dt)))
        .map(|maybe_dt| {
            let (key, dt) = maybe_dt?;
            Ok(Field::new(key.as_ref(), dt, true))
        })
        .collect::<Result<Vec<_>>>()?;
    if fields.is_empty() {
        // Converts empty Structs to structs with a single field named "" and with a NullType
        // This is because Arrow2 MutableStructArray cannot handle empty Structs
        Ok(DataType::Struct(vec![Field::new("", DataType::Null, true)]))
    } else {
        Ok(DataType::Struct(fields))
    }
}

fn infer_array(values: &[BorrowedValue]) -> Result<DataType> {
    let types = values
        .iter()
        .map(infer)
        // Deduplicate dtypes.
        .collect::<Result<HashSet<_>>>()?;

    let dt = if !types.is_empty() {
        coerce_data_type(types)
    } else {
        DataType::Null
    };

    // Don't add a record that contains only nulls.
    Ok(if dt == DataType::Null {
        dt
    } else {
        DataType::List(Box::new(Field::new(ITEM_NAME, dt, true)))
    })
}

/// Convert each column's set of inferred dtypes to a field with a consolidated dtype, following the coercion rules
/// defined in coerce_data_type.
pub(crate) fn column_types_map_to_fields(
    column_types: IndexMap<String, HashSet<arrow2::datatypes::DataType>>,
) -> Vec<arrow2::datatypes::Field> {
    column_types
        .into_iter()
        .map(|(name, dtype_set)| {
            // Get consolidated dtype for column.
            let dtype = coerce_data_type(dtype_set);
            arrow2::datatypes::Field::new(name, dtype, true)
        })
        .collect::<Vec<_>>()
}

/// Coerce an heterogeneous set of [`DataType`] into a single one. Rules:
/// * The empty set is coerced to `Null`
/// * `Int64` and `Float64` are `Float64`
/// * Lists and scalars are coerced to a list of a compatible scalar
/// * Structs contain the union of all fields
/// * All other types are coerced to `Utf8`
pub(crate) fn coerce_data_type(mut datatypes: HashSet<DataType>) -> DataType {
    // Drop null dtype from the dtype set.
    datatypes.remove(&DataType::Null);

    if datatypes.is_empty() {
        return DataType::Null;
    }

    // All equal.
    if datatypes.len() == 1 {
        return datatypes.into_iter().next().unwrap();
    }

    let are_all_structs = datatypes
        .iter()
        .all(|x| matches!((*x).borrow(), DataType::Struct(_)));

    if are_all_structs {
        // All structs => union of all field dtypes (these may have equal names).
        let fields = datatypes.into_iter().fold(vec![], |mut acc, dt| {
            if let DataType::Struct(new_fields) = dt {
                acc.extend(new_fields);
            };
            acc
        });
        // Group fields by unique names.
        let fields = fields.into_iter().fold(
            IndexMap::<String, HashSet<DataType>>::new(),
            |mut acc, field| {
                match acc.entry(field.name) {
                    indexmap::map::Entry::Occupied(mut v) => {
                        v.get_mut().insert(field.data_type);
                    }
                    indexmap::map::Entry::Vacant(v) => {
                        let mut a = HashSet::new();
                        a.insert(field.data_type);
                        v.insert(a);
                    }
                }
                acc
            },
        );
        // Coerce dtype set for each field.
        let fields = fields
            .into_iter()
            .map(|(name, dts)| Field::new(name, coerce_data_type(dts), true))
            .filter(|f| !f.name.is_empty() && f.data_type != DataType::Null)
            .collect();
        return DataType::Struct(fields);
    }
    datatypes
        .into_iter()
        .reduce(|lhs, rhs| {
            match (lhs, rhs) {
                (lhs, rhs) if lhs == rhs => lhs,
                (DataType::Utf8, _) | (_, DataType::Utf8) => DataType::Utf8,
                (DataType::List(lhs), DataType::List(rhs)) => {
                    let inner =
                        coerce_data_type([lhs.data_type().clone(), rhs.data_type().clone()].into());
                    DataType::List(Box::new(Field::new(ITEM_NAME, inner, true)))
                }
                (scalar, DataType::List(list)) | (DataType::List(list), scalar) => {
                    let inner = coerce_data_type([scalar, list.data_type().clone()].into());
                    DataType::List(Box::new(Field::new(ITEM_NAME, inner, true)))
                }
                (DataType::Float64, DataType::Int64) | (DataType::Int64, DataType::Float64) => {
                    DataType::Float64
                }
                (DataType::Int64, DataType::Boolean) | (DataType::Boolean, DataType::Int64) => {
                    DataType::Int64
                }
                (DataType::Time64(left_tu), DataType::Time64(right_tu)) => {
                    // Set unified time unit to the lowest granularity time unit.
                    let unified_tu = if left_tu == right_tu
                        || time_unit_to_ordinal(&left_tu) < time_unit_to_ordinal(&right_tu)
                    {
                        left_tu
                    } else {
                        right_tu
                    };
                    DataType::Time64(unified_tu)
                }
                (
                    DataType::Timestamp(left_tu, left_tz),
                    DataType::Timestamp(right_tu, right_tz),
                ) => {
                    // Set unified time unit to the lowest granularity time unit.
                    let unified_tu = if left_tu == right_tu
                        || time_unit_to_ordinal(&left_tu) < time_unit_to_ordinal(&right_tu)
                    {
                        left_tu
                    } else {
                        right_tu
                    };
                    // Set unified time zone to UTC.
                    let unified_tz = match (&left_tz, &right_tz) {
                        (None, None) => None,
                        (None, _) | (_, None) => return DataType::Utf8,
                        (Some(l), Some(r)) if l == r => left_tz,
                        (Some(_), Some(_)) => Some("Z".to_string()),
                    };
                    DataType::Timestamp(unified_tu, unified_tz)
                }
                (DataType::Timestamp(_, None), DataType::Date32)
                | (DataType::Date32, DataType::Timestamp(_, None)) => {
                    DataType::Timestamp(TimeUnit::Second, None)
                }
                (_, _) => DataType::Utf8,
            }
        })
        .unwrap()
}

fn time_unit_to_ordinal(tu: &TimeUnit) -> usize {
    match tu {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}
