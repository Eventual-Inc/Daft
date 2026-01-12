use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex},
};

use arrow::datatypes::DataType;
use daft_arrow::compute::cast;

// TODO(Clark): Refactor to GILOnceCell in order to avoid deadlock between the below mutex and the Python GIL.
static REGISTRY: LazyLock<Mutex<HashMap<std::string::String, daft_arrow::datatypes::DataType>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static ARROW_REGISTRY: LazyLock<Mutex<HashMap<std::string::String, arrow::datatypes::DataType>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn coerce_arrow_to_daft_compatible_schema(
    schema: arrow::datatypes::Schema,
) -> arrow::datatypes::Schema {
    let arrow::datatypes::Schema { fields, metadata } = schema;
    let fields = fields
        .into_iter()
        .map(|field| {
            let data_type = coerce_arrow_to_daft_compatible_type(field);
            match data_type {
                Some(data_type) => Arc::new(field.as_ref().clone().with_data_type(data_type)),
                None => field.clone(),
            }
        })
        .collect::<arrow::datatypes::Fields>();
    arrow::datatypes::Schema::new_with_metadata(fields, metadata)
}

#[deprecated(note = "arrow2 migration")]
fn coerce_arrow_to_daft_compatible_type(
    field: &arrow::datatypes::Field,
) -> Option<arrow::datatypes::DataType> {
    if let Some(extension_type_name) = field.extension_type_name() {
        let inner_dtype = field.data_type();
        ARROW_REGISTRY
            .lock()
            .unwrap()
            .insert(extension_type_name.to_string(), inner_dtype.clone());
        let new_inner_dtype = coerce_arrow_to_daft_compatible_type(field)?;
        return Some(new_inner_dtype);
    }

    match field.data_type() {
        DataType::Utf8 => Some(DataType::LargeUtf8),
        arrow::datatypes::DataType::Binary => Some(DataType::LargeBinary),
        DataType::List(field) => {
            let new_field = match coerce_arrow_to_daft_compatible_type(field) {
                Some(new_inner_dtype) => {
                    Arc::new(field.as_ref().clone().with_data_type(new_inner_dtype))
                }
                None => field.clone(),
            };
            Some(DataType::LargeList(new_field))
        }
        DataType::LargeList(field) => {
            let new_inner_dtype = coerce_arrow_to_daft_compatible_type(field)?;
            Some(DataType::LargeList(Arc::new(
                field.as_ref().clone().with_data_type(new_inner_dtype),
            )))
        }
        DataType::Map(field, sorted) => {
            let new_inner_dtype = coerce_arrow_to_daft_compatible_type(field)?;
            Some(DataType::Map(
                Arc::new(field.as_ref().clone().with_data_type(new_inner_dtype)),
                *sorted,
            ))
        }
        DataType::FixedSizeList(field, size) => {
            let new_inner_dtype = coerce_arrow_to_daft_compatible_type(field)?;
            Some(DataType::FixedSizeList(
                Arc::new(field.as_ref().clone().with_data_type(new_inner_dtype)),
                *size,
            ))
        }
        DataType::Struct(fields) => {
            let new_fields = fields
                .iter()
                .map(|field| match coerce_arrow_to_daft_compatible_type(field) {
                    Some(new_inner_dtype) => {
                        Arc::new(field.as_ref().clone().with_data_type(new_inner_dtype))
                    }
                    None => field.clone(),
                })
                .collect::<arrow::datatypes::Fields>();
            Some(DataType::Struct(new_fields))
        }
        _ => None,
    }
}

#[deprecated(note = "arrow2 migration")]
fn coerce_arrow2_to_daft_compatible_type(
    dtype: &daft_arrow::datatypes::DataType,
) -> Option<daft_arrow::datatypes::DataType> {
    match dtype {
        daft_arrow::datatypes::DataType::Utf8 => Some(daft_arrow::datatypes::DataType::LargeUtf8),
        daft_arrow::datatypes::DataType::Binary => {
            Some(daft_arrow::datatypes::DataType::LargeBinary)
        }
        daft_arrow::datatypes::DataType::List(field) => {
            let new_field = match coerce_arrow2_to_daft_compatible_type(field.data_type()) {
                Some(new_inner_dtype) => Box::new(
                    daft_arrow::datatypes::Field::new(
                        field.name.clone(),
                        new_inner_dtype,
                        field.is_nullable,
                    )
                    .with_metadata(field.metadata.clone()),
                ),
                None => field.clone(),
            };
            Some(daft_arrow::datatypes::DataType::LargeList(new_field))
        }
        daft_arrow::datatypes::DataType::LargeList(field) => {
            let new_inner_dtype = coerce_arrow2_to_daft_compatible_type(field.data_type())?;
            Some(daft_arrow::datatypes::DataType::LargeList(Box::new(
                daft_arrow::datatypes::Field::new(
                    field.name.clone(),
                    new_inner_dtype,
                    field.is_nullable,
                )
                .with_metadata(field.metadata.clone()),
            )))
        }
        daft_arrow::datatypes::DataType::Map(field, sorted) => {
            let new_field = match coerce_arrow2_to_daft_compatible_type(field.data_type()) {
                Some(new_inner_dtype) => Box::new(
                    daft_arrow::datatypes::Field::new(
                        field.name.clone(),
                        new_inner_dtype,
                        field.is_nullable,
                    )
                    .with_metadata(field.metadata.clone()),
                ),
                None => field.clone(),
            };
            Some(daft_arrow::datatypes::DataType::Map(new_field, *sorted))
        }
        daft_arrow::datatypes::DataType::FixedSizeList(field, size) => {
            let new_inner_dtype = coerce_arrow2_to_daft_compatible_type(field.data_type())?;
            Some(daft_arrow::datatypes::DataType::FixedSizeList(
                Box::new(
                    daft_arrow::datatypes::Field::new(
                        field.name.clone(),
                        new_inner_dtype,
                        field.is_nullable,
                    )
                    .with_metadata(field.metadata.clone()),
                ),
                *size,
            ))
        }
        daft_arrow::datatypes::DataType::Struct(fields) => {
            let new_fields = fields
                .iter()
                .map(
                    |field| match coerce_arrow2_to_daft_compatible_type(field.data_type()) {
                        Some(new_inner_dtype) => daft_arrow::datatypes::Field::new(
                            field.name.clone(),
                            new_inner_dtype,
                            field.is_nullable,
                        )
                        .with_metadata(field.metadata.clone()),
                        None => field.clone(),
                    },
                )
                .collect::<Vec<daft_arrow::datatypes::Field>>();
            if &new_fields == fields {
                None
            } else {
                Some(daft_arrow::datatypes::DataType::Struct(new_fields))
            }
        }
        daft_arrow::datatypes::DataType::Extension(name, inner, metadata) => {
            let new_inner_dtype = coerce_arrow2_to_daft_compatible_type(inner.as_ref())?;
            REGISTRY.lock().unwrap().insert(name.clone(), dtype.clone());
            Some(daft_arrow::datatypes::DataType::Extension(
                name.clone(),
                Box::new(new_inner_dtype),
                metadata.clone(),
            ))
        }
        _ => None,
    }
}

#[deprecated(note = "arrow2 migration")]
pub fn cast_arrow2_array_for_daft_if_needed(
    arrow_array: Box<dyn daft_arrow::array::Array>,
) -> Box<dyn daft_arrow::array::Array> {
    match coerce_arrow2_to_daft_compatible_type(arrow_array.data_type()) {
        Some(coerced_dtype) => cast::cast(
            arrow_array.as_ref(),
            &coerced_dtype,
            cast::CastOptions {
                wrapped: true,
                partial: false,
            },
        )
        .unwrap(),
        None => arrow_array,
    }
}

#[deprecated(note = "arrow2 migration")]
fn coerce_arrow2_from_daft_compatible_type(
    dtype: &daft_arrow::datatypes::DataType,
) -> Option<daft_arrow::datatypes::DataType> {
    match dtype {
        daft_arrow::datatypes::DataType::Extension(name, _, _)
            if REGISTRY.lock().unwrap().contains_key(name) =>
        {
            let entry = REGISTRY.lock().unwrap();
            Some(entry.get(name).unwrap().clone())
        }
        _ => None,
    }
}

#[deprecated(note = "arrow2 migration")]
pub fn cast_arrow2_array_from_daft_if_needed(
    arrow_array: Box<dyn daft_arrow::array::Array>,
) -> Box<dyn daft_arrow::array::Array> {
    match coerce_arrow2_from_daft_compatible_type(arrow_array.data_type()) {
        Some(coerced_dtype) => cast::cast(
            arrow_array.as_ref(),
            &coerced_dtype,
            cast::CastOptions {
                wrapped: true,
                partial: false,
            },
        )
        .unwrap(),
        None => arrow_array,
    }
}

#[deprecated(note = "arrow2 migration")]
fn coerce_arrow_from_daft_compatible_type(
    field: &arrow::datatypes::Field,
) -> Option<arrow::datatypes::DataType> {
    if let Some(name) = field.extension_type_name() {
        let lock = ARROW_REGISTRY.lock().unwrap();
        if lock.contains_key(name) {
            return Some(lock.get(name).unwrap().clone());
        }
    }
    None
}

pub fn cast_arrow_array_from_daft_if_needed(
    arrow_array: arrow::array::ArrayRef,
    field: &arrow::datatypes::Field,
) -> arrow::array::ArrayRef {
    let Some(coerced_dtype) = coerce_arrow_from_daft_compatible_type(field) else {
        return arrow_array;
    };

    arrow::compute::cast(arrow_array.as_ref(), &coerced_dtype).unwrap_or_else(|_| {
        panic!(
            "Failed to cast Daft-compatible Arrow Array of dtype {} to actual dtype {}",
            field.data_type(),
            coerced_dtype
        )
    })
}
