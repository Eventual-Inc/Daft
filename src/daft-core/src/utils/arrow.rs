use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex},
};

use daft_arrow::{arrow_array, compute::cast};

// TODO(Clark): Refactor to GILOnceCell in order to avoid deadlock between the below mutex and the Python GIL.
static REGISTRY: LazyLock<Mutex<HashMap<std::string::String, daft_arrow::datatypes::DataType>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn coerce_to_daft_compatible_schema(
    schema: daft_arrow::datatypes::Schema,
) -> daft_arrow::datatypes::Schema {
    let fields = schema
        .fields
        .into_iter()
        .map(|field| {
            let data_type =
                coerce_to_daft_compatible_type(field.data_type()).unwrap_or(field.data_type);
            daft_arrow::datatypes::Field::new(field.name, data_type, field.is_nullable)
                .with_metadata(field.metadata)
        })
        .collect::<Vec<_>>();
    daft_arrow::datatypes::Schema {
        fields,
        metadata: schema.metadata,
    }
}

fn coerce_to_daft_compatible_type(
    dtype: &daft_arrow::datatypes::DataType,
) -> Option<daft_arrow::datatypes::DataType> {
    match dtype {
        daft_arrow::datatypes::DataType::Utf8 => Some(daft_arrow::datatypes::DataType::LargeUtf8),
        daft_arrow::datatypes::DataType::Binary => {
            Some(daft_arrow::datatypes::DataType::LargeBinary)
        }
        daft_arrow::datatypes::DataType::List(field) => {
            let new_field = match coerce_to_daft_compatible_type(field.data_type()) {
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
            let new_inner_dtype = coerce_to_daft_compatible_type(field.data_type())?;
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
            let new_field = match coerce_to_daft_compatible_type(field.data_type()) {
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
            let new_inner_dtype = coerce_to_daft_compatible_type(field.data_type())?;
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
                    |field| match coerce_to_daft_compatible_type(field.data_type()) {
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
            let new_inner_dtype = coerce_to_daft_compatible_type(inner.as_ref())?;
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

fn coerce_arrow_to_daft_compatible_type(
    dtype: &arrow::datatypes::DataType,
) -> Option<arrow::datatypes::DataType> {
    match dtype {
        arrow::datatypes::DataType::Utf8 => Some(arrow::datatypes::DataType::LargeUtf8),
        arrow::datatypes::DataType::Binary => Some(arrow::datatypes::DataType::LargeBinary),
        arrow::datatypes::DataType::List(field) => {
            let new_field = match coerce_arrow_to_daft_compatible_type(field.data_type()) {
                Some(new_inner_dtype) => Arc::new(
                    arrow::datatypes::Field::new(
                        field.name().clone(),
                        new_inner_dtype,
                        field.is_nullable(),
                    )
                    .with_metadata(field.metadata().clone()),
                ),
                None => field.clone(),
            };
            Some(arrow::datatypes::DataType::LargeList(new_field))
        }
        arrow::datatypes::DataType::LargeList(field) => {
            let new_inner_dtype = coerce_arrow_to_daft_compatible_type(field.data_type())?;
            Some(arrow::datatypes::DataType::LargeList(Arc::new(
                arrow::datatypes::Field::new(
                    field.name().clone(),
                    new_inner_dtype,
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            )))
        }
        arrow::datatypes::DataType::Map(field, sorted) => {
            let new_field = match coerce_arrow_to_daft_compatible_type(field.data_type()) {
                Some(new_inner_dtype) => Arc::new(
                    arrow::datatypes::Field::new(
                        field.name().clone(),
                        new_inner_dtype,
                        field.is_nullable(),
                    )
                    .with_metadata(field.metadata().clone()),
                ),
                None => field.clone(),
            };
            Some(arrow::datatypes::DataType::Map(new_field, *sorted))
        }
        arrow::datatypes::DataType::FixedSizeList(field, size) => {
            let new_inner_dtype = coerce_arrow_to_daft_compatible_type(field.data_type())?;
            Some(arrow::datatypes::DataType::FixedSizeList(
                Arc::new(
                    arrow::datatypes::Field::new(
                        field.name().clone(),
                        new_inner_dtype,
                        field.is_nullable(),
                    )
                    .with_metadata(field.metadata().clone()),
                ),
                *size,
            ))
        }
        arrow::datatypes::DataType::Struct(fields) => {
            let new_fields = arrow::datatypes::Fields::from(
                fields
                    .iter()
                    .map(
                        |field| match coerce_arrow_to_daft_compatible_type(field.data_type()) {
                            Some(new_inner_dtype) => arrow::datatypes::Field::new(
                                field.name().clone(),
                                new_inner_dtype,
                                field.is_nullable(),
                            )
                            .with_metadata(field.metadata().clone()),
                            None => field.as_ref().clone(),
                        },
                    )
                    .collect::<Vec<arrow::datatypes::Field>>(),
            );
            if &new_fields == fields {
                None
            } else {
                Some(arrow::datatypes::DataType::Struct(
                    arrow::datatypes::Fields::from(new_fields),
                ))
            }
        }
        _ => None,
    }
}

pub fn cast_array_for_daft_if_needed(
    arrow_array: Box<dyn daft_arrow::array::Array>,
) -> Box<dyn daft_arrow::array::Array> {
    match coerce_to_daft_compatible_type(arrow_array.data_type()) {
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

pub fn cast_arrow_array_for_daft_if_needed(
    arrow_array: Arc<dyn arrow_array::Array>,
) -> Arc<dyn arrow_array::Array> {
    let options = arrow::compute::CastOptions {
        safe: false,
        ..Default::default()
    };

    match coerce_arrow_to_daft_compatible_type(arrow_array.data_type()) {
        Some(coerced_dtype) => {
            arrow::compute::cast_with_options(arrow_array.as_ref(), &coerced_dtype, &options)
                .unwrap()
        }
        None => arrow_array,
    }
}

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
