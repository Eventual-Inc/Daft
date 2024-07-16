use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;

use arrow2::compute::cast;

// TODO(Clark): Refactor to GILOnceCell in order to avoid deadlock between the below mutex and the Python GIL.
lazy_static! {
    static ref REGISTRY: Mutex<HashMap<std::string::String, arrow2::datatypes::DataType>> =
        Mutex::new(HashMap::new());
}

fn coerce_to_daft_compatible_type(
    dtype: &arrow2::datatypes::DataType,
) -> Option<arrow2::datatypes::DataType> {
    match dtype {
        arrow2::datatypes::DataType::Utf8 => Some(arrow2::datatypes::DataType::LargeUtf8),
        arrow2::datatypes::DataType::Binary => Some(arrow2::datatypes::DataType::LargeBinary),
        arrow2::datatypes::DataType::List(field) => {
            let new_field = match coerce_to_daft_compatible_type(field.data_type()) {
                Some(new_inner_dtype) => Box::new(
                    arrow2::datatypes::Field::new(
                        field.name.clone(),
                        new_inner_dtype,
                        field.is_nullable,
                    )
                    .with_metadata(field.metadata.clone()),
                ),
                None => field.clone(),
            };
            Some(arrow2::datatypes::DataType::LargeList(new_field))
        }
        arrow2::datatypes::DataType::LargeList(field) => {
            let new_inner_dtype = coerce_to_daft_compatible_type(field.data_type())?;
            Some(arrow2::datatypes::DataType::LargeList(Box::new(
                arrow2::datatypes::Field::new(
                    field.name.clone(),
                    new_inner_dtype,
                    field.is_nullable,
                )
                .with_metadata(field.metadata.clone()),
            )))
        }
        arrow2::datatypes::DataType::Map(field, sorted) => {
            let new_field = match coerce_to_daft_compatible_type(field.data_type()) {
                Some(new_inner_dtype) => Box::new(
                    arrow2::datatypes::Field::new(
                        field.name.clone(),
                        new_inner_dtype,
                        field.is_nullable,
                    )
                    .with_metadata(field.metadata.clone()),
                ),
                None => field.clone(),
            };
            Some(arrow2::datatypes::DataType::Map(new_field, *sorted))
        }
        arrow2::datatypes::DataType::FixedSizeList(field, size) => {
            let new_inner_dtype = coerce_to_daft_compatible_type(field.data_type())?;
            Some(arrow2::datatypes::DataType::FixedSizeList(
                Box::new(
                    arrow2::datatypes::Field::new(
                        field.name.clone(),
                        new_inner_dtype,
                        field.is_nullable,
                    )
                    .with_metadata(field.metadata.clone()),
                ),
                *size,
            ))
        }
        arrow2::datatypes::DataType::Struct(fields) => {
            let new_fields = fields
                .iter()
                .map(
                    |field| match coerce_to_daft_compatible_type(field.data_type()) {
                        Some(new_inner_dtype) => arrow2::datatypes::Field::new(
                            field.name.clone(),
                            new_inner_dtype,
                            field.is_nullable,
                        )
                        .with_metadata(field.metadata.clone()),
                        None => field.clone(),
                    },
                )
                .collect::<Vec<arrow2::datatypes::Field>>();
            if &new_fields == fields {
                None
            } else {
                Some(arrow2::datatypes::DataType::Struct(new_fields))
            }
        }
        arrow2::datatypes::DataType::Extension(name, inner, metadata) => {
            let new_inner_dtype = coerce_to_daft_compatible_type(inner.as_ref())?;
            REGISTRY.lock().unwrap().insert(name.clone(), dtype.clone());
            Some(arrow2::datatypes::DataType::Extension(
                name.clone(),
                Box::new(new_inner_dtype),
                metadata.clone(),
            ))
        }
        _ => None,
    }
}

pub fn cast_array_for_daft_if_needed(
    arrow_array: Box<dyn arrow2::array::Array>,
) -> Box<dyn arrow2::array::Array> {
    match coerce_to_daft_compatible_type(arrow_array.data_type()) {
        Some(coerced_dtype) => match coerced_dtype {
            // TODO: Consolidate Map to use the same cast::cast method as other datatypes.
            // Currently, Map is not supported in Arrow2::compute::cast, so this workaround is necessary.
            // A known limitation of this workaround is that it does not handle nested maps.
            arrow2::datatypes::DataType::Map(to_field, sorted) => {
                let map_array = arrow_array
                    .as_any()
                    .downcast_ref::<arrow2::array::MapArray>()
                    .unwrap();
                let casted = cast_array_for_daft_if_needed(map_array.field().clone());
                Box::new(arrow2::array::MapArray::new(
                    arrow2::datatypes::DataType::Map(to_field.clone(), sorted),
                    map_array.offsets().clone(),
                    casted,
                    arrow_array.validity().cloned(),
                ))
            }
            _ => cast::cast(
                arrow_array.as_ref(),
                &coerced_dtype,
                cast::CastOptions {
                    wrapped: true,
                    partial: false,
                },
            )
            .unwrap(),
        },
        None => arrow_array,
    }
}

fn coerce_from_daft_compatible_type(
    dtype: &arrow2::datatypes::DataType,
) -> Option<arrow2::datatypes::DataType> {
    match dtype {
        arrow2::datatypes::DataType::Extension(name, _, _)
            if REGISTRY.lock().unwrap().contains_key(name) =>
        {
            let entry = REGISTRY.lock().unwrap();
            Some(entry.get(name).unwrap().clone())
        }
        _ => None,
    }
}

pub fn cast_array_from_daft_if_needed(
    arrow_array: Box<dyn arrow2::array::Array>,
) -> Box<dyn arrow2::array::Array> {
    match coerce_from_daft_compatible_type(arrow_array.data_type()) {
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

#[inline]
pub fn arrow_bitmap_and_helper(
    l_bitmap: Option<&arrow2::bitmap::Bitmap>,
    r_bitmap: Option<&arrow2::bitmap::Bitmap>,
) -> Option<arrow2::bitmap::Bitmap> {
    match (l_bitmap, r_bitmap) {
        (None, None) => None,
        (Some(l), None) => Some(l.clone()),
        (None, Some(r)) => Some(r.clone()),
        (Some(l), Some(r)) => Some(arrow2::bitmap::and(l, r)),
    }
}
