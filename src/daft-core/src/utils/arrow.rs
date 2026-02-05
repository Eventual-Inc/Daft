use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex},
};

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::{Field, Schema},
};
use daft_arrow::{
    arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY},
    compute::cast,
};
use daft_schema::dtype::DAFT_SUPER_EXTENSION_NAME;

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

pub fn cast_recordbatch_for_daft_if_needed(batch: RecordBatch) -> RecordBatch {
    let mut output_columns = Vec::with_capacity(batch.num_columns());
    let mut output_schema = Vec::with_capacity(batch.num_columns());
    let schema = batch.schema().clone();

    for (i, column) in batch.columns().iter().enumerate() {
        let orig_field = schema.field(i);
        let field = daft_schema::field::Field::from_arrow(orig_field, true)
            .expect("Failed to create field");
        dbg!(&field);

        let field = field.to_arrow().unwrap();

        if orig_field == &field {
            output_columns.push(column.clone());
            output_schema.push(field.clone());
        } else {
            output_columns.push(arrow::compute::cast(column.as_ref(), &field.data_type()).unwrap());
            output_schema.push(field);
        }
    }
    let output_schema = Schema::new(output_schema);
    unsafe { RecordBatch::new_unchecked(Arc::new(output_schema), output_columns, batch.num_rows()) }

    // let arr = arrow_batch.column(0);
    // let dtype = arr.data_type();

    // match coerce_to_daft_compatible_type_v2(&dtype) {
    //     Some(coerced_dtype) => arrow::compute::cast(arr.as_ref(), &coerced_dtype).unwrap(),
    //     None => arr.clone(),
    // }
}

fn coerce_from_daft_compatible_type(
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

pub fn cast_array_from_daft_if_needed(
    arrow_array: Box<dyn daft_arrow::array::Array>,
) -> Box<dyn daft_arrow::array::Array> {
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
