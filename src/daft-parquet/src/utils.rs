#![allow(deprecated, reason = "arrow2 migration")]

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
            Some(daft_arrow::datatypes::DataType::Extension(
                name.clone(),
                Box::new(new_inner_dtype),
                metadata.clone(),
            ))
        }
        _ => None,
    }
}
