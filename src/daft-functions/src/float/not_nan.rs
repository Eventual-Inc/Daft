use common_error::DaftError;
use daft_core::{datatypes::Field, DataType};
use daft_dsl::make_udf_function;

make_udf_function! {
    name: "not_nan",
    to_field: (data, schema) {
        match data.to_field(schema) {
            Ok(data_field) => match &data_field.dtype {
                // DataType::Float16 |
                DataType::Float32 | DataType::Float64 => {
                    Ok(Field::new(data_field.name, DataType::Boolean))
                }
                _ => Err(DaftError::TypeError(format!(
                    "Expects input to is_nan to be float, but received {data_field}",
                ))),
            },
            Err(e) => Err(e),
        }
    },
    evaluate: (data) {
        data.not_nan()
    }
}
