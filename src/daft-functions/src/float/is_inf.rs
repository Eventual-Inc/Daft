use common_error::DaftError;
use daft_core::prelude::*;

make_udf_function! {
    name: "is_inf",
    to_field: (data, schema) {
        match data.to_field(schema) {
            Ok(data_field) => match &data_field.dtype {
                // DataType::Float16 |
                DataType::Float32 | DataType::Float64 => {
                    Ok(Field::new(data_field.name, DataType::Boolean))
                }
                _ => Err(DaftError::TypeError(format!(
                    "Expects input to is_inf to be float, but received {data_field}",
                ))),
            },
            Err(e) => Err(e),
        }
    },
    evaluate: (data) {
        data.is_inf()
    }
}
