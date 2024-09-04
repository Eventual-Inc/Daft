use daft_core::datatypes::{DataType, Field};
use common_error::DaftError;
use daft_dsl::make_unary_udf_function;

make_unary_udf_function! {
    name: "is_nan",
    to_field: (inputs, schema) {
        match inputs {
            [data] => match data.to_field(schema) {
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
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    },
    evaluate: (inputs) {
        match inputs {
            [data] => data.is_nan(),
            _ => Err(DaftError::ValueError(format!(
                inputs.len()
            ))),
        }
    }
}
