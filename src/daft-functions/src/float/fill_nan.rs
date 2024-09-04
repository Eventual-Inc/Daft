use common_error::DaftError;
use daft_core::{datatypes::Field, utils::supertype::try_get_supertype};
use daft_dsl::make_binary_udf_function;

make_binary_udf_function! {
    name: "fill_nan",
    to_field: (inputs, schema) {
        match inputs {
            [data, fill_value] => match (data.to_field(schema), fill_value.to_field(schema)) {
                (Ok(data_field), Ok(fill_value_field)) => {
                    match (&data_field.dtype.is_floating(), &fill_value_field.dtype.is_floating(), try_get_supertype(&data_field.dtype, &fill_value_field.dtype)) {
                        (true, true, Ok(dtype)) => Ok(Field::new(data_field.name, dtype)),
                        _ => Err(DaftError::TypeError(format!(
                            "Expects input to fill_nan to be float, but received {data_field} and {fill_value_field}",
                        ))),
                    }
                }
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    },
    evaluate: (inputs) {
        match inputs {
            [data, fill_value] => data.fill_nan(fill_value),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
