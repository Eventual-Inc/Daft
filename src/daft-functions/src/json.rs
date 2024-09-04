use common_error::DaftError;
use daft_core::{datatypes::Field, DataType};
use daft_dsl::make_parameterized_udf_function;

make_parameterized_udf_function! {
    name: "json_query",
    params: (query: String),
    to_field: (data, schema) {
        match data.to_field(schema) {
            Ok(data_field) => match &data_field.dtype {
                DataType::Utf8 => {
                    Ok(Field::new(data_field.name, DataType::Utf8))
                }
                _ => Err(DaftError::TypeError(format!(
                    "Expected input to be a string type, received: {data_field}",
                ))),
            },
            Err(e) => Err(e),
        }
    },
    evaluate: (self, data) {
        data.json_query(self.query.as_str())
    }
}
