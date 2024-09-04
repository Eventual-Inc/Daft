use daft_core::{
    datatypes::{DataType, Field},
    CountMode, IntoSeries,
};

use common_error::DaftError;
use daft_dsl::make_parameterized_udf_function;

make_parameterized_udf_function! {
    name: "list_count",
    params: (mode: CountMode),
    to_field: (self, input, schema) {
        let input_field = input.to_field(schema)?;

        match input_field.dtype {
            DataType::List(_) | DataType::FixedSizeList(_, _) => {
                Ok(Field::new(input.name(), DataType::UInt64))
            },
            _ => Err(DaftError::TypeError(format!(
                "Expected input to be a list type, received: {}",
                input_field.dtype
            ))),
        }
    },
    evaluate: (self, data) {
        data.list_count(self.mode).map(|count| count.into_series())
    }
}
