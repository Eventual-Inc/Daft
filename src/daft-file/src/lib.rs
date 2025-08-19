use common_error::DaftError;
use daft_core::{datatypes::logical::FileArray, series::IntoSeries};
use daft_dsl::functions::prelude::*;
use daft_io::IOConfig;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
pub mod python;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct File;
#[derive(FunctionArgs)]
struct FileArgs<T> {
    input: T,
    runner_name: String,
    io_config: IOConfig,
}

#[typetag::serde]
impl ScalarUDF for File {
    fn name(&self) -> &'static str {
        "file"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let FileArgs { input, .. } = args.try_into()?;
        Ok(match input.data_type() {
            DataType::Binary => {
                FileArray::new_from_data_array(input.name(), input.binary()?).into_series()
            }
            DataType::Utf8 => {
                FileArray::new_from_reference_array(input.name(), input.utf8()?).into_series()
            }
            _ => {
                return Err(DaftError::ValueError(format!(
                "Unsupported data type for 'file' function: {}. Expected either String | Binary",
                input.data_type()
            )))
            }
        })
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let FileArgs {
            input,
            runner_name,
            io_config,
        } = args.try_into()?;
        let input = input.to_field(schema)?;
        if input.dtype == DataType::Utf8 && runner_name.as_str() == "ray" {
            return Err(DaftError::ValueError(
                "Cannot reference local files within this context".to_string(),
            ));
        }

        Ok(Field::new(input.name, DataType::File))
    }
}
