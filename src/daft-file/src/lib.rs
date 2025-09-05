use common_error::DaftError;
use daft_core::{datatypes::FileArray, series::IntoSeries};
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
    #[arg(optional)]
    io_config: Option<IOConfig>,
}

#[typetag::serde]
impl ScalarUDF for File {
    fn name(&self) -> &'static str {
        "file"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let FileArgs {
            input, io_config, ..
        } = args.try_into()?;
        Ok(match input.data_type() {
            DataType::Binary => {
                FileArray::new_from_data_array(input.name(), input.binary()?).into_series()
            }
            DataType::Utf8 => {
                FileArray::new_from_reference_array(input.name(), input.utf8()?, io_config)
                    .into_series()
            }
            _ => {
                return Err(DaftError::ValueError(format!(
                    "Unsupported data type for 'file' function: {}. Expected either String | Binary",
                    input.data_type()
                )));
            }
        })
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let FileArgs { input, .. } = args.try_into()?;

        let input = input.to_field(schema)?;

        Ok(Field::new(input.name, DataType::File))
    }
}
