use common_error::DaftError;
use daft_core::{datatypes::FileArray, prelude::UInt64Array, series::IntoSeries};
use daft_dsl::functions::{UnaryArg, prelude::*, scalar::AsyncScalarUDF};
use daft_io::IOConfig;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

use crate::file::DaftFile;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Size;

#[typetag::serde]
#[async_trait::async_trait]
impl AsyncScalarUDF for Size {
    fn name(&self) -> &'static str {
        "file_size"
    }

    async fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        use futures::stream::{self, StreamExt};
        let UnaryArg { input } = args.try_into()?;
        let s = input.file()?;
        let len = s.len();

        let mut results: Vec<(usize, Option<u64>)> = stream::iter((0..len).map(|i| async move {
            match s.get(i) {
                Some(f) => {
                    let f = DaftFile::new(f).await?;
                    let size = f.size()?;
                    DaftResult::Ok((i, Some(size as u64)))
                }
                None => DaftResult::Ok((i, None)),
            }
        }))
        .buffer_unordered(256) // todo(universalmin303): somehow set this as a variable on the file array instead.
        .try_collect::<Vec<_>>()
        .await?;

        results.sort_by_key(|k| k.0);

        Ok(UInt64Array::from_iter(
            Field::new(s.name(), DataType::UInt64),
            results.into_iter().map(|(_, v)| v),
        )
        .into_series())
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let UnaryArg { input } = args.try_into()?;
        let name = input.to_field(schema)?.name;

        Ok(Field::new(name, DataType::UInt64))
    }
}
