use common_error::DaftError;
use daft_core::{
    datatypes::FileArray,
    file::{FileFormatUnknown, FileFormatVideo, FileReference},
    prelude::UInt64Array,
    series::IntoSeries,
};
use daft_dsl::functions::{UnaryArg, prelude::*};
use daft_io::IOConfig;
use daft_schema::file_format::FileFormat;
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
                FileArray::<FileFormatUnknown>::new_from_data_array(input.name(), input.binary()?)
                    .into_series()
            }
            DataType::Utf8 => FileArray::<FileFormatUnknown>::new_from_reference_array(
                input.name(),
                input.utf8()?,
                io_config,
            )
            .into_series(),
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

        Ok(Field::new(input.name, DataType::File(FileFormat::Unknown)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct VideoFile;
#[derive(FunctionArgs)]
struct VideoFileArgs<T> {
    input: T,
    verify: bool,
    #[arg(optional)]
    io_config: Option<IOConfig>,
}

#[typetag::serde]
impl ScalarUDF for VideoFile {
    fn name(&self) -> &'static str {
        "video_file"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let VideoFileArgs {
            input,
            verify,
            io_config,
        } = args.try_into()?;

        fn verify_file(file_ref: FileReference) -> DaftResult<FileReference> {
            let mut daft_file: DaftFile = file_ref.clone().try_into()?;

            let mime_type = daft_file.guess_mime_type();
            match mime_type {
                Some(mime_type) if mime_type.starts_with("video") => Ok(file_ref),
                Some(mime_type) => Err(DaftError::ValueError(format!(
                    "Invalid video file: {:?}",
                    mime_type
                ))),
                None => Err(DaftError::ValueError("Invalid Video file".to_string())),
            }
        }
        Ok(match input.data_type() {
            DataType::Binary => {
                let bin = input.binary()?;

                let data = bin.into_iter().map(|data| {
                    data.map(|data| {
                        let file_ref =
                            FileReference::new_from_data(FileFormat::Video, data.to_vec());
                        if verify {
                            verify_file(file_ref)
                        } else {
                            Ok(file_ref)
                        }
                    })
                    .transpose()
                });
                FileArray::<FileFormatVideo>::new_from_file_references(input.name(), data)?
                    .into_series()
            }
            DataType::Utf8 => {
                let utf8 = input.utf8()?;
                let data = utf8.into_iter().map(|data| {
                    data.map(|data| {
                        let file_ref = FileReference::new_from_reference(
                            FileFormat::Video,
                            data.to_string(),
                            io_config.clone(),
                        );
                        if verify {
                            verify_file(file_ref)
                        } else {
                            Ok(file_ref)
                        }
                    })
                    .transpose()
                });
                FileArray::<FileFormatVideo>::new_from_file_references(input.name(), data)?
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
        let VideoFileArgs { input, .. } = args.try_into()?;

        let input = input.to_field(schema)?;

        Ok(Field::new(input.name, DataType::File(FileFormat::Video)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Size;

#[typetag::serde]
impl ScalarUDF for Size {
    fn name(&self) -> &'static str {
        "file_size"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input } = args.try_into()?;
        let s = input.file()?;
        let len = s.len();
        let mut out = Vec::with_capacity(len);
        // todo(cory): can likely optimize this a lot more than a naive for loop.
        for i in 0..len {
            let opt: Option<u64> = s
                .get(i)
                .map(|f| {
                    let f = DaftFile::try_from(f)?;
                    let size = f.size()?;
                    DaftResult::Ok(size as _)
                })
                .transpose()?;
            out.push(opt);
        }

        Ok(
            UInt64Array::from_iter(Field::new(s.name(), DataType::UInt64), out.into_iter())
                .into_series(),
        )
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let UnaryArg { input } = args.try_into()?;
        let name = input.to_field(schema)?.name;

        Ok(Field::new(name, DataType::UInt64))
    }
}
