use common_error::DaftError;
use daft_core::{
    datatypes::FileArray,
    file::{FileReference, MediaTypeAudio, MediaTypeUnknown, MediaTypeVideo},
    series::IntoSeries,
    with_match_file_types,
};
use daft_dsl::functions::{UnaryArg, prelude::*};
use daft_io::IOConfig;
use daft_schema::media_type::MediaType;
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
            DataType::File(MediaType::Unknown) => input,
            DataType::File(MediaType::Video) => input.cast(&DataType::File(MediaType::Unknown))?,
            DataType::Utf8 => FileArray::<MediaTypeUnknown>::new_from_reference_array(
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

        Ok(Field::new(input.name, DataType::File(MediaType::Unknown)))
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

        // TODO(universalmind303): can we use an async stream here instead so we're not blocking on each iteration
        fn verify_file(file_ref: FileReference) -> DaftResult<FileReference> {
            let mut daft_file = DaftFile::load_blocking(file_ref.clone(), false)?;

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
            DataType::File(MediaType::Video) => input,
            DataType::File(MediaType::Unknown) => {
                let casted = input.cast(&DataType::File(MediaType::Video))?;
                let files = casted.file::<MediaTypeVideo>()?.clone();

                if verify {
                    for file in files.into_iter().flatten() {
                        verify_file(file)?;
                    }
                }
                files.into_series()
            }
            DataType::Utf8 => {
                let utf8 = input.utf8()?;
                // TODO(universalmind303): refactor this to be async once we have async scalar fns.
                let data = utf8.into_iter().map(|data| {
                    data.map(|data| {
                        let file_ref = FileReference::new(
                            MediaType::Video,
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
                FileArray::<MediaTypeVideo>::new_from_file_references(input.name(), data)?
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

        Ok(Field::new(input.name, DataType::File(MediaType::Video)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AudioFile;
#[derive(FunctionArgs)]
struct AudioFileArgs<T> {
    input: T,
    verify: bool,
    #[arg(optional)]
    io_config: Option<IOConfig>,
}

#[typetag::serde]
impl ScalarUDF for AudioFile {
    fn name(&self) -> &'static str {
        "audio_file"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let AudioFileArgs {
            input,
            verify,
            io_config,
        } = args.try_into()?;

        // TODO(universalmind303): can we use an async stream here instead so we're not blocking on each iteration
        fn verify_file(file_ref: FileReference) -> DaftResult<FileReference> {
            let mut daft_file = DaftFile::load_blocking(file_ref.clone(), false)?;

            let mime_type = daft_file.guess_mime_type();
            match mime_type {
                Some(mime_type) if mime_type.starts_with("audio") => Ok(file_ref),
                Some(mime_type) => Err(DaftError::ValueError(format!(
                    "Invalid audio file: {:?}",
                    mime_type
                ))),
                None => Err(DaftError::ValueError("Invalid audio file".to_string())),
            }
        }
        Ok(match input.data_type() {
            DataType::File(MediaType::Audio) => input,
            DataType::File(MediaType::Unknown) => {
                let casted = input.cast(&DataType::File(MediaType::Audio))?;
                let files = casted.file::<MediaTypeAudio>()?.clone();

                if verify {
                    for file in files.into_iter().flatten() {
                        verify_file(file)?;
                    }
                }
                files.into_series()
            }
            DataType::Utf8 => {
                let utf8 = input.utf8()?;
                // TODO(universalmind303): refactor this to be async once we have async scalar fns.
                let data = utf8.into_iter().map(|data| {
                    data.map(|data| {
                        let file_ref = FileReference::new(
                            MediaType::Audio,
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
                FileArray::<MediaTypeAudio>::new_from_file_references(input.name(), data)?
                    .into_series()
            }
            _ => {
                return Err(DaftError::ValueError(format!(
                    "Unsupported data type for 'file' function: {}. Expected String",
                    input.data_type()
                )));
            }
        })
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let AudioFileArgs { input, .. } = args.try_into()?;

        let input = input.to_field(schema)?;

        Ok(Field::new(input.name, DataType::File(MediaType::Audio)))
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

        with_match_file_types!(input.data_type(), |$P| {
            let s = input.file::<$P>()?;
            let len = s.len();
            let mut out = Vec::with_capacity(len);
            // TODO(cory): can likely optimize this a lot more than a naive for loop.
            for i in 0..len {
                let opt: Option<u64> = s
                    .get(i)
                    .map(|f| {
                        let f = DaftFile::load_blocking(f, false)?;
                        let size = f.size()?;
                        DaftResult::Ok(size as _)
                    })
                    .transpose()?;
                out.push(opt);
            }
            Ok(
                daft_core::prelude::UInt64Array::from_iter(Field::new(s.name(), DataType::UInt64), out.into_iter())
                    .into_series(),
            )
        })
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let UnaryArg { input } = args.try_into()?;
        let name = input.to_field(schema)?.name;

        Ok(Field::new(name, DataType::UInt64))
    }
}
