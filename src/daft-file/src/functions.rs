use common_error::DaftError;
use daft_core::{
    array::blob_array::BlobArray,
    datatypes::FileArray,
    file::{FileReference, MediaTypeUnknown, MediaTypeVideo},
    series::IntoSeries,
    with_match_file_types,
};
use daft_dsl::functions::{BuiltinScalarFnVariant, FUNCTION_REGISTRY, UnaryArg, prelude::*};
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
            DataType::File(MediaType::Unknown) | DataType::Blob(MediaType::Unknown) => input,
            DataType::File(MediaType::Video) => input.cast(&DataType::File(MediaType::Unknown))?,
            DataType::Blob(MediaType::Video) => input.cast(&DataType::Blob(MediaType::Unknown))?,

            DataType::Binary => {
                BlobArray::<MediaTypeUnknown>::from_values(input.name(), input.binary()?)
                    .into_series()
            }
            DataType::Utf8 => {
                FileArray::<MediaTypeUnknown>::from_values(input.name(), input.utf8()?, io_config)
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
        match input.dtype {
            DataType::Utf8 => Ok(Field::new(input.name, DataType::File(MediaType::Unknown))),
            DataType::Binary => Ok(Field::new(input.name, DataType::Blob(MediaType::Unknown))),
            other => Err(DaftError::TypeError(format!(
                "Unsupported data type for 'file' function: {}. Expected either String | Binary",
                other
            ))),
        }
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
            DataType::File(MediaType::Video) | DataType::Blob(MediaType::Video) => input,
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
            DataType::Blob(MediaType::Unknown) => {
                let casted = input.cast(&DataType::Blob(MediaType::Video))?;
                let files = casted.blob::<MediaTypeVideo>()?.clone();

                if verify {
                    for file in files.into_iter().flatten() {
                        verify_file(file)?;
                    }
                }
                files.into_series()
            }
            DataType::Binary => {
                let bin = input.binary()?;
                let blob_arr = BlobArray::<MediaTypeVideo>::from_values(bin.name(), bin);
                if verify {
                    for file in blob_arr.into_iter().flatten() {
                        verify_file(file)?;
                    }
                }
                blob_arr.into_series()
            }
            DataType::Utf8 => {
                let utf8 = input.utf8()?;
                let file_arr =
                    FileArray::<MediaTypeVideo>::from_values(utf8.name(), utf8, io_config);
                if verify {
                    for file in file_arr.into_iter().flatten() {
                        verify_file(file)?;
                    }
                }
                file_arr.into_series()
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
        let mt = MediaType::Video;
        let return_dtype = match &input.dtype {
            DataType::Utf8 => DataType::File(mt),
            DataType::Binary => DataType::Blob(mt),
            _ => return Err(DaftError::type_error("expected utf8 or binary")),
        };

        Ok(Field::new(input.name, return_dtype))
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
        let UnaryArg { input } = args.clone().try_into()?;
        match input.data_type() {
            DataType::File(..) => {
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
            // use the built in binary length function to get the size
            DataType::Blob(_) => {
                let length_fn = FUNCTION_REGISTRY.read().unwrap().get("length").unwrap();
                let BuiltinScalarFnVariant::Sync(f) =
                    length_fn.get_function(FunctionArgs::empty(), &Schema::empty())?
                else {
                    unreachable!("length function is always sync")
                };
                let new_args = args
                    .iter()
                    .map(|arg| arg.map(|arg| arg.cast(&DataType::Binary).unwrap()));
                let new_args = FunctionArgs::new_unchecked(new_args.collect());
                f.call(new_args)
            }
            other => Err(DaftError::ComputeError(format!(
                "Unsupported data type for file size: {}",
                other
            ))),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let UnaryArg { input } = args.try_into()?;
        let field = input.to_field(schema)?;

        ensure!(
           matches!(field.dtype, DataType::File(_) | DataType::Blob(_)),
           TypeError: "Unsupported data type for file size: {}",
           field.dtype
        );

        Ok(Field::new(field.name, DataType::UInt64))
    }
}
