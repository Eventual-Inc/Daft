use std::io::Read;

use common_error::{DaftError, DaftResult, ensure};
use common_image::CowImage;
use daft_core::{
    array::ops::image::image_array_from_img_buffers, prelude::*, with_match_file_types,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use daft_file::DaftFile;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DecodeImageFile;

#[derive(FunctionArgs)]
struct DecodeImageFileArgs<T> {
    input: T,
    #[arg(optional)]
    mode: Option<ImageMode>,
    #[arg(optional)]
    on_error: Option<T>,
}

#[typetag::serde]
impl ScalarUDF for DecodeImageFile {
    fn name(&self) -> &'static str {
        "decode_image_file"
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let DecodeImageFileArgs {
            input,
            mode,
            on_error,
        } = args.try_into()?;

        let raise_on_error = on_error
            .map(|s| {
                ensure!(s.len() == 1, "on_error must be a scalar value");
                if s.data_type().is_null() {
                    return Ok("raise".to_string());
                }
                Ok(s.utf8()?.get(0).unwrap().to_string())
            })
            .transpose()?
            .unwrap_or_else(|| "raise".to_string());

        let raise_on_error = match raise_on_error.to_lowercase().as_str() {
            "raise" => true,
            "null" => false,
            _ => {
                return Err(DaftError::ValueError(format!(
                    "Invalid on_error value: {raise_on_error}"
                )));
            }
        };

        with_match_file_types!(input.data_type(), |$P| {
            let s = input.file::<$P>()?;
            let len = s.len();
            let mut img_bufs: Vec<Option<CowImage>> = Vec::with_capacity(len);

            for i in 0..len {
                let img_buf = match s.get(i) {
                    None => None,
                    Some(file_ref) => {
                        let result = (|| {
                            let mut file = DaftFile::load_blocking(file_ref, true)?;
                            let mut buf = Vec::new();
                            file.read_to_end(&mut buf).map_err(|e| {
                                DaftError::ComputeError(format!("Failed to read file: {e}"))
                            })?;
                            CowImage::decode(&buf)
                        })();

                        match result {
                            Ok(img) => Some(img),
                            Err(err) => {
                                if raise_on_error {
                                    return Err(err);
                                }
                                log::warn!(
                                    "Error during image file decode at index {i}: {err} (falling back to Null)"
                                );
                                None
                            }
                        }
                    }
                };

                let img_buf = match (img_buf, mode) {
                    (Some(buf), Some(m)) => Some(buf.into_mode(m)),
                    (buf, _) => buf,
                };

                img_bufs.push(img_buf);
            }

            let result = image_array_from_img_buffers(input.name(), img_bufs.into_iter(), mode)?;
            Ok(result.into_series())
        })
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let DecodeImageFileArgs {
            input,
            mode,
            on_error,
        } = args.try_into()?;

        let field = input.to_field(schema)?;

        if !matches!(field.dtype, DataType::File(_)) {
            return Err(DaftError::TypeError(format!(
                "decode_image_file requires a File input, got {field}"
            )));
        }

        if let Some(on_error) = on_error {
            let f = on_error.to_field(schema)?;
            ensure!(f.dtype == DataType::Utf8, "on_error must be a string");
        }

        Ok(Field::new(field.name, DataType::Image(mode)))
    }

    fn docstring(&self) -> &'static str {
        "Decodes image files from a File column into an Image column. Supports optional mode conversion and error handling."
    }
}
