use std::io::BufReader;

use common_error::{DaftError, DaftResult};
use daft_core::{prelude::*, with_match_file_types};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg},
};
use daft_file::DaftFile;
use image::{ColorType, ImageDecoder as _, ImageReader};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageFileMetadata;

fn image_format_to_string(format: image::ImageFormat) -> &'static str {
    match format {
        image::ImageFormat::Png => "png",
        image::ImageFormat::Jpeg => "jpeg",
        image::ImageFormat::Gif => "gif",
        image::ImageFormat::WebP => "webp",
        image::ImageFormat::Tiff => "tiff",
        image::ImageFormat::Bmp => "bmp",
        image::ImageFormat::Ico => "ico",
        image::ImageFormat::Hdr => "hdr",
        _ => "unknown",
    }
}

fn color_type_to_mode(ct: ColorType) -> &'static str {
    match ct {
        ColorType::L8 => "L",
        ColorType::La8 => "LA",
        ColorType::Rgb8 => "RGB",
        ColorType::Rgba8 => "RGBA",
        ColorType::L16 => "L16",
        ColorType::La16 => "LA16",
        ColorType::Rgb16 => "RGB16",
        ColorType::Rgba16 => "RGBA16",
        ColorType::Rgb32F => "RGB32F",
        ColorType::Rgba32F => "RGBA32F",
        _ => "unknown",
    }
}

fn struct_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("width", DataType::UInt32),
        Field::new("height", DataType::UInt32),
        Field::new("format", DataType::Utf8),
        Field::new("mode", DataType::Utf8),
    ])
}

#[typetag::serde]
impl ScalarUDF for ImageFileMetadata {
    fn name(&self) -> &'static str {
        "image_file_metadata"
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = args.try_into()?;

        with_match_file_types!(input.data_type(), |$P| {
            let s = input.file::<$P>()?;
            let len = s.len();
            let mut widths: Vec<Option<u32>> = Vec::with_capacity(len);
            let mut heights: Vec<Option<u32>> = Vec::with_capacity(len);
            let mut formats: Vec<Option<String>> = Vec::with_capacity(len);
            let mut modes: Vec<Option<String>> = Vec::with_capacity(len);

            for i in 0..len {
                match s.get(i) {
                    None => {
                        widths.push(None);
                        heights.push(None);
                        formats.push(None);
                        modes.push(None);
                    }
                    Some(file_ref) => {
                        let file = DaftFile::load_blocking(file_ref, false)?;
                        let reader = ImageReader::new(BufReader::new(file))
                            .with_guessed_format()
                            .map_err(|e| {
                                DaftError::ComputeError(format!(
                                    "Failed to guess image format: {e}"
                                ))
                            })?;

                        let format_str = reader.format().map(image_format_to_string);

                        let decoder = reader.into_decoder().map_err(|e| {
                            DaftError::ComputeError(format!(
                                "Failed to read image header: {e}"
                            ))
                        })?;

                        let (w, h) = decoder.dimensions();
                        let mode_str = color_type_to_mode(decoder.color_type());

                        widths.push(Some(w));
                        heights.push(Some(h));
                        formats.push(format_str.map(|s| s.to_string()));
                        modes.push(Some(mode_str.to_string()));
                    }
                }
            }

            let name = input.name();
            let width_array = UInt32Array::from_iter(
                Field::new("width", DataType::UInt32),
                widths.into_iter(),
            );
            let height_array = UInt32Array::from_iter(
                Field::new("height", DataType::UInt32),
                heights.into_iter(),
            );
            let format_array = Utf8Array::from_iter("format", formats.iter().map(|o| o.as_deref()));
            let mode_array = Utf8Array::from_iter("mode", modes.iter().map(|o| o.as_deref()));

            let struct_array = StructArray::new(
                Field::new(name, struct_dtype()),
                vec![
                    width_array.into_series(),
                    height_array.into_series(),
                    format_array.into_series(),
                    mode_array.into_series(),
                ],
                None,
            );
            Ok(struct_array.into_series())
        })
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let UnaryArg { input } = args.try_into()?;
        let field = input.to_field(schema)?;

        if !matches!(field.dtype, DataType::File(_)) {
            return Err(DaftError::TypeError(format!(
                "image_file_metadata requires a File input, got {field}"
            )));
        }

        Ok(Field::new(field.name, struct_dtype()))
    }

    fn docstring(&self) -> &'static str {
        "Extracts image metadata (width, height, format, mode) from a File column containing image data."
    }
}
