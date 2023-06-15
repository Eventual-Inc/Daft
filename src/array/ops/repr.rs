use base64::Engine;

use crate::{
    array::DataArray,
    datatypes::{
        logical::{DateArray, EmbeddingArray, FixedShapeImageArray, ImageArray, TimestampArray},
        BinaryArray, BooleanArray, DaftNumericType, ExtensionArray, FixedSizeListArray,
        ImageFormat, ListArray, NullArray, StructArray, Utf8Array,
    },
    error::DaftResult,
};

// Default implementation of str_value: format the value with the given format string.
macro_rules! impl_array_str_value {
    ($ArrayT:ty, $fmt:expr) => {
        impl $ArrayT {
            pub fn str_value(&self, idx: usize) -> DaftResult<String> {
                let val = self.get(idx);
                match val {
                    None => Ok("None".to_string()),
                    Some(v) => Ok(format!($fmt, v)),
                }
            }
        }
    };
}

impl_array_str_value!(BooleanArray, "{}");
impl_array_str_value!(ListArray, "{:?}");
impl_array_str_value!(FixedSizeListArray, "{:?}");
impl_array_str_value!(StructArray, "{:?}");
impl_array_str_value!(ExtensionArray, "{:?}");
impl_array_str_value!(EmbeddingArray, "{:?}");
impl_array_str_value!(ImageArray, "{:?}");
impl_array_str_value!(FixedShapeImageArray, "{:?}");

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v}")),
        }
    }
}

impl Utf8Array {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(v.to_string()),
        }
    }
}
impl NullArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        Ok("None".to_string())
    }
}
impl BinaryArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            // TODO: [RUST-INT] proper display of bytes as string here, preferably similar to how Python displays it
            // See discussion: https://stackoverflow.com/questions/54358833/how-does-bytes-repr-representation-work
            Some(v) => Ok(format!("b\"{:?}\"", v)),
        }
    }
}

#[cfg(feature = "python")]
impl crate::datatypes::PythonArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        use pyo3::prelude::*;

        let val = self.get(idx);

        let call_result =
            Python::with_gil(|py| val.call_method0(py, pyo3::intern!(py, "__str__")))?;

        let extracted = Python::with_gil(|py| call_result.extract(py))?;

        Ok(extracted)
    }
}

impl DateArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v}")),
        }
    }
}

impl TimestampArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let res = self.get(idx).map_or_else(
            || "None".to_string(),
            |val| -> String {
                use crate::datatypes::DataType::Timestamp;
                use crate::array::ops::cast::{
                    timestamp_to_str_naive,
                    timestamp_to_str_offset,
                    timestamp_to_str_tz,
                };

                let Timestamp(unit, timezone) = &self.field.dtype else { panic!("Wrong dtype for TimestampArray: {}", self.field.dtype) };

                timezone.as_ref().map_or_else(
                    || timestamp_to_str_naive(val, unit),
                    |timezone| {
                        // In arrow, timezone string can be either:
                        // 1. a fixed offset "-07:00", parsed using parse_offset, or
                        // 2. a timezone name e.g. "America/Los_Angeles", parsed using parse_offset_tz.
                        if let Ok(offset) = arrow2::temporal_conversions::parse_offset(timezone) {
                            timestamp_to_str_offset(val, unit, &offset)
                        } else if let Ok(tz) = arrow2::temporal_conversions::parse_offset_tz(timezone) {
                            timestamp_to_str_tz(val, unit, &tz)
                        } else {
                            panic!("Unable to parse timezone string {}", timezone)
                        }
                    },
                )
            }
        );
        Ok(res)
    }
}

// Default implementation of html_value: html escape the str_value.
macro_rules! impl_array_html_value {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn html_value(&self, idx: usize) -> String {
                let str_value = self.str_value(idx).unwrap();
                html_escape::encode_text(&str_value)
                    .into_owned()
                    .replace('\n', "<br />")
            }
        }
    };
}

impl_array_html_value!(Utf8Array);
impl_array_html_value!(BooleanArray);
impl_array_html_value!(NullArray);
impl_array_html_value!(BinaryArray);
impl_array_html_value!(ListArray);
impl_array_html_value!(FixedSizeListArray);
impl_array_html_value!(StructArray);
impl_array_html_value!(ExtensionArray);

#[cfg(feature = "python")]
impl_array_html_value!(crate::datatypes::PythonArray);

impl_array_html_value!(DateArray);
impl_array_html_value!(TimestampArray);
impl_array_html_value!(EmbeddingArray);
impl_array_html_value!(FixedShapeImageArray);

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl ImageArray {
    pub fn html_value(&self, idx: usize) -> String {
        let maybe_image = self.as_image_obj(idx);
        let str_val = self.str_value(idx).unwrap();

        match maybe_image {
            None => "None".to_string(),
            Some(image) => {
                let thumb = image.fit_to(128, 128);
                let mut bytes: Vec<u8> = vec![];
                thumb.encode(ImageFormat::JPEG, &mut bytes).unwrap();
                format!(
                    "<img style=\"max-height:128px;width:auto\" src=\"data:image/png;base64, {}\" alt=\"{}\" />",
                    base64::engine::general_purpose::STANDARD.encode(&bytes),
                    str_val,
                )
            }
        }
    }
}
