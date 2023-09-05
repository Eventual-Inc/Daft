use base64::Engine;

use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{
        logical::{
            DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeTensorArray, ImageArray, TensorArray, TimestampArray,
        },
        BinaryArray, BooleanArray, DaftNumericType, ExtensionArray, ImageFormat, NullArray,
        UInt64Array, Utf8Array,
    },
    with_match_daft_types, DataType, Series,
};
use common_error::DaftResult;

use super::image::AsImageObj;

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
impl_array_str_value!(ExtensionArray, "{:?}");
impl_array_str_value!(DurationArray, "{}");

fn pretty_print_bytes(bytes: &[u8], max_len: usize) -> DaftResult<String> {
    /// influenced by pythons bytes repr
    /// https://github.com/python/cpython/blob/main/Objects/bytesobject.c#L1336
    const PREFIX: &str = "b\"";
    const POSTFIX: &str = "\"";
    const POSTFIX_TRUNC: &str = "\"...";
    let ending_bytes = if bytes.len() <= max_len {
        POSTFIX.len()
    } else {
        POSTFIX_TRUNC.len()
    };
    let mut builder = String::with_capacity(max_len);
    builder.push_str(PREFIX);
    const QUOTE: u8 = b'\"';
    const SLASH: u8 = b'\\';
    const TAB: u8 = b'\t';
    const NEWLINE: u8 = b'\n';
    const CARRIAGE: u8 = b'\r';
    const SPACE: u8 = b' ';

    use std::fmt::Write;
    let chars_to_add = max_len - ending_bytes;

    for c in bytes {
        if builder.len() >= chars_to_add {
            break;
        }
        match *c {
            QUOTE | SLASH => write!(builder, "\\{}", *c as char),
            TAB => write!(builder, "\\t"),
            NEWLINE => write!(builder, "\\n"),
            CARRIAGE => write!(builder, "\\r"),
            v if !(SPACE..0x7f).contains(&v) => write!(builder, "\\x{:02x}", v),
            v => write!(builder, "{}", v as char),
        }?;
    }
    if bytes.len() <= max_len {
        builder.push_str(POSTFIX);
    } else {
        builder.push_str(POSTFIX_TRUNC);
    };

    Ok(builder)
}

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
            Some(v) => {
                const LEN_TO_TRUNC: usize = 40;
                pretty_print_bytes(v, LEN_TO_TRUNC)
            }
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
                use crate::array::ops::cast::{
                    timestamp_to_str_naive, timestamp_to_str_offset, timestamp_to_str_tz,
                };
                use crate::datatypes::DataType::Timestamp;

                let Timestamp(unit, timezone) = &self.field.dtype else {
                    panic!("Wrong dtype for TimestampArray: {}", self.field.dtype)
                };

                timezone.as_ref().map_or_else(
                    || timestamp_to_str_naive(val, unit),
                    |timezone| {
                        // In arrow, timezone string can be either:
                        // 1. a fixed offset "-07:00", parsed using parse_offset, or
                        // 2. a timezone name e.g. "America/Los_Angeles", parsed using parse_offset_tz.
                        if let Ok(offset) = arrow2::temporal_conversions::parse_offset(timezone) {
                            timestamp_to_str_offset(val, unit, &offset)
                        } else if let Ok(tz) =
                            arrow2::temporal_conversions::parse_offset_tz(timezone)
                        {
                            timestamp_to_str_tz(val, unit, &tz)
                        } else {
                            panic!("Unable to parse timezone string {}", timezone)
                        }
                    },
                )
            },
        );
        Ok(res)
    }
}

impl Decimal128Array {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let res = self.get(idx).map_or_else(
            || "None".to_string(),
            |val| -> String {
                use crate::array::ops::cast::decimal128_to_str;
                use crate::datatypes::DataType::Decimal128;
                let Decimal128(precision, scale) = &self.field.dtype else {
                    panic!("Wrong dtype for Decimal128Array: {}", self.field.dtype)
                };
                decimal128_to_str(val, *precision as u8, *scale as i8)
            },
        );
        Ok(res)
    }
}

/// Helper that prints a Series as a list ("[e1, e2, e3, ...]")
fn series_as_list_str(series: &Series) -> DaftResult<String> {
    with_match_daft_types!(series.data_type(), |$T| {
        let arr = series.downcast::<<$T as DaftDataType>::ArrayType>()?;
        let mut s = String::new();
        s += "[";
        s += (0..series.len()).map(|i| arr.str_value(i)).collect::<DaftResult<Vec<String>>>()?.join(", ").as_str();
        s += "]";
        Ok(s)
    })
}

impl ListArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => series_as_list_str(&v),
        }
    }
}

impl FixedSizeListArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => series_as_list_str(&v),
        }
    }
}

impl EmbeddingArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if self.physical.is_valid(idx) {
            Ok("<Embedding>".to_string())
        } else {
            Ok("None".to_string())
        }
    }
}

impl ImageArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if self.physical.is_valid(idx) {
            Ok("<Image>".to_string())
        } else {
            Ok("None".to_string())
        }
    }
}

impl FixedShapeImageArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if self.physical.is_valid(idx) {
            Ok("<FixedShapeImage>".to_string())
        } else {
            Ok("None".to_string())
        }
    }
}

impl FixedShapeTensorArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if self.physical.is_valid(idx) {
            Ok("<FixedShapeTensor>".to_string())
        } else {
            Ok("None".to_string())
        }
    }
}

impl TensorArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let shape_element = self.physical.children[1]
            .downcast::<ListArray>()
            .unwrap()
            .get(idx);
        match shape_element {
            Some(shape) => Ok(format!(
                "<Tensor shape=({})>",
                shape
                    .downcast::<UInt64Array>()
                    .unwrap()
                    .into_iter()
                    .map(|dim| match dim {
                        None => "None".to_string(),
                        Some(dim) => dim.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            None => Ok("None".to_string()),
        }
    }
}

impl StructArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if self.is_valid(idx) {
            match &self.field.dtype {
                DataType::Struct(fields) => {
                    let fields_to_strs = fields
                        .iter()
                        .zip(self.children.iter())
                        .map(|(f, s)| Ok(format!("{}: {},\n", f.name.as_str(), s.str_value(idx)?)))
                        .collect::<DaftResult<Vec<_>>>()?;
                    let mut result = "{".to_string();
                    for line in fields_to_strs {
                        result += &line;
                    }
                    result += "}";
                    Ok(result)
                }
                dt => unreachable!("StructArray must have Struct dtype, but found: {}", dt),
            }
        } else {
            Ok("None".to_string())
        }
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
impl_array_html_value!(Decimal128Array);
impl_array_html_value!(DateArray);
impl_array_html_value!(DurationArray);
impl_array_html_value!(TimestampArray);
impl_array_html_value!(EmbeddingArray);

#[cfg(feature = "python")]
impl crate::datatypes::PythonArray {
    pub fn html_value(&self, idx: usize) -> String {
        use pyo3::prelude::*;

        let val = self.get(idx);

        let custom_viz_hook_result: Option<String> = Python::with_gil(|py| {
            // Find visualization hooks for this object's class
            let pyany = val.into_ref(py);
            let get_viz_hook = py
                .import("daft.viz.html_viz_hooks")?
                .getattr("get_viz_hook")?;
            let hook = get_viz_hook.call1((pyany,))?;

            if hook.is_none() {
                Ok(None)
            } else {
                hook.call1((pyany,))?.extract()
            }
        })
        .unwrap();

        match custom_viz_hook_result {
            None => html_escape::encode_text(&self.str_value(idx).unwrap())
                .into_owned()
                .replace('\n', "<br />"),
            Some(result) => result,
        }
    }
}

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
                let mut writer = std::io::BufWriter::new(std::io::Cursor::new(&mut bytes));
                thumb.encode(ImageFormat::JPEG, &mut writer).unwrap();
                drop(writer);
                format!(
                    "<img style=\"max-height:128px;width:auto\" src=\"data:image/png;base64, {}\" alt=\"{}\" />",
                    base64::engine::general_purpose::STANDARD.encode(&mut bytes),
                    str_val,
                )
            }
        }
    }
}

impl FixedShapeImageArray {
    pub fn html_value(&self, idx: usize) -> String {
        let maybe_image = self.as_image_obj(idx);
        let str_val = self.str_value(idx).unwrap();

        match maybe_image {
            None => "None".to_string(),
            Some(image) => {
                let thumb = image.fit_to(128, 128);
                let mut bytes: Vec<u8> = vec![];
                let mut writer = std::io::BufWriter::new(std::io::Cursor::new(&mut bytes));
                thumb.encode(ImageFormat::JPEG, &mut writer).unwrap();
                drop(writer);
                format!(
                    "<img style=\"max-height:128px;width:auto\" src=\"data:image/png;base64, {}\" alt=\"{}\" />",
                    base64::engine::general_purpose::STANDARD.encode(&mut bytes),
                    str_val,
                )
            }
        }
    }
}

impl FixedShapeTensorArray {
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl TensorArray {
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}
