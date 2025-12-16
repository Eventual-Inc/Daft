use common_display::table_display::StrValue;
use common_error::DaftResult;

#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftNumericType, DataType, Decimal128Array, ExtensionArray,
        FileArray, FixedSizeBinaryArray, IntervalArray, IntervalValue, NullArray, UInt64Array,
        Utf8Array,
        logical::{
            DateArray, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeSparseTensorArray, FixedShapeTensorArray, ImageArray, MapArray,
            SparseTensorArray, TensorArray, TimeArray, TimestampArray,
        },
    },
    file::DaftMediaType,
    series::Series,
    utils::display::{
        display_date32, display_decimal128, display_duration, display_time64, display_timestamp,
    },
    with_match_daft_types,
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
impl_array_str_value!(ExtensionArray, "{:?}");

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
    }

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
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );
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

impl FixedSizeBinaryArray {
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
impl PythonArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        use pyo3::prelude::*;

        let val = self.get(idx);

        if let Some(val) = val {
            Ok(Python::attach(|py| {
                val.bind(py)
                    .call_method0(pyo3::intern!(py, "__str__"))?
                    .extract()
            })?)
        } else {
            Ok("None".to_string())
        }
    }
}

impl DateArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(display_date32(v)),
        }
    }
}

impl TimeArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let res = self.get(idx).map_or_else(
            || "None".to_string(),
            |val| -> String {
                let DataType::Time(unit) = &self.field.dtype else {
                    panic!("Wrong dtype for TimeArray: {}", self.field.dtype)
                };
                display_time64(val, unit)
            },
        );
        Ok(res)
    }
}

impl TimestampArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let res = self.get(idx).map_or_else(
            || "None".to_string(),
            |val| -> String {
                let DataType::Timestamp(unit, timezone) = &self.field.dtype else {
                    panic!("Wrong dtype for TimestampArray: {}", self.field.dtype)
                };
                display_timestamp(val, unit, timezone)
            },
        );
        Ok(res)
    }
}

impl DurationArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let res = self.get(idx).map_or_else(
            || "None".to_string(),
            |val| -> String {
                let DataType::Duration(time_unit) = &self.field.dtype else {
                    panic!("Wrong dtype for DurationArray: {}", self.field.dtype)
                };
                display_duration(val, time_unit)
            },
        );
        Ok(res)
    }
}

impl IntervalArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let res = self.get(idx).map_or_else(
            || "None".to_string(),
            |v| -> String {
                let value: IntervalValue = v.into();
                format!("{value}")
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
                use crate::datatypes::DataType::Decimal128;
                let Decimal128(precision, scale) = &self.field.dtype else {
                    panic!("Wrong dtype for Decimal128Array: {}", self.field.dtype)
                };
                display_decimal128(val, *precision as u8, *scale as i8)
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

impl MapArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => series_as_list_str(&v),
        }
    }
}

fn sparkline_from_floats(values: &[f32], num_bins: usize) -> String {
    const SPARK_CHARS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    if values.is_empty() {
        return String::new();
    }

    // Compute bin size (last bin may be smaller)
    let bin_size = (values.len() as f32 / num_bins as f32).ceil() as usize;

    let mut bins = Vec::with_capacity(num_bins);

    for i in (0..values.len()).step_by(bin_size) {
        let end = usize::min(i + bin_size, values.len());
        let slice = &values[i..end];
        let energy = slice.iter().copied().map(|x| x * x).sum::<f32>() / slice.len() as f32;
        bins.push(energy.sqrt());
    }

    // Normalize bins to 0-1
    let min = bins.iter().copied().fold(f32::INFINITY, f32::min);
    let max = bins.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let range = (max - min).max(1e-8); // avoid division by 0

    bins.iter()
        .map(|&v| {
            let norm = (v - min) / range;
            let idx = (norm * (SPARK_CHARS.len() as f32 - 1.0)).round() as usize;
            SPARK_CHARS[idx]
        })
        .collect()
}

impl EmbeddingArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if self.physical.is_valid(idx) {
            let value = self
                .get(idx)
                .expect("str_value should only be called in bounds");
            let value_casted = value.cast(&DataType::Float32)?;
            let value_f32 = value_casted.f32()?;
            let slice = value_f32.as_slice();
            let sparkline = sparkline_from_floats(slice, 24);
            Ok(sparkline)
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

impl SparseTensorArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        // Shapes are always valid, use values array validity
        let is_valid = self
            .values_array()
            .validity()
            .is_none_or(|v| v.is_valid(idx));
        let shape_element = if is_valid {
            self.shape_array().get(idx)
        } else {
            None
        };
        match shape_element {
            Some(shape) => Ok(format!(
                "<SparseTensor shape=({})>",
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

impl FixedShapeSparseTensorArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if self.physical.is_valid(idx) {
            Ok("<FixedShapeSparseTensor>".to_string())
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
                        .filter(|(f, _)| !f.name.is_empty() && f.dtype != DataType::Null)
                        .map(|(f, s)| Ok(format!("{}: {},\n", f.name.as_str(), s.str_value(idx))))
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
impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        Ok(self.get_lit(idx).to_string())
    }
}

// Truncate strings so they do not crash the browser when rendering HTML
fn truncate_for_html(s: &str) -> String {
    // Limit string length to 1MB to prevent browser crashes
    const MAX_HTML_STRING_LEN: usize = 1024 * 1024; // 1MB

    if s.len() > MAX_HTML_STRING_LEN {
        // Find the last character boundary at or before MAX_HTML_STRING_LEN
        let mut end_idx = MAX_HTML_STRING_LEN;
        while !s.is_char_boundary(end_idx) && end_idx > 0 {
            end_idx -= 1;
        }
        format!("{}...", &s[..end_idx])
    } else {
        s.to_string()
    }
}

// Default implementation of html_value: html escape the str_value with truncation.
macro_rules! impl_array_html_value {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn html_value(&self, idx: usize, truncate: bool) -> String {
                let str_value = self.str_value(idx).unwrap();
                let truncated = if truncate {
                    truncate_for_html(&str_value)
                } else {
                    str_value
                };
                html_escape::encode_text(&truncated)
                    .into_owned()
                    .replace('\n', "<br />")
            }
        }
    };
}

impl Utf8Array {
    pub fn html_value(&self, idx: usize, truncate: bool) -> String {
        let str_value = self.str_value(idx).unwrap();
        let truncated = if truncate {
            truncate_for_html(&str_value)
        } else {
            str_value
        };
        html_escape::encode_text(&truncated)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl_array_html_value!(BooleanArray);
impl_array_html_value!(NullArray);
impl_array_html_value!(BinaryArray);
impl_array_html_value!(FixedSizeBinaryArray);
impl_array_html_value!(ListArray);
impl_array_html_value!(FixedSizeListArray);
impl_array_html_value!(MapArray);
impl_array_html_value!(StructArray);
impl_array_html_value!(ExtensionArray);
impl_array_html_value!(Decimal128Array);
impl_array_html_value!(DateArray);
impl_array_html_value!(TimeArray);
impl_array_html_value!(DurationArray);
impl_array_html_value!(IntervalArray);
impl_array_html_value!(TimestampArray);
impl_array_html_value!(EmbeddingArray);

#[cfg(feature = "python")]
impl PythonArray {
    pub fn html_value(&self, idx: usize, truncate: bool) -> String {
        use pyo3::prelude::*;

        let val = self.get(idx);

        let custom_viz_hook_result: Option<String> = Python::attach(|py| {
            if let Some(val) = val {
                // Find visualization hooks for this object's class
                let pyany = val.bind(py);
                let get_viz_hook = py
                    .import(pyo3::intern!(py, "daft.viz.html_viz_hooks"))?
                    .getattr(pyo3::intern!(py, "get_viz_hook"))?;
                let hook = get_viz_hook.call1((pyany,))?;

                if hook.is_none() {
                    Ok(None)
                } else {
                    hook.call1((pyany,))?.extract()
                }
            } else {
                Ok(None)
            }
        })
        .unwrap();

        match custom_viz_hook_result {
            None => {
                let str_value = self.str_value(idx).unwrap();
                let truncated = if truncate {
                    truncate_for_html(&str_value)
                } else {
                    str_value
                };
                html_escape::encode_text(&truncated)
                    .into_owned()
                    .replace('\n', "<br />")
            }
            Some(result) => truncate_for_html(&result),
        }
    }
}

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn html_value(&self, idx: usize, truncate: bool) -> String {
        let str_value = self.str_value(idx).unwrap();
        let truncated = if truncate {
            truncate_for_html(&str_value)
        } else {
            str_value
        };
        html_escape::encode_text(&truncated)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl FixedShapeTensorArray {
    pub fn html_value(&self, idx: usize, truncate: bool) -> String {
        let str_value = self.str_value(idx).unwrap();
        let truncated = if truncate {
            truncate_for_html(&str_value)
        } else {
            str_value
        };
        html_escape::encode_text(&truncated)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl TensorArray {
    pub fn html_value(&self, idx: usize, truncate: bool) -> String {
        let str_value = self.str_value(idx).unwrap();
        let truncated = if truncate {
            truncate_for_html(&str_value)
        } else {
            str_value
        };
        html_escape::encode_text(&truncated)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl SparseTensorArray {
    pub fn html_value(&self, idx: usize, truncate: bool) -> String {
        let str_value = self.str_value(idx).unwrap();
        let truncated = if truncate {
            truncate_for_html(&str_value)
        } else {
            str_value
        };
        html_escape::encode_text(&truncated)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl FixedShapeSparseTensorArray {
    pub fn html_value(&self, idx: usize, truncate: bool) -> String {
        let str_value = self.str_value(idx).unwrap();
        let truncated = if truncate {
            truncate_for_html(&str_value)
        } else {
            str_value
        };
        html_escape::encode_text(&truncated)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub fn html_value(&self, idx: usize, truncate: bool) -> String {
        let str_value = self.str_value(idx).unwrap();
        let truncated = if truncate {
            truncate_for_html(&str_value)
        } else {
            str_value
        };
        html_escape::encode_text(&truncated)
            .into_owned()
            .replace('\n', "<br />")
    }
}
