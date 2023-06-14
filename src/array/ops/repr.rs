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

    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
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

    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl BooleanArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v}")),
        }
    }
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl NullArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        Ok("None".to_string())
    }
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
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
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl ListArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v:?}")),
        }
    }
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl FixedSizeListArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v:?}")),
        }
    }
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl StructArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v:?}")),
        }
    }
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl ExtensionArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v:?}")),
        }
    }
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
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
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
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
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
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

    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl EmbeddingArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v:?}")),
        }
    }
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl ImageArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v:?}")),
        }
    }
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

impl FixedShapeImageArray {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v:?}")),
        }
    }
    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}
