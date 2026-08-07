use std::borrow::Cow;

use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{Utf8ArrayUtils, unary_utf8_evaluate, unary_utf8_to_field};

/// Returns true when `b` does not need to be percent-encoded under
/// `application/x-www-form-urlencoded` (Spark's `url_encode` semantics).
fn is_form_unreserved(b: u8) -> bool {
    matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*')
}

fn encode_url_form(s: &str) -> Cow<'_, str> {
    // Fast path: scan the input first; if everything is already safe, return borrowed.
    let bytes = s.as_bytes();
    let needs_encoding = bytes.iter().any(|b| *b == b' ' || !is_form_unreserved(*b));
    if !needs_encoding {
        return Cow::Borrowed(s);
    }

    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if b == b' ' {
            out.push('+');
        } else if is_form_unreserved(b) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_upper(b >> 4));
            out.push(hex_upper(b & 0x0f));
        }
    }
    Cow::Owned(out)
}

fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'A' + (nibble - 10)) as char,
        _ => unreachable!(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UrlEncode;

#[typetag::serde]
impl ScalarUDF for UrlEncode {
    fn name(&self) -> &'static str {
        "url_encode"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            let arr = s.cast(&DataType::Utf8)?;
            arr.with_utf8_array(|utf8| {
                let result: Utf8Array = utf8.unary_broadcasted_op(encode_url_form)?;
                Ok(result.into_series())
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        unary_utf8_to_field(inputs, schema, self.name(), DataType::Utf8)
    }

    fn docstring(&self) -> &'static str {
        "Translates a string into `application/x-www-form-urlencoded` format using UTF-8 \
        encoding. Equivalent to Spark's `url_encode`. Spaces become `+` and unsafe \
        characters are percent-encoded."
    }
}

#[must_use]
pub fn url_encode(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(UrlEncode, vec![input]).into()
}

/// Decode `application/x-www-form-urlencoded` strings (Spark `url_decode`).
fn decode_url_form(s: &str) -> DaftResult<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(DaftError::ValueError(format!(
                        "url_decode: invalid percent-encoding in input '{s}'"
                    )));
                }
                let high = hex_value(bytes[i + 1]).ok_or_else(|| {
                    DaftError::ValueError(format!("url_decode: invalid hex digit in input '{s}'"))
                })?;
                let low = hex_value(bytes[i + 2]).ok_or_else(|| {
                    DaftError::ValueError(format!("url_decode: invalid hex digit in input '{s}'"))
                })?;
                out.push((high << 4) | low);
                i += 3;
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8(out)
        .map_err(|e| DaftError::ValueError(format!("url_decode: result is not valid UTF-8: {e}")))
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UrlDecode;

#[typetag::serde]
impl ScalarUDF for UrlDecode {
    fn name(&self) -> &'static str {
        "url_decode"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            let arr = s.cast(&DataType::Utf8)?;
            arr.with_utf8_array(|utf8| {
                let mut builder: Vec<Option<String>> = Vec::with_capacity(utf8.len());
                for opt in utf8 {
                    match opt {
                        Some(v) => builder.push(Some(decode_url_form(v)?)),
                        None => builder.push(None),
                    }
                }
                let arr: Utf8Array = builder
                    .into_iter()
                    .collect::<Utf8Array>()
                    .rename(utf8.name());
                Ok(arr.into_series())
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        unary_utf8_to_field(inputs, schema, self.name(), DataType::Utf8)
    }

    fn docstring(&self) -> &'static str {
        "Decodes a string in `application/x-www-form-urlencoded` format using UTF-8 \
        encoding. Equivalent to Spark's `url_decode`. `+` is converted to a space, \
        and `%XX` escape sequences are converted back to bytes."
    }
}

#[must_use]
pub fn url_decode(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(UrlDecode, vec![input]).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_encode_basic() {
        assert_eq!(encode_url_form("Spark SQL"), "Spark+SQL");
        assert_eq!(
            encode_url_form("https://spark.apache.org"),
            "https%3A%2F%2Fspark.apache.org"
        );
        assert_eq!(encode_url_form("hello"), "hello");
        assert_eq!(encode_url_form("a-b_c.d*e"), "a-b_c.d*e");
    }

    #[test]
    fn test_url_encode_unicode() {
        // The character "中" is encoded as 0xE4 0xB8 0xAD in UTF-8.
        assert_eq!(encode_url_form("中"), "%E4%B8%AD");
    }

    #[test]
    fn test_url_decode_basic() {
        assert_eq!(decode_url_form("Spark+SQL").unwrap(), "Spark SQL");
        assert_eq!(
            decode_url_form("https%3A%2F%2Fspark.apache.org").unwrap(),
            "https://spark.apache.org"
        );
        assert_eq!(decode_url_form("hello").unwrap(), "hello");
        assert_eq!(decode_url_form("%E4%B8%AD").unwrap(), "中");
    }

    #[test]
    fn test_url_decode_invalid() {
        assert!(decode_url_form("%ZZ").is_err());
        assert!(decode_url_form("%2").is_err());
    }

    #[test]
    fn test_round_trip() {
        let cases = [
            "hello world",
            "Spark SQL",
            "中文 测试",
            "https://daft.ai/path?x=1&y=2",
        ];
        for c in cases {
            let encoded = encode_url_form(c);
            let decoded = decode_url_form(&encoded).unwrap();
            assert_eq!(decoded, c);
        }
    }
}
