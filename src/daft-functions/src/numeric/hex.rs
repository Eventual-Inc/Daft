use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{BinaryArray, Utf8Array},
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Hex;

#[typetag::serde]
impl ScalarUDF for Hex {
    /// Spark `hex`: integer -> uppercase hex (negatives use 64-bit two's complement);
    /// string/binary -> uppercase hex of the bytes.
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        hex_impl(input)
    }

    fn name(&self) -> &'static str {
        "hex"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        if !field.dtype.is_integer() && !matches!(field.dtype, DataType::Utf8 | DataType::Binary) {
            return Err(DaftError::TypeError(format!(
                "Expected input to hex to be integer, string, or binary, got {}",
                field.dtype
            )));
        }
        Ok(Field::new(field.name, DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Converts an integer to its uppercase hexadecimal string. Negatives are \
         encoded as 64-bit two's complement (e.g. `hex(-1) == 'FFFFFFFFFFFFFFFF'`). \
         For string or binary inputs, returns the uppercase hex of the underlying bytes."
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Unhex;

#[typetag::serde]
impl ScalarUDF for Unhex {
    /// Spark `unhex`: hex-string -> binary. Odd-length strings are left-padded
    /// with `0`. Returns NULL for inputs containing non-hex characters.
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        unhex_impl(input)
    }

    fn name(&self) -> &'static str {
        "unhex"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        if !matches!(field.dtype, DataType::Utf8) {
            return Err(DaftError::TypeError(format!(
                "Expected input to unhex to be string, got {}",
                field.dtype
            )));
        }
        Ok(Field::new(field.name, DataType::Binary))
    }

    fn docstring(&self) -> &'static str {
        "Inverse of `hex`. Decodes a hexadecimal string into binary bytes. \
         Odd-length inputs are left-padded with '0'. Returns NULL when the input \
         contains characters outside `[0-9a-fA-F]`."
    }
}

fn hex_impl(s: Series) -> DaftResult<Series> {
    let result = match s.data_type() {
        DataType::Utf8 => {
            let arr = s.utf8()?;
            Utf8Array::from_iter(
                arr.name(),
                arr.iter()
                    .map(|v| v.map(|s| bytes_to_hex_upper(s.as_bytes()))),
            )
        }
        DataType::Binary => {
            let arr = s.binary()?;
            Utf8Array::from_iter(
                arr.name(),
                arr.into_iter().map(|v| v.map(bytes_to_hex_upper)),
            )
        }
        DataType::UInt64 => {
            let arr = s.u64()?;
            Utf8Array::from_iter(
                arr.name(),
                arr.iter().map(|v| v.map(|n| format!("{:X}", n))),
            )
        }
        dt if dt.is_integer() => {
            // Spark/Hive: cast to i64 and reinterpret as u64 so negatives produce
            // the 64-bit two's-complement upper-case hex (e.g. -1 -> FFFFFFFFFFFFFFFF).
            let casted = s.cast(&DataType::Int64)?;
            let arr = casted.i64().unwrap();
            Utf8Array::from_iter(
                arr.name(),
                arr.iter().map(|v| v.map(|n| format!("{:X}", n as u64))),
            )
        }
        dt => {
            return Err(DaftError::TypeError(format!(
                "hex not implemented for {}",
                dt
            )));
        }
    };
    Ok(result.into_series())
}

fn unhex_impl(s: Series) -> DaftResult<Series> {
    let arr = s.utf8()?;
    let result: BinaryArray = BinaryArray::from_iter(
        arr.name(),
        arr.iter().map(|v| v.and_then(decode_hex_padded)),
    );
    Ok(result.into_series())
}

#[inline]
fn bytes_to_hex_upper(bytes: &[u8]) -> String {
    // Manual upper-case hex for parity with Spark (which always uppercases).
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0F) as usize] as char);
    }
    out
}

/// Decodes a hex string into bytes, left-padding odd-length inputs with '0'.
/// Returns `None` if any character is not a valid hex digit, matching Spark
/// `Unhex` (`org.apache.spark.sql.catalyst.expressions.Hex#unhex`).
fn decode_hex_padded(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let out_len = len.div_ceil(2);
    let mut out = Vec::with_capacity(out_len);

    // If the length is odd, treat it as if it were left-padded with a '0' so
    // that "F" becomes 0x0F. Spark's behaviour: unhex('F') -> [0x0F].
    let mut i = if len % 2 == 1 {
        out.push(hex_digit(bytes[0])?);
        1
    } else {
        0
    };
    while i < len {
        let hi = hex_digit(bytes[i])?;
        let lo = hex_digit(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

#[inline]
fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[must_use]
pub fn hex(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Hex {}, vec![input]).into()
}

#[must_use]
pub fn unhex(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Unhex {}, vec![input]).into()
}

#[cfg(test)]
mod tests {
    use daft_core::{
        datatypes::{BinaryArray, Int64Array, Utf8Array},
        prelude::{DataType, Field},
        series::IntoSeries,
    };
    use daft_dsl::functions::{FunctionArgs, ScalarUDF, scalar::EvalContext};

    use super::{Hex, Unhex};

    #[test]
    fn test_hex_integers() {
        let s = Int64Array::from_iter(
            Field::new("x", DataType::Int64),
            vec![Some(0i64), Some(255), Some(-1), None].into_iter(),
        )
        .into_series();
        let ctx = EvalContext { row_count: s.len() };
        let out = Hex {}
            .call(FunctionArgs::new_unnamed(vec![s]), &ctx)
            .unwrap();
        let expected = Utf8Array::from_iter(
            "x",
            vec![Some("0"), Some("FF"), Some("FFFFFFFFFFFFFFFF"), None].into_iter(),
        );
        assert_eq!(out.utf8().unwrap(), &expected);
    }

    #[test]
    fn test_hex_string_input() {
        // Spark: hex('Spark') -> '537061726B'
        let s = Utf8Array::from_iter("x", vec![Some("Spark"), Some(""), None].into_iter())
            .into_series();
        let ctx = EvalContext { row_count: s.len() };
        let out = Hex {}
            .call(FunctionArgs::new_unnamed(vec![s]), &ctx)
            .unwrap();
        let expected =
            Utf8Array::from_iter("x", vec![Some("537061726B"), Some(""), None].into_iter());
        assert_eq!(out.utf8().unwrap(), &expected);
    }

    #[test]
    fn test_unhex_basic() {
        // Inverse of hex('Spark') and odd-length padding: 'F' -> [0x0F].
        let s = Utf8Array::from_iter(
            "x",
            vec![Some("537061726B"), Some("F"), Some(""), None].into_iter(),
        )
        .into_series();
        let ctx = EvalContext { row_count: s.len() };
        let out = Unhex {}
            .call(FunctionArgs::new_unnamed(vec![s]), &ctx)
            .unwrap();
        let expected = BinaryArray::from_iter(
            "x",
            vec![
                Some(b"Spark".to_vec()),
                Some(vec![0x0F]),
                Some(vec![]),
                None,
            ]
            .into_iter(),
        );
        assert_eq!(out.binary().unwrap(), &expected);
    }

    #[test]
    fn test_unhex_invalid_returns_null() {
        // 'GG' is not valid hex -> NULL (matches Spark's Unhex behaviour).
        let s = Utf8Array::from_iter("x", vec![Some("GG"), Some("aZ"), Some("aB")].into_iter())
            .into_series();
        let ctx = EvalContext { row_count: s.len() };
        let out = Unhex {}
            .call(FunctionArgs::new_unnamed(vec![s]), &ctx)
            .unwrap();
        let expected = BinaryArray::from_iter("x", vec![None, None, Some(vec![0xAB])].into_iter());
        assert_eq!(out.binary().unwrap(), &expected);
    }

    #[test]
    fn test_hex_lowercase_input_uppercased() {
        // hex(int) always emits uppercase; verify with a value containing all letter digits.
        let s = Int64Array::from_iter(
            Field::new("x", DataType::Int64),
            vec![Some(0xabcdefi64)].into_iter(),
        )
        .into_series();
        let ctx = EvalContext { row_count: s.len() };
        let out = Hex {}
            .call(FunctionArgs::new_unnamed(vec![s]), &ctx)
            .unwrap();
        let expected = Utf8Array::from_iter("x", vec![Some("ABCDEF")].into_iter());
        assert_eq!(out.utf8().unwrap(), &expected);
    }
}
