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
        if !field.dtype.is_integer()
            && !matches!(
                field.dtype,
                DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_)
            )
        {
            return Err(DaftError::TypeError(format!(
                "Expected input to hex to be integer, string, or binary, got {}",
                field.dtype
            )));
        }
        Ok(Field::new(field.name, DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Returns the uppercase hexadecimal string representation of an integer, string, or binary value."
    }
}

fn hex_impl(s: Series) -> DaftResult<Series> {
    let result = match s.data_type() {
        DataType::UInt64 => {
            let arr = s.u64().unwrap();
            Utf8Array::from_iter(
                arr.name(),
                arr.iter().map(|v| v.map(|n| format!("{:X}", n))),
            )
        }
        dt if dt.is_integer() => {
            let casted = s.cast(&DataType::Int64)?;
            let arr = casted.i64().unwrap();
            // `as u64` matches PySpark: negatives use 64-bit two's complement.
            Utf8Array::from_iter(
                arr.name(),
                arr.iter().map(|v| v.map(|n| format!("{:X}", n as u64))),
            )
        }
        DataType::Utf8 => {
            let arr = s.utf8()?;
            Utf8Array::from_iter(
                arr.name(),
                arr.iter().map(|v| v.map(|s| bytes_to_hex(s.as_bytes()))),
            )
        }
        DataType::Binary | DataType::FixedSizeBinary(_) => {
            let casted = s.cast(&DataType::Binary)?;
            let arr = casted.binary()?;
            Utf8Array::from_iter(arr.name(), arr.iter().map(|v| v.map(bytes_to_hex)))
        }
        dt => {
            return Err(DaftError::TypeError(format!(
                "Expected input to hex to be integer, string, or binary, got {}",
                dt
            )));
        }
    };
    Ok(result.into_series())
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(out, "{:02X}", b).unwrap();
    }
    out
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Unhex;

#[typetag::serde]
impl ScalarUDF for Unhex {
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
        "Converts a hexadecimal string to binary. Odd-length inputs are padded with a leading zero; returns NULL for non-hexadecimal characters."
    }
}

fn unhex_impl(s: Series) -> DaftResult<Series> {
    let arr = s.utf8()?;
    let result = BinaryArray::from_iter(arr.name(), arr.iter().map(|v| v.and_then(unhex_one)));
    Ok(result.into_series())
}

/// Returns NULL if the input contains any non-hexadecimal character.
/// Odd-length inputs get an implicit leading zero, matching Spark's
/// `Hex.unhex` (e.g. "123" parses as [0x01, 0x23]).
fn unhex_one(s: &str) -> Option<Vec<u8>> {
    let digits = s.as_bytes();
    let mut out = Vec::with_capacity(digits.len().div_ceil(2));
    let mut iter = digits.iter();
    if !digits.len().is_multiple_of(2) {
        let lo = (*iter.next().unwrap() as char).to_digit(16)?;
        out.push(lo as u8);
    }
    while let Some(a) = iter.next() {
        let hi = (*a as char).to_digit(16)?;
        let lo = (*iter.next().unwrap() as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

#[must_use]
pub fn hex(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Hex {}, vec![input]).into()
}

#[must_use]
pub fn unhex(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Unhex {}, vec![input]).into()
}
