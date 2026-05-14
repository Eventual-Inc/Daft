use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::Utf8Array,
    prelude::{DataType, Field, FullNull, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Conv;

#[derive(FunctionArgs)]
struct ConvArgs<T> {
    num: T,
    from_base: i64,
    to_base: i64,
}

#[typetag::serde]
impl ScalarUDF for Conv {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ConvArgs {
            num,
            from_base,
            to_base,
        } = inputs.try_into()?;
        conv_impl(num, from_base, to_base)
    }

    fn name(&self) -> &'static str {
        "conv"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ConvArgs { num, .. } = inputs.try_into()?;
        let f = num.to_field(schema)?;
        if !matches!(f.dtype, DataType::Utf8) && !f.dtype.is_integer() {
            return Err(DaftError::TypeError(format!(
                "Expected input to conv to be string or integer, got {}",
                f.dtype
            )));
        }
        Ok(Field::new(f.name, DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Converts a number from one base to another (bases 2-36). \
         Negative `to_base` returns a signed result; positive `to_base` \
         interprets negative inputs as 64-bit two's complement. Trailing \
         invalid characters are silently truncated. Returns NULL on \
         out-of-range bases, on u64 overflow during parsing, or when \
         a negated magnitude exceeds 2^63."
    }
}

fn conv_impl(num: Series, from_base: i64, to_base: i64) -> DaftResult<Series> {
    // Spark casts num to string implicitly (`ImplicitCastInputTypes`).
    let casted = num.cast(&DataType::Utf8)?;
    let utf8 = casted.utf8()?;

    let in_range = |b: i64| (2..=36).contains(&b);
    if !in_range(from_base) || !to_base.checked_abs().is_some_and(in_range) {
        return Ok(Utf8Array::full_null(utf8.name(), &DataType::Utf8, utf8.len()).into_series());
    }

    let result = Utf8Array::from_iter(
        utf8.name(),
        utf8.iter()
            .map(|s| s.and_then(|s| convert_one(s, from_base, to_base))),
    );
    Ok(result.into_series())
}

/// Returns NULL on u64 overflow during parsing or when a negated magnitude
/// exceeds 2^63 (cannot be represented as a 64-bit two's-complement value).
fn convert_one(s: &str, from_base: i64, to_base: i64) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (negative, body) = s.strip_prefix('-').map_or((false, s), |b| (true, b));

    // Stop at the first invalid char (Hive/MySQL parity, e.g. "11abc" base 10 -> 11).
    let mut v: u64 = 0;
    for c in body.chars() {
        match c.to_digit(from_base as u32) {
            Some(d) => v = v.checked_mul(from_base as u64)?.checked_add(d as u64)?,
            None => break,
        }
    }

    // Spark NumberConverter sign rules (mathExpressions.scala L167-191).
    let (output_v, prefix_minus) = if negative && to_base > 0 {
        if v > i64::MIN.unsigned_abs() {
            return None;
        }
        (v.wrapping_neg(), false)
    } else if to_base < 0 && (v as i64) < 0 {
        ((v as i64).wrapping_neg() as u64, true)
    } else {
        (v, negative && to_base < 0)
    };

    let mut out = u64_to_base_string(output_v, to_base.unsigned_abs() as u32);
    if prefix_minus {
        out.insert(0, '-');
    }
    Some(out)
}

fn u64_to_base_string(mut n: u64, base: u32) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let mut digits = Vec::with_capacity(64);
    while n > 0 {
        let d = (n % base as u64) as u32;
        digits.push(char::from_digit(d, base).unwrap().to_ascii_uppercase());
        n /= base as u64;
    }
    digits.iter().rev().collect()
}

#[must_use]
pub fn conv(num: ExprRef, from_base: ExprRef, to_base: ExprRef) -> ExprRef {
    ScalarFn::builtin(Conv {}, vec![num, from_base, to_base]).into()
}
