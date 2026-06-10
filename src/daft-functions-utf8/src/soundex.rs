use std::borrow::Cow;

use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{Utf8ArrayUtils, unary_utf8_evaluate, unary_utf8_to_field};

/// Spark-compatible `soundex` function.
/// Returns the Soundex code of the string.
/// Soundex is a phonetic algorithm that produces a 4-character code.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Soundex;

#[typetag::serde]
impl ScalarUDF for Soundex {
    fn name(&self) -> &'static str {
        "soundex"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            s.with_utf8_array(|arr| {
                Ok(arr
                    .unary_broadcasted_op(|val| Cow::Owned(soundex_encode(val)))?
                    .into_series())
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
        "Returns the Soundex code of the string. Soundex is a phonetic algorithm that produces a 4-character code representing the sound of the string."
    }
}

#[must_use]
pub fn soundex(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Soundex {}, vec![input]).into()
}

/// Compute the Soundex code for a given string, matching Spark's semantics:
///
/// 1. If the input does not start with an ASCII letter, it is returned unchanged
///    (Spark: `soundex('3M') == '3M'`).
/// 2. Otherwise, retain the first letter as the initial of the result.
/// 3. Replace consonants with digits (after the first letter):
///    - `B, F, P, V` → `1`
///    - `C, G, J, K, Q, S, X, Z` → `2`
///    - `D, T` → `3`
///    - `L` → `4`
///    - `M, N` → `5`
///    - `R` → `6`
/// 4. Adjacent letters with the same code collapse into a single digit. Letters
///    separated by `H` or `W` are still considered adjacent. Vowels (and any
///    non-letter character — Spark treats interior non-letters as separators)
///    reset the "last code" so that same-coded letters on either side are
///    emitted separately. e.g. `soundex('S3S') == 'S200'`.
/// 5. Truncate or right-pad with zeros to 4 characters.
fn soundex_encode(input: &str) -> String {
    // Spark returns the input unchanged when it does not start with a letter.
    let mut bytes = input.bytes();
    let first_byte = match bytes.next() {
        Some(b) => b,
        None => return String::new(),
    };
    if !first_byte.is_ascii_alphabetic() {
        return input.to_string();
    }

    let first_char = (first_byte as char).to_ascii_uppercase();

    let mut result = String::with_capacity(4);
    result.push(first_char);

    let mut last_code = soundex_code(first_char);

    for c in input.chars().skip(1) {
        if !c.is_ascii_alphabetic() {
            // Spark treats interior non-letters as separators: a same-coded
            // letter that follows must be emitted separately.
            last_code = '0';
            continue;
        }

        let upper = c.to_ascii_uppercase();
        let code = soundex_code(upper);

        if code == '0' {
            // Vowels reset the running code so the next same-coded consonant is
            // emitted separately; H and W do NOT (so a same-coded letter after
            // an H/W is treated as adjacent and skipped).
            if !matches!(upper, 'H' | 'W') {
                last_code = '0';
            }
        } else if code != last_code {
            result.push(code);
            if result.len() == 4 {
                return result;
            }
            last_code = code;
        }
        // else: same code as last (adjacent or separated by H/W) — skip.
    }

    // Right-pad with zeros to 4 characters.
    while result.len() < 4 {
        result.push('0');
    }

    result
}

/// Map a character to its Soundex code.
fn soundex_code(c: char) -> char {
    match c {
        'B' | 'F' | 'P' | 'V' => '1',
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
        'D' | 'T' => '3',
        'L' => '4',
        'M' | 'N' => '5',
        'R' => '6',
        _ => '0', // A, E, I, O, U, H, W, Y
    }
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::Utf8Array;

    use super::*;

    #[test]
    fn test_soundex_basic() {
        assert_eq!(soundex_encode("Robert"), "R163");
        assert_eq!(soundex_encode("Rupert"), "R163");
        assert_eq!(soundex_encode("Rubin"), "R150");
        assert_eq!(soundex_encode("Ashcraft"), "A261");
        assert_eq!(soundex_encode("Ashcroft"), "A261");
        assert_eq!(soundex_encode("Tymczak"), "T522");
    }

    #[test]
    fn test_soundex_empty() {
        assert_eq!(soundex_encode(""), "");
    }

    #[test]
    fn test_soundex_single_char() {
        assert_eq!(soundex_encode("A"), "A000");
    }

    #[test]
    fn test_soundex_non_letter_start_passthrough() {
        // Spark: when the input does not start with a letter, return it unchanged.
        assert_eq!(soundex_encode("3M"), "3M");
        assert_eq!(soundex_encode("123"), "123");
    }

    #[test]
    fn test_soundex_interior_non_letter_separator() {
        // Spark: interior non-letters act as separators, so adjacent same-coded
        // letters across them are emitted twice. soundex('S3S') == 'S200'.
        assert_eq!(soundex_encode("S3S"), "S200");
    }

    #[test]
    fn test_soundex_array() {
        let arr = Utf8Array::from_iter("a", vec![Some("Robert"), Some("Rupert"), None].into_iter());

        let result = arr
            .unary_broadcasted_op(|val| Cow::Owned(soundex_encode(val)))
            .unwrap();

        assert_eq!(result.get(0), Some("R163"));
        assert_eq!(result.get(1), Some("R163"));
        assert_eq!(result.get(2), None);
    }
}
