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

/// Compute the Soundex code for a given string.
/// The algorithm:
/// 1. Retain the first letter.
/// 2. Replace consonants with digits (after the first letter):
///    B, F, P, V -> 1
///    C, G, J, K, Q, S, X, Z -> 2
///    D, T -> 3
///    L -> 4
///    M, N -> 5
///    R -> 6
/// 3. Two adjacent letters with the same code are coded as a single number.
///    Letters with the same code separated by 'h' or 'w' are treated as a single number.
///    Letters with the same code separated by a vowel are coded separately.
/// 4. Return the first 4 characters, padding with zeros if necessary.
fn soundex_encode(input: &str) -> String {
    if input.is_empty() {
        return String::new();
    }

    let mut chars = input.chars().filter(|c| c.is_ascii_alphabetic());

    let first_char = match chars.next() {
        Some(c) => c.to_ascii_uppercase(),
        None => return String::new(),
    };

    let mut result = String::with_capacity(4);
    result.push(first_char);

    let mut last_code = soundex_code(first_char);
    let mut last_was_hw = false;

    for c in chars {
        let upper = c.to_ascii_uppercase();
        let code = soundex_code(upper);

        if code == '0' {
            // Vowel (A, E, I, O, U, Y) separates same-code letters
            // H and W do NOT separate same-code letters
            if matches!(upper, 'H' | 'W') {
                last_was_hw = true;
            } else {
                // It's a vowel separator - next same-code letter will be coded separately
                last_was_hw = false;
                last_code = '0';
            }
        } else if code != last_code || (!last_was_hw && last_code == '0') {
            result.push(code);
            if result.len() == 4 {
                return result;
            }
            last_code = code;
            last_was_hw = false;
        } else {
            // Same code as last, skip (adjacent or separated by h/w)
            last_was_hw = false;
        }
    }

    // Pad with zeros
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
