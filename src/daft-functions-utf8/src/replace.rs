use std::borrow::{Borrow, Cow};

use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    prelude::{DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{create_broadcasted_str_iter, parse_inputs};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegexpReplace;

#[typetag::serde]
impl ScalarUDF for RegexpReplace {
    fn name(&self) -> &'static str {
        "regexp_replace"
    }
    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        let replacement = inputs.required((2, "replacement"))?;
        series_replace(input, pattern, replacement, true)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        get_return_field_impl(inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Replaces all occurrences of a substring with a new string using a regular expression"
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Replace;

#[typetag::serde]
impl ScalarUDF for Replace {
    fn name(&self) -> &'static str {
        "replace"
    }
    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        let replacement = inputs.required((2, "replacement"))?;
        series_replace(input, pattern, replacement, false)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        get_return_field_impl(inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Replaces all occurrences of a substring with a new string"
    }
}

#[must_use]
pub fn replace(input: ExprRef, pattern: ExprRef, replacement: ExprRef, regex: bool) -> ExprRef {
    let inputs = vec![input, pattern, replacement];

    if regex {
        ScalarFn::builtin(RegexpReplace, inputs).into()
    } else {
        ScalarFn::builtin(Replace, inputs).into()
    }
}

fn get_return_field_impl(inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
    ensure!(inputs.len() == 3, "Replace expects 3 arguments");
    let input = inputs.required((0, "input"))?.to_field(schema)?;
    let pattern = inputs.required((1, "pattern"))?.to_field(schema)?;
    let replacement = inputs.required((2, "replacement"))?.to_field(schema)?;
    ensure!(
        input.dtype.is_string(), TypeError: "Input must be of type Utf8"
    );
    ensure!(
        pattern.dtype.is_string(), TypeError: "Pattern must be of type Utf8"
    );
    ensure!(
        replacement.dtype.is_string(), TypeError: "Replacement must be of type Utf8"
    );

    Ok(input)
}

fn series_replace(
    s: &Series,
    pattern: &Series,
    replacement: &Series,
    regex: bool,
) -> DaftResult<Series> {
    s.with_utf8_array(|arr| {
        pattern.with_utf8_array(|pattern_arr| {
            replacement.with_utf8_array(|replacement_arr| {
                Ok(replace_impl(arr, pattern_arr, replacement_arr, regex)?.into_series())
            })
        })
    })
}

fn replace_impl(
    arr: &Utf8Array,
    pattern: &Utf8Array,
    replacement: &Utf8Array,
    regex: bool,
) -> DaftResult<Utf8Array> {
    let (is_full_null, expected_size) = parse_inputs(arr, &[pattern, replacement])
        .map_err(|e| DaftError::ValueError(format!("Error in replace: {e}")))?;
    if is_full_null {
        return Ok(Utf8Array::full_null(
            arr.name(),
            &DataType::Utf8,
            expected_size,
        ));
    }
    if expected_size == 0 {
        return Ok(Utf8Array::empty(arr.name(), &DataType::Utf8));
    }

    let arr_iter = create_broadcasted_str_iter(arr, expected_size);
    let replacement_iter = create_broadcasted_str_iter(replacement, expected_size);

    let result = match (regex, pattern.len()) {
        (true, 1) => {
            let regex_val = regex::Regex::new(pattern.get(0).unwrap());
            let regex = regex_val.as_ref().map_err(|e| e.clone());
            let regex_iter = std::iter::repeat_n(Some(regex), expected_size);
            regex_replace(arr_iter, regex_iter, replacement_iter, arr.name())?
        }
        (true, _) => {
            let regex_iter = pattern.into_iter().map(|pat| pat.map(regex::Regex::new));
            regex_replace(arr_iter, regex_iter, replacement_iter, arr.name())?
        }
        (false, _) => {
            let pattern_iter = create_broadcasted_str_iter(pattern, expected_size);
            replace_on_literal(arr_iter, pattern_iter, replacement_iter, arr.name())?
        }
    };
    assert_eq!(result.len(), expected_size);
    Ok(result)
}

/// Translates POSIX-style capture-group references (`\1`, `\12`, ...) into the
/// `regex` crate's `${1}` syntax so both `\n` and `$n`/`${n}` work as group
/// references in `regexp_replace`.
///
/// Rules (single left-to-right pass):
///
/// * `\\` (two backslashes) is an escaped backslash and becomes one `\`.
///   This takes precedence so `\\1` stays a literal `\1` instead of being
///   misread as a group reference starting at the second backslash.
/// * `\` followed by one or more ASCII digits is a group reference (`\1` ->
///   `${1}`, `\12` -> `${12}`, `\0` -> `${0}`).
/// * A `\` followed by anything else (or a trailing `\`) is preserved
///   literally, so e.g. `a\b` stays `a\b`. The `regex` crate treats
///   backslashes in replacements literally, so this yields a literal backslash
///   in the output instead of silently dropping it.
/// * Everything else, including `$`-style references (`$1`, `${1}`, `$$`),
///   passes through untouched so `$` keeps exactly the `regex` crate's semantics.
///
/// Returns a borrowed `str` when no backslash is present to avoid allocating
/// in the common case.
fn regex_replace_posix_groups(replacement: &str) -> Cow<'_, str> {
    if !replacement.contains('\\') {
        return Cow::Borrowed(replacement);
    }
    let mut translated = String::with_capacity(replacement.len());
    let mut chars = replacement.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            translated.push(c);
            continue;
        }
        match chars.peek() {
            // Escaped backslash: `\\` -> `\`.
            Some('\\') => {
                translated.push('\\');
                chars.next();
            }
            // POSIX group reference: `\` + digits -> `${digits}`.
            Some(d) if d.is_ascii_digit() => {
                translated.push_str("${");
                while let Some(d) = chars.peek() {
                    if d.is_ascii_digit() {
                        translated.push(*d);
                        chars.next();
                    } else {
                        break;
                    }
                }
                translated.push('}');
            }
            // Lone backslash: preserve literally.
            _ => translated.push('\\'),
        }
    }
    Cow::Owned(translated)
}

fn regex_replace<'a, R: Borrow<regex::Regex>>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<R, regex::Error>>>,
    replacement_iter: impl Iterator<Item = Option<&'a str>>,
    name: &str,
) -> DaftResult<Utf8Array> {
    let result = arr_iter
        .zip(regex_iter)
        .zip(replacement_iter)
        .map(|((val, re), replacement)| match (val, re, replacement) {
            (Some(val), Some(re), Some(replacement)) => {
                let replacement = regex_replace_posix_groups(replacement);
                Ok(Some(re?.borrow().replace_all(val, replacement.as_ref())))
            }
            _ => Ok(None),
        })
        .collect::<DaftResult<Utf8Array>>()?;

    Ok(result.rename(name))
}
fn replace_on_literal<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    pattern_iter: impl Iterator<Item = Option<&'a str>>,
    replacement_iter: impl Iterator<Item = Option<&'a str>>,
    name: &str,
) -> DaftResult<Utf8Array> {
    let result = arr_iter
        .zip(pattern_iter)
        .zip(replacement_iter)
        .map(|((val, pat), replacement)| match (val, pat, replacement) {
            (Some(val), Some(pat), Some(replacement)) => Ok(Some(val.replace(pat, replacement))),
            _ => Ok(None),
        })
        .collect::<DaftResult<Utf8Array>>()?;

    Ok(result.rename(name))
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::Utf8Array;

    use super::*;

    #[test]
    fn test_replace_literal_with_values() {
        let arr = Utf8Array::from_iter(
            "a",
            vec![Some("hello world"), Some("hello hello"), Some("world")].into_iter(),
        );
        let pattern = Utf8Array::from_iter("p", vec![Some("hello")].into_iter());
        let replacement = Utf8Array::from_iter("r", vec![Some("hi")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, false).unwrap();

        assert_eq!(result.get(0), Some("hi world"));
        assert_eq!(result.get(1), Some("hi hi"));
        assert_eq!(result.get(2), Some("world"));
    }

    #[test]
    fn test_replace_literal_with_nulls() {
        let arr = Utf8Array::from_iter(
            "a",
            vec![Some("hello world"), None, Some("world")].into_iter(),
        );
        let pattern = Utf8Array::from_iter("p", vec![Some("world")].into_iter());
        let replacement = Utf8Array::from_iter("r", vec![Some("earth")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, false).unwrap();

        assert_eq!(result.get(0), Some("hello earth"));
        assert!(result.get(1).is_none());
        assert_eq!(result.get(2), Some("earth"));
    }

    #[test]
    fn test_regexp_replace_with_values() {
        let arr = Utf8Array::from_iter(
            "a",
            vec![Some("hello123world"), Some("abc456def"), Some("nodigits")].into_iter(),
        );
        let pattern = Utf8Array::from_iter("p", vec![Some(r"\d+")].into_iter());
        let replacement = Utf8Array::from_iter("r", vec![Some("NUM")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, true).unwrap();

        assert_eq!(result.get(0), Some("helloNUMworld"));
        assert_eq!(result.get(1), Some("abcNUMdef"));
        assert_eq!(result.get(2), Some("nodigits"));
    }

    #[test]
    fn test_regexp_replace_with_nulls() {
        let arr = Utf8Array::from_iter(
            "a",
            vec![Some("hello123"), None, Some("world456")].into_iter(),
        );
        let pattern = Utf8Array::from_iter("p", vec![Some(r"\d+")].into_iter());
        let replacement = Utf8Array::from_iter("r", vec![Some("")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, true).unwrap();

        assert_eq!(result.get(0), Some("hello"));
        assert!(result.get(1).is_none());
        assert_eq!(result.get(2), Some("world"));
    }

    #[test]
    fn test_replace_broadcast_pattern() {
        let arr =
            Utf8Array::from_iter("a", vec![Some("aaa"), Some("bbb"), Some("ccc")].into_iter());
        let pattern = Utf8Array::from_iter("p", vec![Some("a"), Some("b"), Some("c")].into_iter());
        let replacement = Utf8Array::from_iter("r", vec![Some("X")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, false).unwrap();

        assert_eq!(result.get(0), Some("XXX"));
        assert_eq!(result.get(1), Some("XXX"));
        assert_eq!(result.get(2), Some("XXX"));
    }

    #[test]
    fn test_regexp_replace_with_capture_groups() {
        let arr = Utf8Array::from_iter("a", vec![Some("hello world")].into_iter());
        let pattern = Utf8Array::from_iter("p", vec![Some(r"(\w+) (\w+)")].into_iter());
        // Using POSIX-style capture group replacement
        let replacement = Utf8Array::from_iter("r", vec![Some(r"\2 \1")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, true).unwrap();

        assert_eq!(result.get(0), Some("world hello"));
    }

    #[test]
    fn test_regex_replace_posix_groups_translation() {

        // No backslash: borrowed, untouched (also covers `$`-only replacements).
        let translated = regex_replace_posix_groups("[$1]");
        assert!(matches!(translated, Cow::Borrowed(_)));
        assert_eq!(translated, "[$1]");

        // `\n` becomes `${n}`; `$n` passes through for the regex crate.
        assert_eq!(regex_replace_posix_groups("[\\1]"), "[${1}]");
        assert_eq!(regex_replace_posix_groups("\\12"), "${12}");
        assert_eq!(regex_replace_posix_groups("\\0"), "${0}");
        assert_eq!(regex_replace_posix_groups("$1"), "$1");
        assert_eq!(regex_replace_posix_groups("$$"), "$$");

        // `\\` is an escaped backslash and collapses to one.
        assert_eq!(regex_replace_posix_groups("\\\\"), "\\");
        // Escaped backslash wins over group parsing: `\\1` is literal `\1`.
        assert_eq!(regex_replace_posix_groups("\\\\1"), "\\1");
        // `\\` + `\1` is a literal backslash followed by a group reference.
        assert_eq!(regex_replace_posix_groups("\\\\\\1"), "\\${1}");

        // Lone backslashes are preserved literally.
        assert_eq!(regex_replace_posix_groups("a\\b"), "a\\b");
        assert_eq!(regex_replace_posix_groups("x\\"), "x\\");
        assert_eq!(regex_replace_posix_groups("\\"), "\\");
        assert_eq!(regex_replace_posix_groups("\\$1"), "\\$1");
    }

    #[test]
    fn test_regexp_replace_preserves_backslashes() {
        // Regression test for https://github.com/Eventual-Inc/Daft/issues/7471:
        // backslashes in the replacement must not be silently dropped.
        let cases = vec![
            // (replacement, expected output for input "abc" with pattern "(b)")
            ("[$1]".to_string(), "a[b]c".to_string()),
            ("[\\1]".to_string(), "a[b]c".to_string()),
            ("a\\b".to_string(), "aa\\bc".to_string()),
            // `\\` (two backslashes) is an escaped backslash -> one backslash.
            ("\\\\".to_string(), "a\\c".to_string()),
            ("x\\".to_string(), "ax\\c".to_string()),
            ("\\1".to_string(), "abc".to_string()),
            // Escaped backslash + literal `1`, not a group reference.
            ("\\\\1".to_string(), "a\\1c".to_string()),
            // Escaped backslash + group reference.
            ("\\\\\\1".to_string(), "a\\bc".to_string()),
            ("$1".to_string(), "abc".to_string()),
            ("$$".to_string(), "a$c".to_string()),
        ];
        for (replacement, expected) in cases {
            let arr = Utf8Array::from_iter("a", vec![Some("abc")].into_iter());
            let pattern = Utf8Array::from_iter("p", vec![Some("(b)")].into_iter());
            let replacement_arr =
                Utf8Array::from_iter("r", vec![Some(replacement.as_str())].into_iter());
            let result = replace_impl(&arr, &pattern, &replacement_arr, true).unwrap();
            assert_eq!(
                result.get(0),
                Some(expected.as_str()),
                "replacement {replacement:?}"
            );
        }
    }
}
