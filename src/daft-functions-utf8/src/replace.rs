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

    // A scalar replacement is broadcast to every row, so translate its template
    // once here instead of re-scanning the same string inside the row loop.
    // A genuinely element-wise replacement column is translated per row.
    let broadcast_template = if replacement.len() == 1 {
        replacement.get(0).map(regex_replace_posix_groups)
    } else {
        None
    };

    let result = match (regex, pattern.len()) {
        (true, 1) => {
            let regex_val = regex::Regex::new(pattern.get(0).unwrap());
            let regex = regex_val.as_ref().map_err(|e| e.clone());
            let regex_iter = std::iter::repeat_n(Some(regex), expected_size);
            let templates = translated_replacement_iter(
                replacement,
                broadcast_template.as_deref(),
                expected_size,
            );
            regex_replace(arr_iter, regex_iter, templates, arr.name())?
        }
        (true, _) => {
            let regex_iter = pattern.into_iter().map(|pat| pat.map(regex::Regex::new));
            let templates = translated_replacement_iter(
                replacement,
                broadcast_template.as_deref(),
                expected_size,
            );
            regex_replace(arr_iter, regex_iter, templates, arr.name())?
        }
        (false, _) => {
            let pattern_iter = create_broadcasted_str_iter(pattern, expected_size);
            replace_on_literal(arr_iter, pattern_iter, replacement_iter, arr.name())?
        }
    };
    assert_eq!(result.len(), expected_size);
    Ok(result)
}

/// Length in bytes of the `$` construct at the start of `s`, if the `regex`
/// crate would read a capture reference (or a `$$` escape) there.
///
/// This mirrors `regex`'s own interpolation rules so that we can copy such
/// constructs through untouched:
///
/// * `$$` is the escape for a literal `$`.
/// * `${name}` is a braced reference; the name is everything up to the first
///   `}`, and an unclosed `${` is not a reference at all.
/// * `$name` is an unbraced reference whose name is the longest run of
///   `[0-9A-Za-z_]`; a `$` followed by none of those is not a reference.
///
/// Returns `None` when the `$` is just a literal dollar sign (a trailing `$`,
/// a `$` before a non-name character, or an unclosed `${`).
fn dollar_construct_len(s: &str) -> Option<usize> {
    debug_assert!(s.starts_with('$'));
    let bytes = s.as_bytes();
    match bytes.get(1)? {
        b'$' => Some(2),
        b'{' => Some(2 + s[2..].find('}')? + 1),
        _ => {
            let name_len = bytes[1..]
                .iter()
                .take_while(|&&b| b.is_ascii_alphanumeric() || b == b'_')
                .count();
            (name_len > 0).then_some(1 + name_len)
        }
    }
}

/// Translates a `regexp_replace` replacement template into the syntax the
/// `regex` crate expects, so that POSIX-style `\1` references, `$`-style
/// references and literal backslashes can all coexist.
///
/// Rules (single left-to-right pass):
///
/// * `\\` (two backslashes) is an escaped backslash and becomes one `\`.
///   This takes precedence so `\\1` stays a literal `\1` instead of being
///   misread as a group reference starting at the second backslash.
/// * `\` followed by a single ASCII digit is a group reference (`\1` ->
///   `${1}`, `\0` -> `${0}` for the whole match). Only one digit is consumed,
///   matching POSIX/sed/Java: `\10` is group 1 followed by a literal `0`, not
///   the usually-nonexistent group 10. Groups past 9 are reachable as `${10}`.
/// * A `\` followed by anything else (or a trailing `\`) is preserved
///   literally, so e.g. `a\b` stays `a\b`. The `regex` crate treats
///   backslashes in replacements literally, so this yields a literal backslash
///   in the output instead of silently dropping it.
/// * `$$` and valid `$`-style references (`$1`, `$name`, `${10}`) are the
///   `regex` crate's own syntax and are copied through untouched.
/// * Any other `$` is a literal dollar sign and is escaped to `$$`. This keeps
///   its meaning unchanged on its own, and stops it from merging with a `${`
///   emitted for a following POSIX reference: without it `$\1` would produce
///   the template `$${1}`, which the crate reads as an escaped `$` followed by
///   the literal text `{1}`.
///
/// Returns a borrowed `str` when there is nothing to translate, to avoid
/// allocating in the common case.
fn regex_replace_posix_groups(replacement: &str) -> Cow<'_, str> {
    if !replacement.contains(['\\', '$']) {
        return Cow::Borrowed(replacement);
    }
    let mut translated = String::with_capacity(replacement.len());
    let mut rest = replacement;
    while let Some(c) = rest.chars().next() {
        match c {
            '\\' => {
                let after = &rest[1..];
                match after.as_bytes().first() {
                    // Escaped backslash: `\\` -> `\`.
                    Some(b'\\') => {
                        translated.push('\\');
                        rest = &after[1..];
                    }
                    // POSIX group reference: `\` + one digit -> `${digit}`.
                    Some(&d) if d.is_ascii_digit() => {
                        translated.push_str("${");
                        translated.push(d as char);
                        translated.push('}');
                        rest = &after[1..];
                    }
                    // Lone or trailing backslash: preserve literally.
                    _ => {
                        translated.push('\\');
                        rest = after;
                    }
                }
            }
            '$' => match dollar_construct_len(rest) {
                // The regex crate's own syntax: copy it through untouched.
                Some(len) => {
                    translated.push_str(&rest[..len]);
                    rest = &rest[len..];
                }
                // A literal `$`: escape it so it stays literal and cannot
                // merge with a `${` we emit for a following `\1`.
                None => {
                    translated.push_str("$$");
                    rest = &rest[1..];
                }
            },
            _ => {
                translated.push(c);
                rest = &rest[c.len_utf8()..];
            }
        }
    }
    Cow::Owned(translated)
}

/// Per-row iterator of replacement templates, already translated into the
/// `regex` crate's syntax.
///
/// When `broadcast_template` is set the same pre-translated template is simply
/// borrowed for every row; otherwise each row's replacement is translated as it
/// is consumed.
fn translated_replacement_iter<'a>(
    replacement: &'a Utf8Array,
    broadcast_template: Option<&'a str>,
    expected_size: usize,
) -> Box<dyn Iterator<Item = Option<Cow<'a, str>>> + 'a> {
    match broadcast_template {
        Some(template) => Box::new(std::iter::repeat_n(
            Some(Cow::Borrowed(template)),
            expected_size,
        )),
        None => Box::new(
            create_broadcasted_str_iter(replacement, expected_size)
                .map(|r| r.map(regex_replace_posix_groups)),
        ),
    }
}

fn regex_replace<'a, R: Borrow<regex::Regex>>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<R, regex::Error>>>,
    replacement_iter: impl Iterator<Item = Option<Cow<'a, str>>>,
    name: &str,
) -> DaftResult<Utf8Array> {
    let result = arr_iter
        .zip(regex_iter)
        .zip(replacement_iter)
        .map(|((val, re), replacement)| match (val, re, replacement) {
            (Some(val), Some(re), Some(replacement)) => {
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
        // Neither `\` nor `$`: borrowed, untouched.
        let translated = regex_replace_posix_groups("[hi]");
        assert!(matches!(translated, Cow::Borrowed(_)));
        assert_eq!(translated, "[hi]");

        // `\n` becomes `${n}`; `$n` passes through for the regex crate.
        assert_eq!(regex_replace_posix_groups("[$1]"), "[$1]");
        assert_eq!(regex_replace_posix_groups("[\\1]"), "[${1}]");
        assert_eq!(regex_replace_posix_groups("\\0"), "${0}");
        assert_eq!(regex_replace_posix_groups("$1"), "$1");
        assert_eq!(regex_replace_posix_groups("$$"), "$$");
        assert_eq!(regex_replace_posix_groups("${10}"), "${10}");
        assert_eq!(regex_replace_posix_groups("$name"), "$name");

        // Only one digit is consumed, so `\12` is group 1 then a literal `2`
        // (POSIX/sed/Java semantics). Groups past 9 are written `${10}`.
        assert_eq!(regex_replace_posix_groups("\\12"), "${1}2");
        assert_eq!(regex_replace_posix_groups("\\10"), "${1}0");

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

        // A `$` the regex crate would not read as a reference is escaped to
        // `$$` so it stays literal and cannot merge with a following `${`.
        assert_eq!(regex_replace_posix_groups("$"), "$$");
        assert_eq!(regex_replace_posix_groups("a$"), "a$$");
        assert_eq!(regex_replace_posix_groups("$ "), "$$ ");
        assert_eq!(regex_replace_posix_groups("${"), "$${");
        assert_eq!(regex_replace_posix_groups("$\\1"), "$$${1}");
    }

    #[test]
    fn test_regexp_replace_literal_dollar_before_posix_group() {
        // A literal `$` directly in front of a POSIX group reference must
        // survive. Emitting a bare `${1}` for `\1` would produce the template
        // `$${1}`, which the regex crate reads as an escaped `$` followed by
        // the literal text `{1}`, yielding "a${1}c".
        let arr = Utf8Array::from_iter("a", vec![Some("abc")].into_iter());
        let pattern = Utf8Array::from_iter("p", vec![Some("(b)")].into_iter());
        let replacement = Utf8Array::from_iter("r", vec![Some("$\\1")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, true).unwrap();

        assert_eq!(result.get(0), Some("a$bc"));
    }

    #[test]
    fn test_regexp_replace_multi_digit_group_reference() {
        // `\10` is group 1 followed by a literal `0`, as in POSIX/sed/Java.
        // Consuming both digits would ask for the nonexistent group 10, which
        // the regex crate expands to the empty string, silently dropping the
        // `0` as well.
        let arr = Utf8Array::from_iter("a", vec![Some("abc")].into_iter());
        let pattern = Utf8Array::from_iter("p", vec![Some("(b)")].into_iter());
        let replacement = Utf8Array::from_iter("r", vec![Some("\\10")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, true).unwrap();

        assert_eq!(result.get(0), Some("ab0c"));
    }

    #[test]
    fn test_regexp_replace_high_group_reference_via_braces() {
        // Groups past 9 stay reachable through the regex crate's own `${n}`.
        let arr = Utf8Array::from_iter("a", vec![Some("abcdefghij")].into_iter());
        let pattern = Utf8Array::from_iter(
            "p",
            vec![Some("(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)")].into_iter(),
        );
        let replacement = Utf8Array::from_iter("r", vec![Some("${10}")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, true).unwrap();

        assert_eq!(result.get(0), Some("j"));
    }

    #[test]
    fn test_replace_literal_does_not_translate_backslashes() {
        // `replace()` (regex = false) has no template semantics: every
        // character of the replacement is emitted verbatim.
        let arr = Utf8Array::from_iter("a", vec![Some("abc")].into_iter());
        let pattern = Utf8Array::from_iter("p", vec![Some("b")].into_iter());
        let replacement = Utf8Array::from_iter("r", vec![Some("\\1$1\\\\")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, false).unwrap();

        assert_eq!(result.get(0), Some("a\\1$1\\\\c"));
    }

    #[test]
    fn test_regexp_replace_unicode_replacement_with_backslash() {
        // Multi-byte characters must survive the scan intact.
        let arr = Utf8Array::from_iter("a", vec![Some("abc")].into_iter());
        let pattern = Utf8Array::from_iter("p", vec![Some("(b)")].into_iter());
        let replacement = Utf8Array::from_iter("r", vec![Some("é\\ü\\1")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, true).unwrap();

        assert_eq!(result.get(0), Some("aé\\übc"));
    }

    #[test]
    fn test_regexp_replace_elementwise_replacement_column() {
        // The per-row path (replacement column longer than one) must translate
        // each row's own template, not reuse a broadcast one.
        let arr = Utf8Array::from_iter("a", vec![Some("abc"), Some("abc")].into_iter());
        let pattern = Utf8Array::from_iter("p", vec![Some("(b)")].into_iter());
        let replacement = Utf8Array::from_iter("r", vec![Some("[\\1]"), Some("x\\")].into_iter());

        let result = replace_impl(&arr, &pattern, &replacement, true).unwrap();

        assert_eq!(result.get(0), Some("a[b]c"));
        assert_eq!(result.get(1), Some("ax\\c"));
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
