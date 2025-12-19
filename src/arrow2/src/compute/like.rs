//! Contains "like" operators such as [`like_utf8`] and [`like_utf8_scalar`].

use ahash::AHashMap;
use common_pattern::like_pattern_to_regex;
use regex::{bytes::Regex as BytesRegex, Regex};

use crate::{
    array::{BinaryArray, BooleanArray, Utf8Array},
    bitmap::Bitmap,
    compute::utils::combine_validities,
    datatypes::DataType,
    error::{Error, Result},
    offset::Offset,
};

#[inline]
fn is_like_pattern(c: char) -> bool {
    c == '%' || c == '_'
}

fn compile_like_regex(pattern: &str) -> Result<Regex> {
    let re_pattern = like_pattern_to_regex(pattern).ok_or_else(|| {
        Error::InvalidArgumentError("Unable to build regex from LIKE pattern".to_string())
    })?;
    Regex::new(&re_pattern).map_err(|e| {
        Error::InvalidArgumentError(format!("Unable to build regex from LIKE pattern: {e}"))
    })
}

fn compile_like_bytes_regex(pattern: &str) -> Result<BytesRegex> {
    let re_pattern = like_pattern_to_regex(pattern).ok_or_else(|| {
        Error::InvalidArgumentError("Unable to build regex from LIKE pattern".to_string())
    })?;
    BytesRegex::new(&re_pattern).map_err(|e| {
        Error::InvalidArgumentError(format!("Unable to build regex from LIKE pattern: {e}"))
    })
}

#[inline]
fn a_like_utf8<O: Offset, F: Fn(bool) -> bool>(
    lhs: &Utf8Array<O>,
    rhs: &Utf8Array<O>,
    op: F,
) -> Result<BooleanArray> {
    if lhs.len() != rhs.len() {
        return Err(Error::InvalidArgumentError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let mut map = AHashMap::new();

    let values =
        Bitmap::try_from_trusted_len_iter(lhs.iter().zip(rhs.iter()).map(|(lhs, rhs)| {
            match (lhs, rhs) {
                (Some(lhs), Some(pattern)) => {
                    let regex = if let Some(regex) = map.get(pattern) {
                        regex
                    } else {
                        let re = compile_like_regex(pattern)?;
                        map.insert(pattern, re);
                        map.get(pattern).unwrap()
                    };
                    Result::Ok(op(regex.is_match(lhs)))
                }
                _ => Ok(false),
            }
        }))?;

    Ok(BooleanArray::new(DataType::Boolean, values, validity))
}

/// Returns `lhs LIKE rhs` operation on two [`Utf8Array`].
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
///
/// # Error
/// Errors iff:
/// * the arrays have a different length
/// * any of the patterns is not valid
/// # Example
/// ```
/// use arrow2::array::{Utf8Array, BooleanArray};
/// use arrow2::compute::like::like_utf8;
///
/// let strings = Utf8Array::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "Arrow", "Ar"]);
/// let patterns = Utf8Array::<i32>::from_slice(&["A%", "B%", "%r_ow", "A_", "A_"]);
///
/// let result = like_utf8(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from_slice(&[true, false, true, false, true]));
/// ```
pub fn like_utf8<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    a_like_utf8(lhs, rhs, |x| x)
}

/// Returns `lhs NOT LIKE rhs` operation on two [`Utf8Array`].
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
pub fn nlike_utf8<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    a_like_utf8(lhs, rhs, |x| !x)
}

fn a_like_utf8_scalar<O: Offset, F: Fn(bool) -> bool>(
    lhs: &Utf8Array<O>,
    rhs: &str,
    op: F,
) -> Result<BooleanArray> {
    let validity = lhs.validity();

    let values = if !rhs.contains(is_like_pattern) {
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x == rhs)))
    } else if rhs.ends_with('%')
        && !rhs.ends_with("\\%")
        && !rhs[..rhs.len() - 1].contains(is_like_pattern)
    {
        // fast path, can use starts_with
        let starts_with = &rhs[..rhs.len() - 1];
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x.starts_with(starts_with))))
    } else if rhs.starts_with('%') && !rhs[1..].contains(is_like_pattern) {
        // fast path, can use ends_with
        let ends_with = &rhs[1..];
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x.ends_with(ends_with))))
    } else if rhs.starts_with('%')
        && rhs.ends_with('%')
        && !rhs.ends_with("\\%")
        && !rhs[1..rhs.len() - 1].contains(is_like_pattern)
    {
        let needle = &rhs[1..rhs.len() - 1];
        let finder = memchr::memmem::Finder::new(needle);
        Bitmap::from_trusted_len_iter(
            lhs.values_iter()
                .map(|x| op(finder.find(x.as_bytes()).is_some())),
        )
    } else {
        let re = compile_like_regex(rhs)?;
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(re.is_match(x))))
    };
    Ok(BooleanArray::new(
        DataType::Boolean,
        values,
        validity.cloned(),
    ))
}

/// Returns `lhs LIKE rhs` operation.
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
///
/// # Error
/// Errors iff:
/// * the arrays have a different length
/// * any of the patterns is not valid
/// # Example
/// ```
/// use arrow2::array::{Utf8Array, BooleanArray};
/// use arrow2::compute::like::like_utf8_scalar;
///
/// let array = Utf8Array::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "BA"]);
///
/// let result = like_utf8_scalar(&array, &"A%").unwrap();
/// assert_eq!(result, BooleanArray::from_slice(&[true, true, true, false]));
/// ```
pub fn like_utf8_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> Result<BooleanArray> {
    a_like_utf8_scalar(lhs, rhs, |x| x)
}

/// Returns `lhs NOT LIKE rhs` operation.
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
pub fn nlike_utf8_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> Result<BooleanArray> {
    a_like_utf8_scalar(lhs, rhs, |x| !x)
}

#[inline]
fn a_like_binary<O: Offset, F: Fn(bool) -> bool>(
    lhs: &BinaryArray<O>,
    rhs: &BinaryArray<O>,
    op: F,
) -> Result<BooleanArray> {
    if lhs.len() != rhs.len() {
        return Err(Error::InvalidArgumentError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let mut map = AHashMap::new();

    let values =
        Bitmap::try_from_trusted_len_iter(lhs.iter().zip(rhs.iter()).map(|(lhs, rhs)| {
            match (lhs, rhs) {
                (Some(lhs), Some(pattern)) => {
                    let regex = if let Some(regex) = map.get(pattern) {
                        regex
                    } else {
                        let pattern_str = simdutf8::basic::from_utf8(pattern).unwrap();
                        let re = compile_like_bytes_regex(pattern_str)?;
                        map.insert(pattern, re);
                        map.get(pattern).unwrap()
                    };
                    Result::Ok(op(regex.is_match(lhs)))
                }
                _ => Ok(false),
            }
        }))?;

    Ok(BooleanArray::new(DataType::Boolean, values, validity))
}

/// Returns `lhs LIKE rhs` operation on two [`BinaryArray`].
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
///
/// # Error
/// Errors iff:
/// * the arrays have a different length
/// * any of the patterns is not valid
/// # Example
/// ```
/// use arrow2::array::{BinaryArray, BooleanArray};
/// use arrow2::compute::like::like_binary;
///
/// let strings = BinaryArray::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "Arrow", "Ar"]);
/// let patterns = BinaryArray::<i32>::from_slice(&["A%", "B%", "%r_ow", "A_", "A_"]);
///
/// let result = like_binary(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from_slice(&[true, false, true, false, true]));
/// ```
pub fn like_binary<O: Offset>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>) -> Result<BooleanArray> {
    a_like_binary(lhs, rhs, |x| x)
}

/// Returns `lhs NOT LIKE rhs` operation on two [`BinaryArray`]s.
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
///
pub fn nlike_binary<O: Offset>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>) -> Result<BooleanArray> {
    a_like_binary(lhs, rhs, |x| !x)
}

fn a_like_binary_scalar<O: Offset, F: Fn(bool) -> bool>(
    lhs: &BinaryArray<O>,
    rhs: &[u8],
    op: F,
) -> Result<BooleanArray> {
    let validity = lhs.validity();
    let pattern = simdutf8::basic::from_utf8(rhs).map_err(|e| {
        Error::InvalidArgumentError(format!("Unable to convert the LIKE pattern to string: {e}"))
    })?;

    let values = if !pattern.contains(is_like_pattern) {
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x == rhs)))
    } else if pattern.ends_with('%')
        && !pattern.ends_with("\\%")
        && !pattern[..pattern.len() - 1].contains(is_like_pattern)
    {
        // fast path, can use starts_with
        let starts_with = &rhs[..rhs.len() - 1];
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x.starts_with(starts_with))))
    } else if pattern.starts_with('%') && !pattern[1..].contains(is_like_pattern) {
        // fast path, can use ends_with
        let ends_with = &rhs[1..];
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x.ends_with(ends_with))))
    } else {
        let re = compile_like_bytes_regex(pattern)?;
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(re.is_match(x))))
    };
    Ok(BooleanArray::new(
        DataType::Boolean,
        values,
        validity.cloned(),
    ))
}

/// Returns `lhs LIKE rhs` operation.
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
///
/// # Error
/// Errors iff:
/// * the arrays have a different length
/// * any of the patterns is not valid
/// # Example
/// ```
/// use arrow2::array::{BinaryArray, BooleanArray};
/// use arrow2::compute::like::like_binary_scalar;
///
/// let array = BinaryArray::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "BA"]);
///
/// let result = like_binary_scalar(&array, b"A%").unwrap();
/// assert_eq!(result, BooleanArray::from_slice(&[true, true, true, false]));
/// ```
pub fn like_binary_scalar<O: Offset>(lhs: &BinaryArray<O>, rhs: &[u8]) -> Result<BooleanArray> {
    a_like_binary_scalar(lhs, rhs, |x| x)
}

/// Returns `lhs NOT LIKE rhs` operation on two [`BinaryArray`]s.
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
///
pub fn nlike_binary_scalar<O: Offset>(lhs: &BinaryArray<O>, rhs: &[u8]) -> Result<BooleanArray> {
    a_like_binary_scalar(lhs, rhs, |x| !x)
}
