use std::{
    borrow::Cow,
    iter::{Repeat, Take},
};

use crate::{
    array::{DataArray, ListArray},
    datatypes::{
        BooleanArray, DaftIntegerType, DaftNumericType, DaftPhysicalType, Field, Int64Array,
        UInt64Array, Utf8Array,
    },
    DataType, Series,
};
use arrow2::array::Array;

use common_error::{DaftError, DaftResult};
use itertools::Itertools;
use num_traits::NumCast;

use super::{as_arrow::AsArrow, full::FullNull};

enum BroadcastedStrIter<'a> {
    Repeat(std::iter::Take<std::iter::Repeat<Option<&'a str>>>),
    NonRepeat(
        arrow2::bitmap::utils::ZipValidity<
            &'a str,
            arrow2::array::ArrayValuesIter<'a, arrow2::array::Utf8Array<i64>>,
            arrow2::bitmap::utils::BitmapIter<'a>,
        >,
    ),
}

impl<'a> Iterator for BroadcastedStrIter<'a> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastedStrIter::Repeat(iter) => iter.next(),
            BroadcastedStrIter::NonRepeat(iter) => iter.next(),
        }
    }
}

fn create_broadcasted_str_iter(arr: &Utf8Array, len: usize) -> BroadcastedStrIter<'_> {
    if arr.len() == 1 {
        BroadcastedStrIter::Repeat(std::iter::repeat(arr.get(0)).take(len))
    } else {
        BroadcastedStrIter::NonRepeat(arr.as_arrow().iter())
    }
}

/// Parse inputs for string operations.
/// Returns a tuple of (is_full_null, expected_size).
fn parse_inputs<T>(
    self_arr: &Utf8Array,
    other_arrs: &[&DataArray<T>],
) -> Result<(bool, usize), String>
where
    T: DaftPhysicalType,
{
    let input_length = self_arr.len();
    let other_lengths = other_arrs.iter().map(|arr| arr.len()).collect::<Vec<_>>();

    // Parse the expected `result_len` from the length of the input and other arguments
    let result_len = if input_length == 0 {
        // Empty input: expect empty output
        0
    } else if other_lengths.iter().all(|&x| x == input_length) {
        // All lengths matching: expect non-broadcasted length
        input_length
    } else if let [broadcasted_len] = std::iter::once(&input_length)
        .chain(other_lengths.iter())
        .filter(|&&x| x != 1)
        .sorted()
        .dedup()
        .collect::<Vec<&usize>>()
        .as_slice()
    {
        // All non-unit lengths match: expect broadcast
        **broadcasted_len
    } else {
        let invalid_length_str = itertools::Itertools::join(
            &mut std::iter::once(&input_length)
                .chain(other_lengths.iter())
                .map(|x| x.to_string()),
            ", ",
        );
        return Err(format!("Inputs have invalid lengths: {invalid_length_str}"));
    };

    // check if any array has all nulls
    if other_arrs.iter().any(|arr| arr.null_count() == arr.len())
        || self_arr.null_count() == self_arr.len()
    {
        return Ok((true, result_len));
    }

    Ok((false, result_len))
}

fn split_array_on_literal<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    pattern_iter: impl Iterator<Item = Option<&'a str>>,
    splits: &mut arrow2::array::MutableUtf8Array<i64>,
    offsets: &mut arrow2::offset::Offsets<i64>,
    validity: &mut arrow2::bitmap::MutableBitmap,
) -> DaftResult<()> {
    for (val, pat) in arr_iter.zip(pattern_iter) {
        let mut num_splits = 0i64;
        match (val, pat) {
            (Some(val), Some(pat)) => {
                for split in val.split(pat) {
                    splits.push(Some(split));
                    num_splits += 1;
                }
                validity.push(true);
            }
            (_, _) => {
                validity.push(false);
            }
        }
        offsets.try_push(num_splits)?;
    }
    Ok(())
}

fn split_array_on_regex<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<regex::Regex, regex::Error>>>,
    splits: &mut arrow2::array::MutableUtf8Array<i64>,
    offsets: &mut arrow2::offset::Offsets<i64>,
    validity: &mut arrow2::bitmap::MutableBitmap,
) -> DaftResult<()> {
    for (val, re) in arr_iter.zip(regex_iter) {
        let mut num_splits = 0i64;
        match (val, re) {
            (Some(val), Some(re)) => {
                for split in re?.split(val) {
                    splits.push(Some(split));
                    num_splits += 1;
                }
                validity.push(true);
            }
            (_, _) => {
                validity.push(false);
            }
        }
        offsets.try_push(num_splits)?;
    }
    Ok(())
}

fn regex_extract_first_match<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<regex::Regex, regex::Error>>>,
    index: usize,
    name: &str,
) -> DaftResult<Utf8Array> {
    let arrow_result = arr_iter
        .zip(regex_iter)
        .map(|(val, re)| match (val, re) {
            (Some(val), Some(re)) => {
                // https://docs.rs/regex/latest/regex/struct.Regex.html#method.captures
                // regex::find is faster than regex::captures but only returns the full match, not the capture groups.
                // So, use regex::find if index == 0, otherwise use regex::captures.
                if index == 0 {
                    Ok(re?.find(val).map(|m| m.as_str()))
                } else {
                    Ok(re?
                        .captures(val)
                        .and_then(|captures| captures.get(index))
                        .map(|m| m.as_str()))
                }
            }
            _ => Ok(None),
        })
        .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>();

    Ok(Utf8Array::from((name, Box::new(arrow_result?))))
}

fn regex_extract_all_matches<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<regex::Regex, regex::Error>>>,
    index: usize,
    len: usize,
    name: &str,
) -> DaftResult<ListArray> {
    let mut matches = arrow2::array::MutableUtf8Array::new();
    let mut offsets = arrow2::offset::Offsets::new();
    let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(len);

    for (val, re) in arr_iter.zip(regex_iter) {
        let mut num_matches = 0i64;
        match (val, re) {
            (Some(val), Some(re)) => {
                // https://docs.rs/regex/latest/regex/struct.Regex.html#method.captures_iter
                // regex::find_iter is faster than regex::captures_iter but only returns the full match, not the capture groups.
                // So, use regex::find_iter if index == 0, otherwise use regex::captures.
                if index == 0 {
                    for m in re?.find_iter(val) {
                        matches.push(Some(m.as_str()));
                        num_matches += 1;
                    }
                } else {
                    for captures in re?.captures_iter(val) {
                        if let Some(capture) = captures.get(index) {
                            matches.push(Some(capture.as_str()));
                            num_matches += 1;
                        }
                    }
                }
                validity.push(true);
            }
            (_, _) => {
                validity.push(false);
            }
        }
        offsets.try_push(num_matches)?;
    }

    let matches: arrow2::array::Utf8Array<i64> = matches.into();
    let offsets: arrow2::offset::OffsetsBuffer<i64> = offsets.into();
    let validity: Option<arrow2::bitmap::Bitmap> = match validity.unset_bits() {
        0 => None,
        _ => Some(validity.into()),
    };
    let flat_child = Series::try_from(("matches", matches.to_boxed()))?;

    Ok(ListArray::new(
        Field::new(name, DataType::List(Box::new(DataType::Utf8))),
        flat_child,
        offsets,
        validity,
    ))
}

fn regex_replace<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<regex::Regex, regex::Error>>>,
    replacement_iter: impl Iterator<Item = Option<&'a str>>,
    name: &str,
) -> DaftResult<Utf8Array> {
    let arrow_result = arr_iter
        .zip(regex_iter)
        .zip(replacement_iter)
        .map(|((val, re), replacement)| match (val, re, replacement) {
            (Some(val), Some(re), Some(replacement)) => Ok(Some(re?.replace_all(val, replacement))),
            _ => Ok(None),
        })
        .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>();

    Ok(Utf8Array::from((name, Box::new(arrow_result?))))
}

fn replace_on_literal<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    pattern_iter: impl Iterator<Item = Option<&'a str>>,
    replacement_iter: impl Iterator<Item = Option<&'a str>>,
    name: &str,
) -> DaftResult<Utf8Array> {
    let arrow_result = arr_iter
        .zip(pattern_iter)
        .zip(replacement_iter)
        .map(|((val, pat), replacement)| match (val, pat, replacement) {
            (Some(val), Some(pat), Some(replacement)) => Ok(Some(val.replace(pat, replacement))),
            _ => Ok(None),
        })
        .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>();

    Ok(Utf8Array::from((name, Box::new(arrow_result?))))
}

#[derive(Debug, Clone, Copy)]
pub enum PadPlacement {
    Left,
    Right,
}

impl Utf8Array {
    pub fn endswith(&self, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
        self.binary_broadcasted_compare(
            pattern,
            |data: &str, pat: &str| Ok(data.ends_with(pat)),
            "endswith",
        )
    }

    pub fn startswith(&self, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
        self.binary_broadcasted_compare(
            pattern,
            |data: &str, pat: &str| Ok(data.starts_with(pat)),
            "startswith",
        )
    }

    pub fn contains(&self, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
        self.binary_broadcasted_compare(
            pattern,
            |data: &str, pat: &str| Ok(data.contains(pat)),
            "contains",
        )
    }

    pub fn match_(&self, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
        if pattern.len() == 1 {
            let pattern_scalar_value = pattern.get(0);
            return match pattern_scalar_value {
                None => Ok(BooleanArray::full_null(
                    self.name(),
                    &DataType::Boolean,
                    self.len(),
                )),
                Some(pattern_v) => {
                    let re = regex::Regex::new(pattern_v)?;
                    let arrow_result: arrow2::array::BooleanArray = self
                        .as_arrow()
                        .into_iter()
                        .map(|self_v| Some(re.is_match(self_v?)))
                        .collect();
                    Ok(BooleanArray::from((self.name(), arrow_result)))
                }
            };
        }

        self.binary_broadcasted_compare(
            pattern,
            |data: &str, pat: &str| Ok(regex::Regex::new(pat)?.is_match(data)),
            "match",
        )
    }

    pub fn split(&self, pattern: &Utf8Array, regex: bool) -> DaftResult<ListArray> {
        let (is_full_null, expected_size) = parse_inputs(self, &[pattern])
            .map_err(|e| DaftError::ValueError(format!("Error in split: {e}")))?;
        if is_full_null {
            return Ok(ListArray::full_null(
                self.name(),
                &DataType::List(Box::new(DataType::Utf8)),
                expected_size,
            ));
        }
        if expected_size == 0 {
            return Ok(ListArray::empty(
                self.name(),
                &DataType::List(Box::new(DataType::Utf8)),
            ));
        }

        let self_arrow = self.as_arrow();
        let buffer_len = self_arrow.values().len();
        // This will overallocate by pattern_len * N_i, where N_i is the number of pattern occurrences in the ith string in arr_iter.
        let mut splits = arrow2::array::MutableUtf8Array::with_capacity(buffer_len);
        let mut offsets = arrow2::offset::Offsets::new();
        let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(self.len());

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        match (regex, pattern.len()) {
            (true, 1) => {
                let regex = regex::Regex::new(pattern.get(0).unwrap());
                let regex_iter = std::iter::repeat(Some(regex)).take(expected_size);
                split_array_on_regex(
                    self_iter,
                    regex_iter,
                    &mut splits,
                    &mut offsets,
                    &mut validity,
                )?
            }
            (true, _) => {
                let regex_iter = pattern
                    .as_arrow()
                    .iter()
                    .map(|pat| pat.map(regex::Regex::new));
                split_array_on_regex(
                    self_iter,
                    regex_iter,
                    &mut splits,
                    &mut offsets,
                    &mut validity,
                )?
            }
            (false, _) => {
                let pattern_iter = create_broadcasted_str_iter(pattern, expected_size);
                split_array_on_literal(
                    self_iter,
                    pattern_iter,
                    &mut splits,
                    &mut offsets,
                    &mut validity,
                )?
            }
        }
        // Shrink splits capacity to current length, since we will have overallocated if any of the patterns actually occurred in the strings.
        splits.shrink_to_fit();
        let splits: arrow2::array::Utf8Array<i64> = splits.into();
        let offsets: arrow2::offset::OffsetsBuffer<i64> = offsets.into();
        let validity: Option<arrow2::bitmap::Bitmap> = match validity.unset_bits() {
            0 => None,
            _ => Some(validity.into()),
        };
        let flat_child = Series::try_from(("splits", splits.to_boxed()))?;
        let result = ListArray::new(
            Field::new(self.name(), DataType::List(Box::new(DataType::Utf8))),
            flat_child,
            offsets,
            validity,
        );
        assert_eq!(result.len(), expected_size);
        Ok(result)
    }

    pub fn extract(&self, pattern: &Utf8Array, index: usize) -> DaftResult<Utf8Array> {
        let (is_full_null, expected_size) = parse_inputs(self, &[pattern])
            .map_err(|e| DaftError::ValueError(format!("Error in extract: {e}")))?;
        if is_full_null {
            return Ok(Utf8Array::full_null(
                self.name(),
                &DataType::Utf8,
                expected_size,
            ));
        }
        if expected_size == 0 {
            return Ok(Utf8Array::empty(self.name(), &DataType::Utf8));
        }

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        let result = match pattern.len() {
            1 => {
                let regex = regex::Regex::new(pattern.get(0).unwrap());
                let regex_iter = std::iter::repeat(Some(regex)).take(expected_size);
                regex_extract_first_match(self_iter, regex_iter, index, self.name())?
            }
            _ => {
                let regex_iter = pattern
                    .as_arrow()
                    .iter()
                    .map(|pat| pat.map(regex::Regex::new));
                regex_extract_first_match(self_iter, regex_iter, index, self.name())?
            }
        };
        assert_eq!(result.len(), expected_size);
        Ok(result)
    }

    pub fn extract_all(&self, pattern: &Utf8Array, index: usize) -> DaftResult<ListArray> {
        let (is_full_null, expected_size) = parse_inputs(self, &[pattern])
            .map_err(|e| DaftError::ValueError(format!("Error in extract_all: {e}")))?;
        if is_full_null {
            return Ok(ListArray::full_null(
                self.name(),
                &DataType::List(Box::new(DataType::Utf8)),
                expected_size,
            ));
        }
        if expected_size == 0 {
            return Ok(ListArray::empty(
                self.name(),
                &DataType::List(Box::new(DataType::Utf8)),
            ));
        }

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        let result = match pattern.len() {
            1 => {
                let regex = regex::Regex::new(pattern.get(0).unwrap());
                let regex_iter = std::iter::repeat(Some(regex)).take(expected_size);
                regex_extract_all_matches(self_iter, regex_iter, index, expected_size, self.name())?
            }
            _ => {
                let regex_iter = pattern
                    .as_arrow()
                    .iter()
                    .map(|pat| pat.map(regex::Regex::new));
                regex_extract_all_matches(self_iter, regex_iter, index, expected_size, self.name())?
            }
        };
        assert_eq!(result.len(), expected_size);
        Ok(result)
    }

    pub fn replace(
        &self,
        pattern: &Utf8Array,
        replacement: &Utf8Array,
        regex: bool,
    ) -> DaftResult<Utf8Array> {
        let (is_full_null, expected_size) = parse_inputs(self, &[pattern, replacement])
            .map_err(|e| DaftError::ValueError(format!("Error in replace: {e}")))?;
        if is_full_null {
            return Ok(Utf8Array::full_null(
                self.name(),
                &DataType::Utf8,
                expected_size,
            ));
        }
        if expected_size == 0 {
            return Ok(Utf8Array::empty(self.name(), &DataType::Utf8));
        }

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        let replacement_iter = create_broadcasted_str_iter(replacement, expected_size);

        let result = match (regex, pattern.len()) {
            (true, 1) => {
                let regex = regex::Regex::new(pattern.get(0).unwrap());
                let regex_iter = std::iter::repeat(Some(regex)).take(expected_size);
                regex_replace(self_iter, regex_iter, replacement_iter, self.name())?
            }
            (true, _) => {
                let regex_iter = pattern
                    .as_arrow()
                    .iter()
                    .map(|pat| pat.map(regex::Regex::new));
                regex_replace(self_iter, regex_iter, replacement_iter, self.name())?
            }
            (false, _) => {
                let pattern_iter = create_broadcasted_str_iter(pattern, expected_size);
                replace_on_literal(self_iter, pattern_iter, replacement_iter, self.name())?
            }
        };
        assert_eq!(result.len(), expected_size);
        Ok(result)
    }

    pub fn length(&self) -> DaftResult<UInt64Array> {
        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|val| {
                let v = val?;
                Some(v.len() as u64)
            })
            .collect::<arrow2::array::UInt64Array>()
            .with_validity(self_arrow.validity().cloned());
        Ok(UInt64Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn lower(&self) -> DaftResult<Utf8Array> {
        self.unary_broadcasted_op(|val| val.to_lowercase().into())
    }

    pub fn upper(&self) -> DaftResult<Utf8Array> {
        self.unary_broadcasted_op(|val| val.to_uppercase().into())
    }

    pub fn lstrip(&self) -> DaftResult<Utf8Array> {
        self.unary_broadcasted_op(|val| val.trim_start().into())
    }

    pub fn rstrip(&self) -> DaftResult<Utf8Array> {
        self.unary_broadcasted_op(|val| val.trim_end().into())
    }

    pub fn reverse(&self) -> DaftResult<Utf8Array> {
        self.unary_broadcasted_op(|val| val.chars().rev().collect::<String>().into())
    }

    pub fn capitalize(&self) -> DaftResult<Utf8Array> {
        self.unary_broadcasted_op(|val| {
            let mut chars = val.chars();
            match chars.next() {
                None => "".into(),
                Some(first) => {
                    let first_char_uppercased = first.to_uppercase();
                    let mut res = String::with_capacity(val.len());
                    res.extend(first_char_uppercased);
                    res.extend(chars.flat_map(|c| c.to_lowercase()));
                    res.into()
                }
            }
        })
    }

    pub fn find(&self, substr: &Utf8Array) -> DaftResult<Int64Array> {
        let (is_full_null, expected_size) = parse_inputs(self, &[substr])
            .map_err(|e| DaftError::ValueError(format!("Error in find: {e}")))?;
        if is_full_null {
            return Ok(Int64Array::full_null(
                self.name(),
                &DataType::Int64,
                expected_size,
            ));
        }
        if expected_size == 0 {
            return Ok(Int64Array::empty(self.name(), &DataType::Int64));
        }

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        let substr_iter = create_broadcasted_str_iter(substr, expected_size);
        let arrow_result = self_iter
            .zip(substr_iter)
            .map(|(val, substr)| match (val, substr) {
                (Some(val), Some(substr)) => {
                    Some(val.find(substr).map(|pos| pos as i64).unwrap_or(-1))
                }
                _ => None,
            })
            .collect::<arrow2::array::Int64Array>();

        let result = Int64Array::from((self.name(), Box::new(arrow_result)));
        assert_eq!(result.len(), expected_size);
        Ok(result)
    }

    pub fn left<I>(&self, nchars: &DataArray<I>) -> DaftResult<Utf8Array>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord,
    {
        let (is_full_null, expected_size) = parse_inputs(self, &[nchars])
            .map_err(|e| DaftError::ValueError(format!("Error in left: {e}")))?;
        if is_full_null {
            return Ok(Utf8Array::full_null(
                self.name(),
                &DataType::Utf8,
                expected_size,
            ));
        }
        if expected_size == 0 {
            return Ok(Utf8Array::empty(self.name(), &DataType::Utf8));
        }

        fn left_most_chars(val: &str, n: usize) -> &str {
            if n == 0 || val.is_empty() {
                ""
            } else {
                val.char_indices().nth(n).map_or(val, |(i, _)| &val[..i])
            }
        }

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        let result = match nchars.len() {
            1 => {
                let n = nchars.get(0).unwrap();
                let n: usize = NumCast::from(n).ok_or_else(|| {
                    DaftError::ComputeError(format!(
                        "Error in left: failed to cast rhs as usize {n}"
                    ))
                })?;
                let arrow_result = self_iter
                    .map(|val| Some(left_most_chars(val?, n)))
                    .collect::<arrow2::array::Utf8Array<i64>>();
                Utf8Array::from((self.name(), Box::new(arrow_result)))
            }
            _ => {
                let arrow_result = self_iter
                    .zip(nchars.as_arrow().iter())
                    .map(|(val, n)| match (val, n) {
                        (Some(val), Some(nchar)) => {
                            let nchar: usize = NumCast::from(*nchar).ok_or_else(|| {
                                DaftError::ComputeError(format!(
                                    "Error in left: failed to cast rhs as usize {nchar}"
                                ))
                            })?;
                            Ok(Some(left_most_chars(val, nchar)))
                        }
                        _ => Ok(None),
                    })
                    .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

                Utf8Array::from((self.name(), Box::new(arrow_result)))
            }
        };
        assert_eq!(result.len(), expected_size);
        Ok(result)
    }

    pub fn right<I>(&self, nchars: &DataArray<I>) -> DaftResult<Utf8Array>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord,
    {
        let (is_full_null, expected_size) = parse_inputs(self, &[nchars])
            .map_err(|e| DaftError::ValueError(format!("Error in right: {e}")))?;
        if is_full_null {
            return Ok(Utf8Array::full_null(
                self.name(),
                &DataType::Utf8,
                expected_size,
            ));
        }
        if expected_size == 0 {
            return Ok(Utf8Array::empty(self.name(), &DataType::Utf8));
        }

        fn right_most_chars(val: &str, nchar: usize) -> &str {
            if nchar == 0 || val.is_empty() {
                ""
            } else {
                let skip = val.chars().count().saturating_sub(nchar);
                val.char_indices().nth(skip).map_or(val, |(i, _)| &val[i..])
            }
        }

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        let result = match nchars.len() {
            1 => {
                let n = nchars.get(0).unwrap();
                let n: usize = NumCast::from(n).ok_or_else(|| {
                    DaftError::ComputeError(format!(
                        "Error in right: failed to cast rhs as usize {n}"
                    ))
                })?;
                let arrow_result = self_iter
                    .map(|val| Some(right_most_chars(val?, n)))
                    .collect::<arrow2::array::Utf8Array<i64>>();
                Utf8Array::from((self.name(), Box::new(arrow_result)))
            }
            _ => {
                let arrow_result = self_iter
                    .zip(nchars.as_arrow().iter())
                    .map(|(val, n)| match (val, n) {
                        (Some(val), Some(nchar)) => {
                            let nchar: usize = NumCast::from(*nchar).ok_or_else(|| {
                                DaftError::ComputeError(format!(
                                    "Error in right: failed to cast rhs as usize {nchar}"
                                ))
                            })?;
                            Ok(Some(right_most_chars(val, nchar)))
                        }
                        _ => Ok(None),
                    })
                    .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

                Utf8Array::from((self.name(), Box::new(arrow_result)))
            }
        };
        assert_eq!(result.len(), expected_size);
        Ok(result)
    }

    pub fn repeat<I>(&self, n: &DataArray<I>) -> DaftResult<Utf8Array>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord,
    {
        let (is_full_null, expected_size) = parse_inputs(self, &[n])
            .map_err(|e| DaftError::ValueError(format!("Error in repeat: {e}")))?;
        if is_full_null {
            return Ok(Utf8Array::full_null(
                self.name(),
                &DataType::Utf8,
                expected_size,
            ));
        }

        if expected_size == 0 {
            return Ok(Utf8Array::empty(self.name(), &DataType::Utf8));
        }

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        let result = match n.len() {
            1 => {
                let n = n.get(0).unwrap();
                let n: usize = NumCast::from(n).ok_or_else(|| {
                    DaftError::ComputeError(format!(
                        "Error in repeat: failed to cast rhs as usize {n}"
                    ))
                })?;
                let arrow_result = self_iter
                    .map(|val| Some(val?.repeat(n)))
                    .collect::<arrow2::array::Utf8Array<i64>>();
                Utf8Array::from((self.name(), Box::new(arrow_result)))
            }
            _ => {
                let arrow_result = self_iter
                    .zip(n.as_arrow().iter())
                    .map(|(val, n)| match (val, n) {
                        (Some(val), Some(n)) => {
                            let n: usize = NumCast::from(*n).ok_or_else(|| {
                                DaftError::ComputeError(format!(
                                    "Error in repeat: failed to cast rhs as usize {n}"
                                ))
                            })?;
                            Ok(Some(val.repeat(n)))
                        }
                        _ => Ok(None),
                    })
                    .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

                Utf8Array::from((self.name(), Box::new(arrow_result)))
            }
        };

        assert_eq!(result.len(), expected_size);
        Ok(result)
    }

    pub fn pad<I>(
        &self,
        length: &DataArray<I>,
        padchar: &Utf8Array,
        placement: PadPlacement,
    ) -> DaftResult<Utf8Array>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord,
    {
        let input_length = self.len();
        let other_lengths = [length.len(), padchar.len()];

        // Parse the expected `result_len` from the length of the input and other arguments
        let expected_size = if input_length == 0 {
            // Empty input: expect empty output
            0
        } else if other_lengths.iter().all(|&x| x == input_length) {
            // All lengths matching: expect non-broadcasted length
            input_length
        } else if let [broadcasted_len] = std::iter::once(&input_length)
            .chain(other_lengths.iter())
            .filter(|&&x| x != 1)
            .sorted()
            .dedup()
            .collect::<Vec<&usize>>()
            .as_slice()
        {
            // All non-unit lengths match: expect broadcast
            **broadcasted_len
        } else {
            let invalid_length_str = itertools::Itertools::join(
                &mut std::iter::once(&input_length)
                    .chain(other_lengths.iter())
                    .map(|x| x.to_string()),
                ", ",
            );
            return Err(DaftError::ValueError(format!(
                "Inputs have invalid lengths: {invalid_length_str}"
            )))?;
        };

        // check if any array has all nulls
        if self.null_count() == self.len()
            || length.null_count() == length.len()
            || padchar.null_count() == padchar.len()
        {
            return Ok(Utf8Array::full_null(
                self.name(),
                &DataType::Utf8,
                expected_size,
            ));
        }

        fn pad_str(
            val: &str,
            length: usize,
            fillchar: &str,
            placement_fn: impl Fn(Take<Repeat<char>>, &str) -> String,
        ) -> DaftResult<String> {
            if val.chars().count() >= length {
                return Ok(val.chars().take(length).collect());
            }
            let fillchar = if fillchar.is_empty() {
                return Err(DaftError::ComputeError(
                    "Error in pad: empty pad character".to_string(),
                ));
            } else if fillchar.chars().count() > 1 {
                return Err(DaftError::ComputeError(format!(
                    "Error in pad: {} is not a valid pad character",
                    fillchar.len()
                )));
            } else {
                fillchar.chars().next().unwrap()
            };
            let fillchar =
                std::iter::repeat(fillchar).take(length.saturating_sub(val.chars().count()));
            Ok(placement_fn(fillchar, val))
        }

        let placement_fn = match placement {
            PadPlacement::Left => |fillchar: Take<Repeat<char>>, val: &str| -> String {
                fillchar.chain(val.chars()).collect()
            },
            PadPlacement::Right => |fillchar: Take<Repeat<char>>, val: &str| -> String {
                val.chars().chain(fillchar).collect()
            },
        };

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        let padchar_iter = create_broadcasted_str_iter(padchar, expected_size);
        let result = match length.len() {
            1 => {
                let len = length.get(0).unwrap();
                let len: usize = NumCast::from(len).ok_or_else(|| {
                    DaftError::ComputeError(format!(
                        "Error in pad: failed to cast length as usize {len}"
                    ))
                })?;
                let arrow_result = self_iter
                    .zip(padchar_iter)
                    .map(|(val, padchar)| match (val, padchar) {
                        (Some(val), Some(padchar)) => {
                            Ok(Some(pad_str(val, len, padchar, placement_fn)?))
                        }
                        _ => Ok(None),
                    })
                    .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

                Utf8Array::from((self.name(), Box::new(arrow_result)))
            }
            _ => {
                let length_iter = length.as_arrow().iter();
                let arrow_result = self_iter
                    .zip(length_iter)
                    .zip(padchar_iter)
                    .map(|((val, len), padchar)| match (val, len, padchar) {
                        (Some(val), Some(len), Some(padchar)) => {
                            let len: usize = NumCast::from(*len).ok_or_else(|| {
                                DaftError::ComputeError(format!(
                                    "Error in pad: failed to cast length as usize {len}"
                                ))
                            })?;
                            Ok(Some(pad_str(val, len, padchar, placement_fn)?))
                        }
                        _ => Ok(None),
                    })
                    .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

                Utf8Array::from((self.name(), Box::new(arrow_result)))
            }
        };

        assert_eq!(result.len(), expected_size);
        Ok(result)
    }

    fn binary_broadcasted_compare<ScalarKernel>(
        &self,
        other: &Self,
        operation: ScalarKernel,
        op_name: &str,
    ) -> DaftResult<BooleanArray>
    where
        ScalarKernel: Fn(&str, &str) -> DaftResult<bool>,
    {
        let (is_full_null, expected_size) = parse_inputs(self, &[other])
            .map_err(|e| DaftError::ValueError(format!("Error in {op_name}: {e}")))?;
        if is_full_null {
            return Ok(BooleanArray::full_null(
                self.name(),
                &DataType::Boolean,
                expected_size,
            ));
        }
        if expected_size == 0 {
            return Ok(BooleanArray::empty(self.name(), &DataType::Boolean));
        }

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        let other_iter = create_broadcasted_str_iter(other, expected_size);
        let arrow_result = self_iter
            .zip(other_iter)
            .map(|(self_v, other_v)| match (self_v, other_v) {
                (Some(self_v), Some(other_v)) => operation(self_v, other_v).map(Some),
                _ => Ok(None),
            })
            .collect::<DaftResult<arrow2::array::BooleanArray>>();

        let result = BooleanArray::from((self.name(), arrow_result?));
        assert_eq!(result.len(), expected_size);
        Ok(result)
    }

    fn unary_broadcasted_op<ScalarKernel>(&self, operation: ScalarKernel) -> DaftResult<Utf8Array>
    where
        ScalarKernel: Fn(&str) -> Cow<'_, str>,
    {
        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|val| Some(operation(val?)))
            .collect::<arrow2::array::Utf8Array<i64>>()
            .with_validity(self_arrow.validity().cloned());
        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_endswith_utf_arrays_broadcast() -> DaftResult<()> {
        let data = Utf8Array::from((
            "data",
            Box::new(arrow2::array::Utf8Array::<i64>::from(vec![
                "x_foo".into(),
                "y_foo".into(),
                "z_bar".into(),
            ])),
        ));
        let pattern = Utf8Array::from((
            "pattern",
            Box::new(arrow2::array::Utf8Array::<i64>::from(vec!["foo".into()])),
        ));
        let result = &data.endswith(&pattern)?;
        assert_eq!(result.len(), 3);
        assert!(result.as_arrow().value(0));
        assert!(result.as_arrow().value(1));
        assert!(!result.as_arrow().value(2));
        Ok(())
    }

    #[test]
    fn check_endswith_utf_arrays() -> DaftResult<()> {
        let data = Utf8Array::from((
            "data",
            Box::new(arrow2::array::Utf8Array::<i64>::from(vec![
                "x_foo".into(),
                "y_foo".into(),
                "z_bar".into(),
            ])),
        ));
        let pattern = Utf8Array::from((
            "pattern",
            Box::new(arrow2::array::Utf8Array::<i64>::from(vec![
                "foo".into(),
                "wrong".into(),
                "bar".into(),
            ])),
        ));
        let result = &data.endswith(&pattern)?;
        assert_eq!(result.len(), 3);
        assert!(result.as_arrow().value(0));
        assert!(!result.as_arrow().value(1));
        assert!(result.as_arrow().value(2));
        Ok(())
    }
}
