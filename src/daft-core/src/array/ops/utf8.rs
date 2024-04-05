use std::borrow::Cow;

use crate::{
    array::{DataArray, ListArray},
    datatypes::{
        BooleanArray, DaftArrayType, DaftIntegerType, DaftNumericType, DaftPhysicalType, Field,
        Int64Array, UInt64Array, Utf8Array,
    },
    DataType, Series,
};
use arrow2::{self, array::Array};

use common_error::{DaftError, DaftResult};
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

fn is_valid_input_lengths(lengths: &[usize]) -> bool {
    // Check if all elements are equal
    if lengths.iter().all(|&x| x == lengths[0]) {
        return true;
    }

    // Separate the elements into '1's and others
    let ones_count = lengths.iter().filter(|&&x| x == 1).count();
    let others: Vec<&usize> = lengths.iter().filter(|&&x| x != 1).collect();

    if ones_count > 0 && !others.is_empty() {
        // Check if all 'other' elements are equal and greater than 1, which means that this is a broadcastable operation
        others.iter().all(|&&x| x == *others[0] && x > 1)
    } else {
        false
    }
}

fn check_valid_inputs<T, I>(
    self_arr: &Utf8Array,
    arrs: &[&DataArray<I>],
    op_name: &str,
    dtype: &DataType,
) -> Option<DaftResult<T>>
where
    T: DaftArrayType + FullNull,
    I: DaftPhysicalType,
{
    assert!(!arrs.is_empty());
    let self_len = self_arr.len();

    // check valid input lengths
    let lengths = std::iter::once(self_len)
        .chain(arrs.iter().map(|arr| arr.len()))
        .collect::<Vec<_>>();
    if !is_valid_input_lengths(&lengths) {
        let lengths_str = lengths
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(" vs ");
        return Some(Err(DaftError::ValueError(format!(
            "Error in {op_name}: inputs have different lengths: {lengths_str}"
        ))));
    }

    // check if all arrays are empty
    if lengths.iter().all(|&x| x == 0) {
        return Some(Ok(T::empty(arrs[0].name(), dtype)));
    }

    // check if any array has all nulls
    if arrs.iter().any(|arr| arr.null_count() == arr.len()) || self_arr.null_count() == self_len {
        let result_len = lengths.iter().max().unwrap();
        return Some(Ok(T::full_null(arrs[0].name(), dtype, *result_len)));
    }

    None
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
        let is_valid = check_valid_inputs(
            self,
            &[pattern],
            "split",
            &DataType::List(Box::new(DataType::Utf8)),
        );
        if let Some(result) = is_valid {
            return result;
        }

        let result_len = std::cmp::max(self.len(), pattern.len());
        let self_arrow = self.as_arrow();
        let buffer_len = self_arrow.values().len();
        // This will overallocate by pattern_len * N_i, where N_i is the number of pattern occurences in the ith string in arr_iter.
        let mut splits = arrow2::array::MutableUtf8Array::with_capacity(buffer_len);
        let mut offsets = arrow2::offset::Offsets::new();
        let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(self.len());

        let self_iter = create_broadcasted_str_iter(self, result_len);
        match (regex, pattern.len()) {
            (true, 1) => {
                let regex = regex::Regex::new(pattern.get(0).unwrap());
                let regex_iter = std::iter::repeat(Some(regex)).take(result_len);
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
                let pattern_iter = create_broadcasted_str_iter(pattern, result_len);
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
        Ok(ListArray::new(
            Field::new(self.name(), DataType::List(Box::new(DataType::Utf8))),
            flat_child,
            offsets,
            validity,
        ))
    }

    pub fn extract(&self, pattern: &Utf8Array, index: usize) -> DaftResult<Utf8Array> {
        let is_valid = check_valid_inputs(self, &[pattern], "extract", &DataType::Utf8);
        if let Some(result) = is_valid {
            return result;
        }

        let result_len = std::cmp::max(self.len(), pattern.len());
        let self_iter = create_broadcasted_str_iter(self, result_len);
        match pattern.len() {
            1 => {
                let regex = regex::Regex::new(pattern.get(0).unwrap());
                let regex_iter = std::iter::repeat(Some(regex)).take(result_len);
                regex_extract_first_match(self_iter, regex_iter, index, self.name())
            }
            _ => {
                let regex_iter = pattern
                    .as_arrow()
                    .iter()
                    .map(|pat| pat.map(regex::Regex::new));
                regex_extract_first_match(self_iter, regex_iter, index, self.name())
            }
        }
    }

    pub fn extract_all(&self, pattern: &Utf8Array, index: usize) -> DaftResult<ListArray> {
        let is_valid = check_valid_inputs(
            self,
            &[pattern],
            "extract_all",
            &DataType::List(Box::new(DataType::Utf8)),
        );
        if let Some(result) = is_valid {
            return result;
        }

        let result_len = std::cmp::max(self.len(), pattern.len());
        let self_iter = create_broadcasted_str_iter(self, result_len);
        match pattern.len() {
            1 => {
                let regex = regex::Regex::new(pattern.get(0).unwrap());
                let regex_iter = std::iter::repeat(Some(regex)).take(result_len);
                regex_extract_all_matches(self_iter, regex_iter, index, result_len, self.name())
            }
            _ => {
                let regex_iter = pattern
                    .as_arrow()
                    .iter()
                    .map(|pat| pat.map(regex::Regex::new));
                regex_extract_all_matches(self_iter, regex_iter, index, result_len, self.name())
            }
        }
    }

    pub fn replace(
        &self,
        pattern: &Utf8Array,
        replacement: &Utf8Array,
        regex: bool,
    ) -> DaftResult<Utf8Array> {
        let is_valid =
            check_valid_inputs(self, &[pattern, replacement], "replace", &DataType::Utf8);
        if let Some(result) = is_valid {
            return result;
        }

        let result_len = std::cmp::max(self.len(), std::cmp::max(pattern.len(), replacement.len()));
        let self_iter = create_broadcasted_str_iter(self, result_len);
        let replacement_iter = create_broadcasted_str_iter(replacement, result_len);

        match (regex, pattern.len()) {
            (true, 1) => {
                let regex = regex::Regex::new(pattern.get(0).unwrap());
                let regex_iter = std::iter::repeat(Some(regex)).take(result_len);
                regex_replace(self_iter, regex_iter, replacement_iter, self.name())
            }
            (true, _) => {
                let regex_iter = pattern
                    .as_arrow()
                    .iter()
                    .map(|pat| pat.map(regex::Regex::new));
                regex_replace(self_iter, regex_iter, replacement_iter, self.name())
            }
            (false, _) => {
                let pattern_iter = create_broadcasted_str_iter(pattern, result_len);
                replace_on_literal(self_iter, pattern_iter, replacement_iter, self.name())
            }
        }
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
        let is_valid = check_valid_inputs(self, &[substr], "find", &DataType::Int64);
        if let Some(result) = is_valid {
            return result;
        }

        let result_len = std::cmp::max(self.len(), substr.len());
        let self_iter = create_broadcasted_str_iter(self, result_len);
        let substr_iter = create_broadcasted_str_iter(substr, result_len);
        let arrow_result = self_iter
            .zip(substr_iter)
            .map(|(val, substr)| match (val, substr) {
                (Some(val), Some(substr)) => {
                    Some(val.find(substr).map(|pos| pos as i64).unwrap_or(-1))
                }
                _ => None,
            })
            .collect::<arrow2::array::Int64Array>();

        Ok(Int64Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn left<I>(&self, nchars: &DataArray<I>) -> DaftResult<Utf8Array>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord,
    {
        let is_valid = check_valid_inputs(self, &[nchars], "left", &DataType::Utf8);
        if let Some(result) = is_valid {
            return result;
        }

        fn left_most_chars(val: &str, n: usize) -> &str {
            if n == 0 || val.is_empty() {
                ""
            } else {
                val.char_indices().nth(n).map_or(val, |(i, _)| &val[..i])
            }
        }

        let result_len = std::cmp::max(self.len(), nchars.len());
        let self_iter = create_broadcasted_str_iter(self, result_len);
        match nchars.len() {
            1 => {
                let n = nchars.get(0).unwrap();
                let n: usize = NumCast::from(n).ok_or_else(|| {
                    DaftError::ComputeError(format!("failed to cast rhs as usize {n}"))
                })?;
                let nchars_iter = std::iter::repeat(n).take(result_len);
                let arrow_result = self_iter
                    .zip(nchars_iter)
                    .map(|(val, n)| Some(left_most_chars(val?, n)))
                    .collect::<arrow2::array::Utf8Array<i64>>();
                Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
            }
            _ => {
                let arrow_result = self_iter
                    .zip(nchars.as_arrow().iter())
                    .map(|(val, n)| match (val, n) {
                        (Some(val), Some(nchar)) => {
                            let nchar: usize = NumCast::from(*nchar).ok_or_else(|| {
                                DaftError::ComputeError(format!(
                                    "failed to cast rhs as usize {nchar}"
                                ))
                            })?;
                            Ok(Some(left_most_chars(val, nchar)))
                        }
                        _ => Ok(None),
                    })
                    .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

                Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
            }
        }
    }

    pub fn right<I>(&self, nchars: &DataArray<I>) -> DaftResult<Utf8Array>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord,
    {
        let is_valid = check_valid_inputs(self, &[nchars], "right", &DataType::Utf8);
        if let Some(result) = is_valid {
            return result;
        }

        fn right_most_chars(val: &str, nchar: usize) -> &str {
            if nchar == 0 || val.is_empty() {
                ""
            } else {
                let skip = val.chars().count().saturating_sub(nchar);
                val.char_indices().nth(skip).map_or(val, |(i, _)| &val[i..])
            }
        }

        let result_len = std::cmp::max(self.len(), nchars.len());
        let self_iter = create_broadcasted_str_iter(self, result_len);
        match nchars.len() {
            1 => {
                let n = nchars.get(0).unwrap();
                let n: usize = NumCast::from(n).ok_or_else(|| {
                    DaftError::ComputeError(format!("failed to cast rhs as usize {n}"))
                })?;
                let nchars_iter = std::iter::repeat(n).take(result_len);
                let arrow_result = self_iter
                    .zip(nchars_iter)
                    .map(|(val, n)| Some(right_most_chars(val?, n)))
                    .collect::<arrow2::array::Utf8Array<i64>>();
                Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
            }
            _ => {
                let arrow_result = self_iter
                    .zip(nchars.as_arrow().iter())
                    .map(|(val, n)| match (val, n) {
                        (Some(val), Some(nchar)) => {
                            let nchar: usize = NumCast::from(*nchar).ok_or_else(|| {
                                DaftError::ComputeError(format!(
                                    "failed to cast rhs as usize {nchar}"
                                ))
                            })?;
                            Ok(Some(right_most_chars(val, nchar)))
                        }
                        _ => Ok(None),
                    })
                    .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

                Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
            }
        }
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
        let is_valid = check_valid_inputs(self, &[other], op_name, &DataType::Boolean);
        if let Some(result) = is_valid {
            return result;
        }

        let result_len = std::cmp::max(self.len(), other.len());
        let self_iter = create_broadcasted_str_iter(self, result_len);
        let other_iter = create_broadcasted_str_iter(other, result_len);
        let arrow_result = self_iter
            .zip(other_iter)
            .map(|(self_v, other_v)| match (self_v, other_v) {
                (Some(self_v), Some(other_v)) => operation(self_v, other_v).map(Some),
                _ => Ok(None),
            })
            .collect::<DaftResult<arrow2::array::BooleanArray>>();

        Ok(BooleanArray::from((self.name(), arrow_result?)))
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
