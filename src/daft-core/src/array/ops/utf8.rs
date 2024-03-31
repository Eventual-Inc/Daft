use crate::{
    array::{DataArray, ListArray},
    datatypes::{
        BooleanArray, DaftIntegerType, DaftNumericType, Field, Int64Array, UInt64Array, Utf8Array,
    },
    DataType, Series,
};
use arrow2::{self, array::Array};

use common_error::{DaftError, DaftResult};
use num_traits::NumCast;

use super::{as_arrow::AsArrow, full::FullNull};

fn split_array_on_patterns<'a, T, U>(
    arr_iter: T,
    pattern_iter: U,
    buffer_len: usize,
    name: &str,
) -> DaftResult<ListArray>
where
    T: arrow2::trusted_len::TrustedLen + Iterator<Item = Option<&'a str>>,
    U: Iterator<Item = Option<&'a str>>,
{
    // This will overallocate by pattern_len * N_i, where N_i is the number of pattern occurences in the ith string in arr_iter.
    let mut splits = arrow2::array::MutableUtf8Array::with_capacity(buffer_len);
    // arr_iter implements TrustedLen, so we can always use size_hint().1 as the exact length of the iterator. The only
    // time this would fail is if the length of the iterator exceeds usize::MAX, which should never happen for an i64
    // offset array, since the array length can't exceed i64::MAX on 64-bit machines.
    let arr_len = arr_iter.size_hint().1.unwrap();
    let mut offsets = arrow2::offset::Offsets::new();
    let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(arr_len);
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
        Field::new(name, DataType::List(Box::new(DataType::Utf8))),
        flat_child,
        offsets,
        validity,
    ))
}

fn right_most_chars(val: &str, nchar: usize) -> &str {
    let len = val.chars().count();
    if nchar == 0 || len == 0 {
        ""
    } else if nchar >= len {
        val
    } else {
        let skip = len.saturating_sub(nchar);
        val.char_indices().nth(skip).map_or("", |(i, _)| &val[i..])
    }
}

fn regex_extract_first_match<'a>(
    iter: impl Iterator<Item = (Option<&'a str>, Option<Result<regex::Regex, regex::Error>>)>,
    index: usize,
    name: &str,
) -> DaftResult<Utf8Array> {
    let arrow_result = iter
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
    iter: impl Iterator<Item = (Option<&'a str>, Option<Result<regex::Regex, regex::Error>>)>,
    index: usize,
    len: usize,
    name: &str,
) -> DaftResult<ListArray> {
    let mut matches = arrow2::array::MutableUtf8Array::new();
    let mut offsets = arrow2::offset::Offsets::new();
    let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(len);

    for (val, re) in iter {
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

    pub fn split(&self, pattern: &Utf8Array) -> DaftResult<ListArray> {
        let self_arrow = self.as_arrow();
        let pattern_arrow = pattern.as_arrow();
        // Handle all-null cases.
        if self_arrow
            .validity()
            .map_or(false, |v| v.unset_bits() == v.len())
            || pattern_arrow
                .validity()
                .map_or(false, |v| v.unset_bits() == v.len())
        {
            return Ok(ListArray::full_null(
                self.name(),
                &DataType::List(Box::new(DataType::Utf8)),
                std::cmp::max(self.len(), pattern.len()),
            ));
        // Handle empty cases.
        } else if self.is_empty() || pattern.is_empty() {
            return Ok(ListArray::empty(
                self.name(),
                &DataType::List(Box::new(DataType::Utf8)),
            ));
        }
        let buffer_len = self_arrow.values().len();
        match (self.len(), pattern.len()) {
            // Matching len case:
            (self_len, pattern_len) if self_len == pattern_len => split_array_on_patterns(
                self_arrow.into_iter(),
                pattern_arrow.into_iter(),
                buffer_len,
                self.name(),
            ),
            // Broadcast pattern case:
            (self_len, 1) => {
                let pattern_scalar_value = pattern.get(0).unwrap();
                split_array_on_patterns(
                    self_arrow.into_iter(),
                    std::iter::repeat(Some(pattern_scalar_value)).take(self_len),
                    buffer_len,
                    self.name(),
                )
            }
            // Broadcast self case:
            (1, pattern_len) => {
                let self_scalar_value = self.get(0).unwrap();
                split_array_on_patterns(
                    std::iter::repeat(Some(self_scalar_value)).take(pattern_len),
                    pattern_arrow.into_iter(),
                    buffer_len * pattern_len,
                    self.name(),
                )
            }
            // Mismatched len case:
            (self_len, pattern_len) => Err(DaftError::ComputeError(format!(
                "Error in split: lhs and rhs have different length arrays: {self_len} vs {pattern_len}"
            ))),
        }
    }

    pub fn extract(&self, pattern: &Utf8Array, index: usize) -> DaftResult<Utf8Array> {
        let self_arrow = self.as_arrow();
        let pattern_arrow = pattern.as_arrow();

        if self.is_empty() || pattern.is_empty() {
            return Ok(Utf8Array::empty(self.name(), self.data_type()));
        }

        match (self.len(), pattern.len()) {
            // Matching len case:
            (self_len, pattern_len) if self_len == pattern_len => {
                let regexes = pattern_arrow.iter().map(|pat| pat.map(regex::Regex::new));
                let iter = self_arrow.iter().zip(regexes);
                regex_extract_first_match(iter, index, self.name())
            }
            // Broadcast pattern case:
            (self_len, 1) => {
                let pattern_scalar_value = pattern.get(0);
                match pattern_scalar_value {
                    None => Ok(Utf8Array::full_null(
                        self.name(),
                        self.data_type(),
                        self_len,
                    )),
                    Some(pattern_v) => {
                        let re = Some(regex::Regex::new(pattern_v));
                        let iter = self_arrow.iter().zip(std::iter::repeat(re).take(self_len));
                        regex_extract_first_match(iter, index, self.name())
                    }
                }
            }
            // Broadcast self case
            (1, pattern_len) => {
                let self_scalar_value = self.get(0);
                match self_scalar_value {
                    None => Ok(Utf8Array::full_null(
                        self.name(),
                        self.data_type(),
                        pattern_len,
                    )),
                    Some(self_v) => {
                        let regexes = pattern_arrow.iter().map(|pat| pat.map(regex::Regex::new));
                        let iter = std::iter::repeat(Some(self_v)).take(pattern_len).zip(regexes);
                        regex_extract_first_match(iter, index, self.name())
                    }
                }
            }
            // Mismatched len case:
            (self_len, pattern_len) => Err(DaftError::ComputeError(format!(
                "Error in extract: lhs and rhs have different length arrays: {self_len} vs {pattern_len}"
            ))),
        }
    }

    pub fn extract_all(&self, pattern: &Utf8Array, index: usize) -> DaftResult<ListArray> {
        let self_arrow = self.as_arrow();
        let pattern_arrow = pattern.as_arrow();

        if self.is_empty() || pattern.is_empty() {
            return Ok(ListArray::empty(
                self.name(),
                &DataType::List(Box::new(DataType::Utf8)),
            ));
        }

        match (self.len(), pattern.len()) {
            // Matching len case:
            (self_len, pattern_len) if self_len == pattern_len => {
                let regexes = pattern_arrow.iter().map(|pat| pat.map(regex::Regex::new));
                let iter = self_arrow.iter().zip(regexes);
                regex_extract_all_matches(iter, index, self_len, self.name())
            }
            // Broadcast pattern case:
            (self_len, 1) => {
                let pattern_scalar_value = pattern.get(0);
                match pattern_scalar_value {
                    None => Ok(ListArray::full_null(
                        self.name(),
                        &DataType::List(Box::new(DataType::Utf8)),
                        self_len,
                    )),
                    Some(pattern_v) => {
                        let re = Some(regex::Regex::new(pattern_v));
                        let iter = self_arrow.iter().zip(std::iter::repeat(re).take(self_len));
                        regex_extract_all_matches(iter, index, self_len, self.name())
                    }
                }
            }
            // Broadcast self case
            (1, pattern_len) => {
                let self_scalar_value = self.get(0);
                match self_scalar_value {
                    None => Ok(ListArray::full_null(
                        self.name(),
                        &DataType::List(Box::new(DataType::Utf8)),
                        pattern_len,
                    )),
                    Some(self_v) => {
                        let regexes = pattern_arrow.iter().map(|pat| pat.map(regex::Regex::new));
                        let iter = std::iter::repeat(Some(self_v)).take(pattern_len).zip(regexes);
                        regex_extract_all_matches(iter, index, pattern_len, self.name())
                    }
                }
            }
            // Mismatched len case:
            (self_len, pattern_len) => Err(DaftError::ComputeError(format!(
                "Error in extract_all: lhs and rhs have different length arrays: {self_len} vs {pattern_len}"
            ))),
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
        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|val| {
                let v = val?;
                Some(v.to_lowercase())
            })
            .collect::<arrow2::array::Utf8Array<i64>>()
            .with_validity(self_arrow.validity().cloned());
        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn upper(&self) -> DaftResult<Utf8Array> {
        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|val| {
                let v = val?;
                Some(v.to_uppercase())
            })
            .collect::<arrow2::array::Utf8Array<i64>>()
            .with_validity(self_arrow.validity().cloned());
        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn lstrip(&self) -> DaftResult<Utf8Array> {
        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|val| {
                let v = val?;
                Some(v.trim_start())
            })
            .collect::<arrow2::array::Utf8Array<i64>>()
            .with_validity(self_arrow.validity().cloned());
        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn rstrip(&self) -> DaftResult<Utf8Array> {
        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|val| {
                let v = val?;
                Some(v.trim_end())
            })
            .collect::<arrow2::array::Utf8Array<i64>>()
            .with_validity(self_arrow.validity().cloned());
        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn reverse(&self) -> DaftResult<Utf8Array> {
        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|val| {
                let v = val?;
                Some(v.chars().rev().collect::<String>())
            })
            .collect::<arrow2::array::Utf8Array<i64>>()
            .with_validity(self_arrow.validity().cloned());
        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn capitalize(&self) -> DaftResult<Utf8Array> {
        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|val| {
                let v = val?;
                let mut chars = v.chars();
                match chars.next() {
                    None => Some(String::new()),
                    Some(first) => {
                        let first_char_uppercased = first.to_uppercase();
                        let mut res = String::with_capacity(v.len());
                        res.extend(first_char_uppercased);
                        res.extend(chars.flat_map(|c| c.to_lowercase()));
                        Some(res)
                    }
                }
            })
            .collect::<arrow2::array::Utf8Array<i64>>()
            .with_validity(self_arrow.validity().cloned());
        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn find(&self, substr: &Utf8Array) -> DaftResult<Int64Array> {
        let self_arrow = self.as_arrow();
        let substr_arrow = substr.as_arrow();
        // Handle empty cases.
        if self.is_empty() || substr.is_empty() {
            return Ok(Int64Array::empty(self.name(), self.data_type()));
        }
        match (self.len(), substr.len()) {
            // matching len case
            (self_len, substr_len) if self_len == substr_len => {
                let arrow_result = self_arrow
                    .iter()
                    .zip(substr_arrow.iter())
                    .map(|(val, substr)| match (val, substr) {
                        (Some(val), Some(substr)) => {
                            Some(val.find(substr).map(|pos| pos as i64).unwrap_or(-1))
                        }
                        _ => None,
                    })
                    .collect::<arrow2::array::Int64Array>();

                Ok(Int64Array::from((self.name(), Box::new(arrow_result))))
            }
            // broadcast pattern case
            (self_len, 1) => {
                let substr_scalar_value = substr.get(0);
                match substr_scalar_value {
                    None => Ok(Int64Array::full_null(
                        self.name(),
                        &DataType::Int64,
                        self_len,
                    )),
                    Some(substr_scalar_value) => {
                        let arrow_result = self_arrow
                            .iter()
                            .map(|val| {
                                let v = val?;
                                Some(
                                    v.find(substr_scalar_value)
                                        .map(|pos| pos as i64)
                                        .unwrap_or(-1),
                                )
                            })
                            .collect::<arrow2::array::Int64Array>();

                        Ok(Int64Array::from((self.name(), Box::new(arrow_result))))
                    }
                }
            }
            // broadcast self case
            (1, substr_len) => {
                let self_scalar_value = self.get(0);
                match self_scalar_value {
                    None => Ok(Int64Array::full_null(
                        self.name(),
                        &DataType::Int64,
                        substr_len,
                    )),
                    Some(self_scalar_value) => {
                        let arrow_result = substr_arrow
                            .iter()
                            .map(|substr| {
                                let substr = substr?;
                                Some(
                                    self_scalar_value
                                        .find(substr)
                                        .map(|pos| pos as i64)
                                        .unwrap_or(-1),
                                )
                            })
                            .collect::<arrow2::array::Int64Array>();

                        Ok(Int64Array::from((self.name(), Box::new(arrow_result))))
                    }
                }
            }
            // Mismatched len case:
            (self_len, substr_len) => Err(DaftError::ComputeError(format!(
                "lhs and rhs have different length arrays: {self_len} vs {substr_len}"
            ))),
        }
    }

    pub fn left<I>(&self, n: &DataArray<I>) -> DaftResult<Utf8Array>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord,
    {
        let self_arrow = self.as_arrow();
        let n_arrow = n.as_arrow();
        // Handle empty cases.
        if self.is_empty() || n_arrow.is_empty() {
            return Ok(Utf8Array::empty(self.name(), self.data_type()));
        }
        match (self.len(), n_arrow.len()) {
            // Matching len case:
            (self_len, n_len) if self_len == n_len => {
                let arrow_result = self_arrow
                    .iter()
                    .zip(n_arrow.iter())
                    .map(|(val, n)| match (val, n) {
                        (Some(val), Some(nchar)) => {
                            let nchar: usize = NumCast::from(*nchar).ok_or_else(|| {
                                DaftError::ComputeError(format!(
                                    "Error in left: failed to cast rhs as usize {nchar}"
                                ))
                            })?;
                            Ok(Some(val.chars().take(nchar).collect::<String>()))
                        }
                        _ => Ok(None),
                    })
                    .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

                Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
            }
            // Broadcast pattern case:
            (self_len, 1) => {
                let n_scalar_value = n.get(0);
                match n_scalar_value {
                    None => Ok(Utf8Array::full_null(
                        self.name(),
                        self.data_type(),
                        self_len,
                    )),
                    Some(n_scalar_value) => {
                        let n_scalar_value: usize =
                            NumCast::from(n_scalar_value).ok_or_else(|| {
                                DaftError::ComputeError(format!(
                                    "Error in left: failed to cast rhs as usize {n_scalar_value}"
                                ))
                            })?;
                        let arrow_result = self_arrow
                            .iter()
                            .map(|val| {
                                let v = val?;
                                Some(v.chars().take(n_scalar_value).collect::<String>())
                            })
                            .collect::<arrow2::array::Utf8Array<i64>>();

                        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
                    }
                }
            }
            // broadcast self case:
            (1, n_len) => {
                let self_scalar_value = self.get(0);
                match self_scalar_value {
                    None => Ok(Utf8Array::full_null(self.name(), self.data_type(), n_len)),
                    Some(self_scalar_value) => {
                        let arrow_result = n_arrow
                            .iter()
                            .map(|n| match n {
                                None => Ok(None),
                                Some(n) => {
                                    let n: usize = NumCast::from(*n).ok_or_else(|| {
                                        DaftError::ComputeError(format!(
                                            "Error in left: failed to cast rhs as usize {n}"
                                        ))
                                    })?;
                                    Ok(Some(self_scalar_value.chars().take(n).collect::<String>()))
                                }
                            })
                            .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

                        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
                    }
                }
            }
            // Mismatched len case:
            (self_len, n_len) => Err(DaftError::ComputeError(format!(
                "Error in left: lhs and rhs have different length arrays: {self_len} vs {n_len}"
            ))),
        }
    }

    pub fn right<I>(&self, n: &DataArray<I>) -> DaftResult<Utf8Array>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord,
    {
        let self_arrow = self.as_arrow();
        let n_arrow = n.as_arrow();
        // Handle empty cases.
        if self.is_empty() || n_arrow.is_empty() {
            return Ok(Utf8Array::empty(self.name(), self.data_type()));
        }
        match (self.len(), n_arrow.len()) {
            // Matching len case:
            (self_len, n_len) if self_len == n_len => {
                let arrow_result = self_arrow
                    .iter()
                    .zip(n_arrow.iter())
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
            // Broadcast pattern case:
            (self_len, 1) => {
                let n_scalar_value = n.get(0);
                match n_scalar_value {
                    None => Ok(Utf8Array::full_null(
                        self.name(),
                        self.data_type(),
                        self_len,
                    )),
                    Some(n_scalar_value) => {
                        let n_scalar_value: usize =
                            NumCast::from(n_scalar_value).ok_or_else(|| {
                                DaftError::ComputeError(format!(
                                    "failed to cast rhs as usize {n_scalar_value}"
                                ))
                            })?;
                        let arrow_result = self_arrow
                            .iter()
                            .map(|val| {
                                let v = val?;
                                Some(right_most_chars(v, n_scalar_value))
                            })
                            .collect::<arrow2::array::Utf8Array<i64>>();

                        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
                    }
                }
            }
            // broadcast self
            (1, n_len) => {
                let self_scalar_value = self.get(0);
                match self_scalar_value {
                    None => Ok(Utf8Array::full_null(self.name(), self.data_type(), n_len)),
                    Some(self_scalar_value) => {
                        let arrow_result = n_arrow
                            .iter()
                            .map(|n| match n {
                                None => Ok(None),
                                Some(n) => {
                                    let n: usize = NumCast::from(*n).ok_or_else(|| {
                                        DaftError::ComputeError(format!(
                                            "failed to cast rhs as usize {n}"
                                        ))
                                    })?;
                                    Ok(Some(right_most_chars(self_scalar_value, n)))
                                }
                            })
                            .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

                        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
                    }
                }
            }
            // Mismatched len case:
            (self_len, n_len) => Err(DaftError::ComputeError(format!(
                "lhs and rhs have different length arrays: {self_len} vs {n_len}"
            ))),
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
        let self_arrow = self.as_arrow();
        let other_arrow = other.as_arrow();
        match (self.len(), other.len()) {
            // Matching len case:
            (self_len, other_len) if self_len == other_len => {
                let arrow_result: DaftResult<arrow2::array::BooleanArray> = self_arrow
                    .into_iter()
                    .zip(other_arrow)
                    .map(|(self_v, other_v)| match (self_v, other_v) {
                        (Some(self_v), Some(other_v)) => operation(self_v, other_v).map(Some),
                        _ => Ok(None),
                    })
                    .collect();
                Ok(BooleanArray::from((self.name(), arrow_result?)))
            }
            // Broadcast other case:
            (self_len, 1) => {
                let other_scalar_value = other.get(0);
                match other_scalar_value {
                    None => Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        self_len,
                    )),
                    Some(other_v) => {
                        let arrow_result: DaftResult<arrow2::array::BooleanArray> = self_arrow
                            .into_iter()
                            .map(|self_v| match self_v {
                                Some(self_v) => operation(self_v, other_v).map(Some),
                                None => Ok(None),
                            })
                            .collect();
                        Ok(BooleanArray::from((self.name(), arrow_result?)))
                    }
                }
            }
            // Broadcast self case
            (1, other_len) => {
                let self_scalar_value = self.get(0);
                match self_scalar_value {
                    None => Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        other_len,
                    )),
                    Some(self_v) => {
                        let arrow_result: DaftResult<arrow2::array::BooleanArray> = other_arrow
                            .into_iter()
                            .map(|other_v| match other_v {
                                Some(other_v) => operation(self_v, other_v).map(Some),
                                None => Ok(None),
                            })
                            .collect();
                        Ok(BooleanArray::from((self.name(), arrow_result?)))
                    }
                }
            }
            // Mismatched len case:
            (self_len, other_len) => Err(DaftError::ComputeError(format!(
                "Error in {op_name}: lhs and rhs have different length arrays: {self_len} vs {other_len}"
            ))),
        }
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
