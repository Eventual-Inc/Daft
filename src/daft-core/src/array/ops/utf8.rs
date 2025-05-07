use std::{
    borrow::Cow,
    iter::{self},
    sync::Arc,
};

use aho_corasick::{AhoCorasickBuilder, MatchKind};
use arrow2::{
    array::BinaryArray as ArrowBinaryArray, datatypes::DataType as ArrowType, offset::Offsets,
};
use common_error::{DaftError, DaftResult};
use itertools::Itertools;

use super::{as_arrow::AsArrow, full::FullNull};
use crate::{array::prelude::*, datatypes::prelude::*};

enum BroadcastedStrIter<'a> {
    Repeat(std::iter::RepeatN<Option<&'a str>>),
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
        BroadcastedStrIter::Repeat(std::iter::repeat_n(arr.get(0), len))
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

impl Utf8Array {
    pub fn upper(&self) -> DaftResult<Self> {
        self.unary_broadcasted_op(|val| val.to_uppercase().into())
    }

    pub fn binary_broadcasted_compare<ScalarKernel>(
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

    // Uses the Aho-Corasick algorithm to count occurrences of a number of patterns.
    pub fn count_matches(
        &self,
        patterns: &Self,
        whole_word: bool,
        case_sensitive: bool,
    ) -> DaftResult<UInt64Array> {
        if patterns.null_count() == patterns.len() {
            // no matches
            return UInt64Array::from_iter(
                Arc::new(Field::new(self.name(), DataType::UInt64)),
                iter::repeat_n(Some(0), self.len()),
            )
            .with_validity(self.validity().cloned());
        }

        let patterns = patterns.as_arrow().iter().flatten();
        let ac = AhoCorasickBuilder::new()
            .ascii_case_insensitive(!case_sensitive)
            .match_kind(MatchKind::LeftmostLongest)
            .build(patterns)
            .map_err(|e| {
                DaftError::ComputeError(format!("Error creating string automaton: {}", e))
            })?;
        let iter = self.as_arrow().iter().map(|opt| {
            opt.map(|s| {
                let results = ac.find_iter(s);
                if whole_word {
                    results
                        .filter(|m| {
                            // ensure this match is a whole word (or set of words)
                            // don't want to filter out things like "brass"
                            let prev_char = s.get(m.start() - 1..m.start());
                            let next_char = s.get(m.end()..=m.end());
                            !(prev_char.is_some_and(|s| s.chars().next().unwrap().is_alphabetic())
                                || next_char
                                    .is_some_and(|s| s.chars().next().unwrap().is_alphabetic()))
                        })
                        .count() as u64
                } else {
                    results.count() as u64
                }
            })
        });
        Ok(UInt64Array::from_iter(
            Arc::new(Field::new(self.name(), DataType::UInt64)),
            iter,
        ))
    }

    pub fn unary_broadcasted_op<ScalarKernel>(&self, operation: ScalarKernel) -> DaftResult<Self>
    where
        ScalarKernel: Fn(&str) -> Cow<'_, str>,
    {
        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|val| Some(operation(val?)))
            .collect::<arrow2::array::Utf8Array<i64>>()
            .with_validity(self_arrow.validity().cloned());
        Ok(Self::from((self.name(), Box::new(arrow_result))))
    }

    /// For text-to-binary encoding.
    pub fn encode<Encoder>(&self, encoder: Encoder) -> DaftResult<BinaryArray>
    where
        Encoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow();
        let buffer = input.values();
        let validity = input.validity().cloned();
        //
        let mut values = Vec::<u8>::new();
        let mut offsets = Offsets::<i64>::new();
        for span in input.offsets().windows(2) {
            let s = span[0] as usize;
            let e = span[1] as usize;
            let bytes = encoder(&buffer[s..e])?;
            //
            offsets.try_push(bytes.len() as i64)?;
            values.extend(bytes);
        }
        //
        let array = ArrowBinaryArray::new(
            ArrowType::LargeBinary,
            offsets.into(),
            values.into(),
            validity,
        );
        let array = Box::new(array);
        Ok(BinaryArray::from((self.name(), array)))
    }

    /// For text-to-binary encoding, but inserts nulls on failures.
    pub fn try_encode<Encoder>(&self, _: Encoder) -> DaftResult<BinaryArray>
    where
        Encoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        todo!("try_encode")
    }
}
