use crate::{
    array::ListArray,
    datatypes::{BooleanArray, Field, UInt64Array, Utf8Array},
    DataType, Series,
};
use arrow2;

use common_error::{DaftError, DaftResult};

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
    let flat_child =
        Series::try_from(("splits", Box::new(splits) as Box<dyn arrow2::array::Array>))?;
    Ok(ListArray::new(
        Field::new(name, DataType::List(Box::new(DataType::Utf8))),
        flat_child,
        offsets,
        validity,
    ))
}

impl Utf8Array {
    pub fn endswith(&self, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
        self.binary_broadcasted_compare(pattern, |data: &str, pat: &str| data.ends_with(pat))
    }

    pub fn startswith(&self, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
        self.binary_broadcasted_compare(pattern, |data: &str, pat: &str| data.starts_with(pat))
    }

    pub fn contains(&self, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
        self.binary_broadcasted_compare(pattern, |data: &str, pat: &str| data.contains(pat))
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
                "lhs and rhs have different length arrays: {self_len} vs {pattern_len}"
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

    fn binary_broadcasted_compare<ScalarKernel>(
        &self,
        other: &Self,
        operation: ScalarKernel,
    ) -> DaftResult<BooleanArray>
    where
        ScalarKernel: Fn(&str, &str) -> bool,
    {
        let self_arrow = self.as_arrow();
        let other_arrow = other.as_arrow();
        match (self.len(), other.len()) {
            // Matching len case:
            (self_len, other_len) if self_len == other_len => {
                let arrow_result: arrow2::array::BooleanArray = self_arrow
                    .into_iter()
                    .zip(other_arrow)
                    .map(|(val, pat)| Some(operation(val?, pat?)))
                    .collect();
                Ok(BooleanArray::from((self.name(), arrow_result)))
            }
            // Broadcast other case:
            (self_len, 1) => {
                let other_scalar_value = other.get(0);
                match other_scalar_value {
                    None => Ok(BooleanArray::full_null(
                        self.name(),
                        self.data_type(),
                        self_len,
                    )),
                    Some(other_v) => {
                        let arrow_result: arrow2::array::BooleanArray = self_arrow
                            .into_iter()
                            .map(|self_v| Some(operation(self_v?, other_v)))
                            .collect();
                        Ok(BooleanArray::from((self.name(), arrow_result)))
                    }
                }
            }
            // Broadcast self case
            (1, other_len) => {
                let self_scalar_value = self.get(0);
                match self_scalar_value {
                    None => Ok(BooleanArray::full_null(
                        self.name(),
                        self.data_type(),
                        other_len,
                    )),
                    Some(self_v) => {
                        let arrow_result: arrow2::array::BooleanArray = other_arrow
                            .into_iter()
                            .map(|other_v| Some(operation(self_v, other_v?)))
                            .collect();
                        Ok(BooleanArray::from((self.name(), arrow_result)))
                    }
                }
            }
            // Mismatched len case:
            (self_len, other_len) => Err(DaftError::ComputeError(format!(
                "lhs and rhs have different length arrays: {self_len} vs {other_len}"
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
