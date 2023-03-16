use crate::array::BaseArray;
use crate::datatypes::{BooleanArray, Utf8Array};
use arrow2;

use crate::error::{DaftError, DaftResult};

impl Utf8Array {
    pub fn endswith(&self, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
        let data_arrow = self.downcast();
        let pattern_arrow = pattern.downcast();
        match (self.len(), pattern.len()) {
            // Broadcast pattern case:
            (data_len, 1) => match pattern_arrow.validity() {
                Some(validity) if !validity.get_bit(0) => {
                    Ok(BooleanArray::full_null(self.name(), data_len))
                }
                _ => {
                    let pattern_val = pattern_arrow.value(0);
                    let arrow_result: arrow2::array::BooleanArray = data_arrow
                        .into_iter()
                        .map(|val| Some(val?.ends_with(pattern_val)))
                        .collect();
                    Ok(BooleanArray::from((self.name(), arrow_result)))
                }
            },
            // Broadcast data case
            (1, pattern_len) => match data_arrow.validity() {
                Some(validity) if !validity.get_bit(0) => {
                    Ok(BooleanArray::full_null(self.name(), pattern_len))
                }
                _ => {
                    let data_val = data_arrow.value(0);
                    let arrow_result: arrow2::array::BooleanArray = pattern_arrow
                        .into_iter()
                        .map(|pat| Some(data_val.ends_with(pat?)))
                        .collect();
                    Ok(BooleanArray::from((self.name(), arrow_result)))
                }
            },
            // Matching len case:
            (data_len, pattern_len) if data_len == pattern_len => {
                let arrow_result: arrow2::array::BooleanArray = data_arrow
                    .into_iter()
                    .zip(pattern_arrow.into_iter())
                    .map(|(val, pat)| Some(val?.ends_with(pat?)))
                    .collect();
                Ok(BooleanArray::from((self.name(), arrow_result)))
            }
            // Mismatched len case:
            (data_len, pattern_len) => Err(DaftError::ComputeError(format!(
                "lhs and rhs have different length arrays: {data_len} vs {pattern_len}"
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
        assert!(result.downcast().value(0));
        assert!(result.downcast().value(1));
        assert!(!result.downcast().value(2));
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
        assert!(result.downcast().value(0));
        assert!(!result.downcast().value(1));
        assert!(result.downcast().value(2));
        Ok(())
    }
}
