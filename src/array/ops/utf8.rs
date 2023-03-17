use crate::array::BaseArray;
use crate::datatypes::{BooleanArray, Utf8Array};
use arrow2;

use crate::error::{DaftError, DaftResult};

fn endswith_arrow_kernel(
    data: &arrow2::array::Utf8Array<i64>,
    pattern: &arrow2::array::Utf8Array<i64>,
) -> arrow2::array::BooleanArray {
    data.into_iter()
        .zip(pattern.into_iter())
        .map(|(val, pat)| Some(val?.ends_with(pat?)))
        .collect()
}

impl Utf8Array {
    pub fn endswith(&self, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
        self.binary_broadcasted_compare(pattern, endswith_arrow_kernel, |data: &str, pat: &str| {
            data.ends_with(pat)
        })
    }

    fn binary_broadcasted_compare<ArrowKernel, ScalarKernel>(
        &self,
        other: &Self,
        kernel: ArrowKernel,
        operation: ScalarKernel,
    ) -> DaftResult<BooleanArray>
    where
        ArrowKernel: Fn(
            &arrow2::array::Utf8Array<i64>,
            &arrow2::array::Utf8Array<i64>,
        ) -> arrow2::array::BooleanArray,
        ScalarKernel: Fn(&str, &str) -> bool,
    {
        let self_arrow = self.downcast();
        let other_arrow = other.downcast();
        match (self.len(), other.len()) {
            // Matching len case:
            (self_len, other_len) if self_len == other_len => Ok(BooleanArray::from((
                self.name(),
                kernel(self_arrow, other_arrow),
            ))),
            // Broadcast other case:
            (self_len, 1) => {
                let other_scalar_value = other.get(0);
                match other_scalar_value {
                    None => Ok(BooleanArray::full_null(self.name(), self_len)),
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
                    None => Ok(BooleanArray::full_null(self.name(), other_len)),
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
