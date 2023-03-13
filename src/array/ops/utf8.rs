use arrow2::array::{BooleanArray, Utf8Array};

use crate::error::{DaftError, DaftResult};

#[allow(dead_code)] //TODO: remove-me
pub fn endswith_utf8_arrays(
    data: &Utf8Array<i64>,
    pattern: &Utf8Array<i64>,
) -> DaftResult<BooleanArray> {
    match (data.len(), pattern.len()) {
        // Broadcast case:
        (data_len, 1) => match pattern.validity() {
            Some(validity) if !validity.get_bit(0) => Ok(BooleanArray::new_null(
                arrow2::datatypes::DataType::Boolean,
                data_len,
            )),
            _ => {
                let pattern_val = pattern.value(0);
                Ok(data
                    .into_iter()
                    .map(|val| Some(val?.ends_with(pattern_val)))
                    .collect())
            }
        },
        // Mismatched len case:
        (data_len, pattern_len) if data_len != pattern_len => Err(DaftError::ComputeError(
            format!("lhs and rhs have different length arrays: {data_len} vs {pattern_len}"),
        )),
        // Matching len case:
        _ => Ok(data
            .into_iter()
            .zip(pattern.into_iter())
            .map(|(val, pat)| Some(val?.ends_with(pat?)))
            .collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_endswith_utf_arrays_broadcast() -> DaftResult<()> {
        let data = Utf8Array::<i64>::from(vec!["x_foo".into(), "y_foo".into(), "z_bar".into()]);
        let pattern = Utf8Array::<i64>::from(vec!["foo".into()]);
        let result = endswith_utf8_arrays(&data, &pattern)?;
        assert_eq!(result.len(), 3);
        assert!(result.value(0));
        assert!(result.value(1));
        assert!(!result.value(2));
        Ok(())
    }

    #[test]
    fn check_endswith_utf_arrays() -> DaftResult<()> {
        let data = Utf8Array::<i64>::from(vec!["x_foo".into(), "y_foo".into(), "z_bar".into()]);
        let pattern = Utf8Array::<i64>::from(vec!["foo".into(), "wrong".into(), "bar".into()]);
        let result = endswith_utf8_arrays(&data, &pattern)?;
        assert_eq!(result.len(), 3);
        assert!(result.value(0));
        assert!(!result.value(1));
        assert!(result.value(2));
        Ok(())
    }
}
