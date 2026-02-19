use common_error::{DaftError, DaftResult};

use crate::prelude::{AsArrow, BinaryArray, BooleanArray, FixedSizeBinaryArray, Utf8Array};

impl Utf8Array {
    /// Returns an iterator of `&str` over the non-null values in this array.
    ///
    /// NOTE: this will error if there are any null values.
    /// If you need to handle nulls, use the `.iter()` method instead.
    pub fn values(&self) -> DaftResult<impl Iterator<Item = &str>> {
        let arrow2_arr = self.as_arrow2();
        if self.null_count() > 0 {
            return Err(DaftError::ComputeError(
                "Utf8Array::values with nulls".to_string(),
            ));
        }
        Ok(arrow2_arr.values_iter())
    }

    /// Returns the value at `idx` without bounds or null checks.
    ///
    /// # Safety
    /// Caller must ensure `idx < self.len()` and the element is valid (not null).
    pub(crate) unsafe fn value_unchecked(&self, idx: usize) -> &str {
        // Safety: caller guarantees idx < len and element is valid.
        unsafe { self.as_arrow2().value_unchecked(idx) }
    }
}

impl BinaryArray {
    /// Returns an iterator of `&[u8]` over the non-null values in this array.
    ///
    /// NOTE: this will error if there are any null values.
    /// If you need to handle nulls, use the `.iter()` method instead.
    pub fn values(&self) -> DaftResult<impl Iterator<Item = &[u8]>> {
        let arrow2_arr = self.as_arrow2();
        if self.null_count() > 0 {
            return Err(DaftError::ComputeError(
                "BinaryArray::values with nulls".to_string(),
            ));
        }
        Ok(arrow2_arr.values_iter())
    }

    /// Returns the value at `idx` without bounds or null checks.
    ///
    /// # Safety
    /// Caller must ensure `idx < self.len()` and the element is valid (not null).
    pub(crate) unsafe fn value_unchecked(&self, idx: usize) -> &[u8] {
        // Safety: caller guarantees idx < len and element is valid.
        unsafe { self.as_arrow2().value_unchecked(idx) }
    }
}

impl FixedSizeBinaryArray {
    /// Returns the value at `idx` without bounds or null checks.
    ///
    /// # Safety
    /// Caller must ensure `idx < self.len()` and the element is valid (not null).
    pub(crate) unsafe fn value_unchecked(&self, idx: usize) -> &[u8] {
        // Safety: caller guarantees idx < len and element is valid.
        unsafe { self.as_arrow2().value_unchecked(idx) }
    }
}

impl BooleanArray {
    /// Returns an iterator of `bool` over the non-null values in this array.
    ///
    /// NOTE: this will error if there are any null values.
    /// If you need to handle nulls, use the `.iter()` method instead.
    pub fn values(&self) -> DaftResult<impl Iterator<Item = bool>> {
        let arrow2_arr = self.as_arrow2();
        if self.null_count() > 0 {
            return Err(DaftError::ComputeError(
                "BooleanArray::values with nulls".to_string(),
            ));
        }
        Ok(arrow2_arr.values_iter())
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::Utf8Array;

    #[test]
    fn test_values() {
        let array = Utf8Array::from_slice("test", &["hello", "world"]);
        let values: Vec<&str> = array.values().unwrap().collect();
        assert_eq!(values, vec!["hello", "world"]);
    }

    #[test]
    fn test_values_with_nulls() {
        let array =
            Utf8Array::from_iter("test", vec![Some("hello"), None, Some("world")].into_iter());
        let result = array.values();
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Utf8Array::values with nulls")
        );
    }
}
