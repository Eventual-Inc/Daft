use arrow::datatypes::ArrowNativeType;
use common_error::{DaftError, DaftResult};

use crate::prelude::{AsArrow, Utf8Array};

impl Utf8Array {
    /// Convert the Utf8Array into Vec<String>
    ///
    /// NOTE: this will error if there are any null values
    /// If you need to preserve the null values, use .iter() instead
    pub fn into_values(self) -> DaftResult<Vec<String>> {
        let arrow_arr = self.as_arrow()?;

        let (offsets, data, None) = arrow_arr.into_parts() else {
            return Err(DaftError::ComputeError(
                "Utf8Array::into_values with nulls".to_string(),
            ));
        };

        let data_bytes = data.as_slice();

        Ok((0..offsets.len() - 1)
            .map(|i| {
                let start = offsets[i].as_usize();
                let end = offsets[i + 1].as_usize();
                std::str::from_utf8(&data_bytes[start..end])
                    .expect("arrow should guarantee valid UTF-8")
                    .to_string()
            })
            .collect::<Vec<String>>())
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::Utf8Array;

    #[test]
    fn test_into_values() {
        let array = Utf8Array::from_slice("test", &["hello", "world"]);
        let values = array.into_values().unwrap();
        assert_eq!(values, vec!["hello", "world"]);
    }

    #[test]
    fn test_into_values_with_nulls() {
        let array =
            Utf8Array::from_iter("test", vec![Some("hello"), None, Some("world")].into_iter());
        let values = array.into_values().unwrap_err();
        assert!(
            values
                .to_string()
                .contains("Utf8Array::into_values with nulls")
        );
    }
}
