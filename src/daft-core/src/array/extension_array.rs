use std::sync::Arc;

use common_error::DaftResult;
use daft_schema::{dtype::DataType, field::Field};

use crate::{prelude::ExtensionArray, series::Series};

impl ExtensionArray {
    /// Create an ExtensionArray from a Series whose dtype matches the extension's storage type.
    ///
    /// This handles building the Extension field with the correct Arrow metadata
    /// so callers don't need to manually construct BTreeMap metadata and go through arrow.
    pub fn from_series(
        storage_series: &Series,
        ext_name: &str,
        metadata: Option<&str>,
    ) -> DaftResult<Self> {
        let ext_dtype = DataType::Extension(
            ext_name.to_string(),
            Box::new(storage_series.data_type().clone()),
            metadata.map(|s| s.to_string()),
        );

        let ext_field = Field::new(storage_series.name(), ext_dtype);

        let arrow_data = storage_series.to_arrow()?;
        Self::from_arrow(Arc::new(ext_field), arrow_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        datatypes::{DataType, Float64Array, Int64Array, Utf8Array},
        prelude::ExtensionArray,
        series::IntoSeries,
    };

    #[test]
    fn test_from_series_int64() {
        let storage = Int64Array::from_values("col", vec![1i64, 2, 3].into_iter()).into_series();
        let ext = ExtensionArray::from_series(&storage, "test.counter", None).unwrap();
        assert!(
            matches!(ext.data_type(), DataType::Extension(name, _, None) if name == "test.counter")
        );
        assert_eq!(ext.len(), 3);
    }

    #[test]
    fn test_from_series_utf8_with_metadata() {
        let storage = Utf8Array::from_iter("col", vec![Some("hello"), Some("world")].into_iter())
            .into_series();
        let ext =
            ExtensionArray::from_series(&storage, "test.label", Some(r#"{"lang":"en"}"#)).unwrap();
        match ext.data_type() {
            DataType::Extension(name, _, meta) => {
                assert_eq!(name, "test.label");
                assert_eq!(meta.as_deref(), Some(r#"{"lang":"en"}"#));
            }
            _ => panic!("Expected Extension type"),
        }
        assert_eq!(ext.len(), 2);
    }

    #[test]
    fn test_from_series_float64() {
        let storage = Float64Array::from_values("col", vec![1.5, 2.5].into_iter()).into_series();
        let ext = ExtensionArray::from_series(&storage, "test.measurement", None).unwrap();
        assert_eq!(ext.len(), 2);
        assert!(ext.data_type().is_extension());
    }
}
