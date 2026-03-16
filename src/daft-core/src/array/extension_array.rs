use std::sync::Arc;

use arrow::{array::ArrayRef, buffer::NullBuffer};
use common_error::DaftResult;
use daft_schema::{dtype::DataType, field::Field};

use crate::{datatypes::DaftArrayType, series::Series};

#[derive(Clone, Debug)]
pub struct ExtensionArray {
    field: Arc<Field>,
    /// Extension type name (e.g. "geoarrow.point")
    extension_name: Arc<str>,
    /// Extension metadata (e.g. '{"crs": "WGS84"}')
    metadata: Option<Arc<str>>,
    /// The underlying storage data
    pub physical: Series,
}

impl ExtensionArray {
    pub fn new(field: Arc<Field>, physical: Series) -> Self {
        let DataType::Extension(ext_name, _, ext_metadata) = &field.dtype else {
            panic!(
                "ExtensionArray field must have Extension dtype, got {}",
                field.dtype
            );
        };
        Self {
            extension_name: Arc::from(ext_name.as_str()),
            metadata: ext_metadata.as_deref().map(Arc::from),
            field,
            physical,
        }
    }

    pub fn name(&self) -> &str {
        self.field.name.as_ref()
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    pub fn extension_name(&self) -> &str {
        &self.extension_name
    }

    pub fn extension_metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn len(&self) -> usize {
        self.physical.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn rename(&self, name: &str) -> Self {
        Self {
            field: Arc::new(Field::new(name, self.field.dtype.clone())),
            extension_name: self.extension_name.clone(),
            metadata: self.metadata.clone(),
            physical: self.physical.rename(name),
        }
    }

    /// Replace the underlying physical `Series` of this `ExtensionArray`.
    pub fn with_physical(&self, physical: Series) -> Self {
        Self {
            field: self.field.clone(),
            extension_name: self.extension_name.clone(),
            metadata: self.metadata.clone(),
            physical,
        }
    }

    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.physical.inner.nulls()
    }

    pub fn to_arrow(&self) -> DaftResult<ArrayRef> {
        let arr = self.physical.to_arrow()?;
        let target_field = self.field.to_arrow()?;
        if arr.data_type() != target_field.data_type() {
            Ok(arrow::compute::cast(&arr, target_field.data_type())?)
        } else {
            Ok(arr)
        }
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        Ok(self.with_physical(self.physical.slice(start, end)?))
    }

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(common_error::DaftError::ValueError(
                "Cannot concat empty list of ExtensionArrays".to_string(),
            ));
        }
        let first = arrays[0];
        let physical_arrays: Vec<&Series> = arrays.iter().map(|a| &a.physical).collect();
        let physical = Series::concat(&physical_arrays)?;
        Ok(first.with_physical(physical))
    }
}

impl DaftArrayType for ExtensionArray {
    fn data_type(&self) -> &DataType {
        &self.field.dtype
    }
}

impl crate::array::ops::from_arrow::FromArrow for ExtensionArray {
    fn from_arrow<F: Into<daft_schema::field::FieldRef>>(
        field: F,
        arrow_arr: ArrayRef,
    ) -> DaftResult<Self> {
        let field: daft_schema::field::FieldRef = field.into();
        let DataType::Extension(_, storage_type, _) = &field.dtype else {
            return Err(common_error::DaftError::TypeError(format!(
                "Expected Extension dtype for ExtensionArray, got {}",
                field.dtype
            )));
        };
        let storage_field = Arc::new(Field::new(field.name.as_ref(), *storage_type.clone()));
        let physical = Series::from_arrow(storage_field, arrow_arr)?;
        Ok(Self::new(field, physical))
    }
}

impl crate::array::ops::full::FullNull for ExtensionArray {
    fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        let DataType::Extension(_, storage_type, _) = dtype else {
            panic!("Expected Extension dtype for ExtensionArray::full_null, got {dtype}");
        };
        let physical = Series::full_null(name, storage_type, length);
        Self::new(Arc::new(Field::new(name, dtype.clone())), physical)
    }

    fn empty(name: &str, dtype: &DataType) -> Self {
        let DataType::Extension(_, storage_type, _) = dtype else {
            panic!("Expected Extension dtype for ExtensionArray::empty, got {dtype}");
        };
        let physical = Series::empty(name, storage_type);
        Self::new(Arc::new(Field::new(name, dtype.clone())), physical)
    }
}
