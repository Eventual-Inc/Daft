pub mod file_array;
mod fixed_size_list_array;
pub mod from;
pub mod growable;
pub mod image_array;
pub mod iterator;
mod list_array;
pub mod ops;
mod serdes;
mod struct_array;
pub mod values;

use arrow::{
    array::{ArrayRef, ArrowPrimitiveType, make_array},
    buffer::{NullBuffer, ScalarBuffer},
    compute::cast,
};
pub use fixed_size_list_array::FixedSizeListArray;
pub use list_array::ListArray;
pub use struct_array::StructArray;
mod boolean;
pub mod prelude;
use std::{marker::PhantomData, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_schema::field::FieldRef;

use crate::{
    datatypes::{
        DaftArrayType, DaftPhysicalType, DaftPrimitiveType, DataType, Field, NumericNative,
    },
    prelude::AsArrow,
};

#[derive(Debug)]
pub struct DataArray<T> {
    pub field: Arc<Field>,
    data: ArrayRef,
    nulls: Option<NullBuffer>,
    marker_: PhantomData<T>,
}

impl<T: DaftPhysicalType> Clone for DataArray<T> {
    fn clone(&self) -> Self {
        Self {
            field: self.field.clone(),
            data: self.data.clone(),
            nulls: self.nulls.clone(),
            marker_: PhantomData,
        }
    }
}

impl<T: DaftPhysicalType> DaftArrayType for DataArray<T> {
    fn data_type(&self) -> &DataType {
        &self.field.as_ref().dtype
    }
}
impl<T: DaftPrimitiveType> DataArray<T> {
    /// Compile-time proof that `T::Native` and the arrow-rs `ARROWTYPE::Native` are
    /// the same size and alignment.
    const fn native_layout_assert() {
        assert!(
            std::mem::size_of::<T::Native>()
                == std::mem::size_of::<
                    <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native,
                >(),
            "T::Native and ARROWTYPE::Native must have the same size"
        );
        assert!(
            std::mem::align_of::<T::Native>()
                == std::mem::align_of::<
                    <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native,
                >(),
            "T::Native and ARROWTYPE::Native must have the same alignment"
        );
    }

    pub fn as_slice(&self) -> &[T::Native] {
        Self::native_layout_assert();

        let original_slice = self.as_arrow().unwrap().values().as_ref();
        // SAFETY: T::Native and <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native
        // are the same type in practice (identical memory layout), but the compiler can't prove this
        // through the trait indirection. This cast is a no-op at runtime.
        let slice: &[T::Native] = unsafe {
            std::slice::from_raw_parts(
                original_slice.as_ptr().cast::<T::Native>(),
                original_slice.len(),
            )
        };
        slice
    }

    pub fn values(&self) -> ScalarBuffer<T::Native> {
        Self::native_layout_assert();

        let arr = self.as_arrow().unwrap();
        let buffer = arr.values().inner();
        unsafe { ScalarBuffer::<T::Native>::new_unchecked(buffer.clone()) }
    }
}

impl<T> DataArray<T> {
    pub fn from_arrow<F: Into<FieldRef>>(
        field: F,
        arrow_arr: arrow::array::ArrayRef,
    ) -> DaftResult<Self> {
        let physical_field = field.into();

        assert!(
            physical_field.dtype.is_physical(),
            "Can only construct DataArray for PhysicalTypes, got {}",
            physical_field.dtype
        );

        // Validate and optionally auto-cast the arrow array to match Daft's expected
        // physical type. A coercion is valid when the actual arrow type round-trips
        // through Daft's type system to the expected arrow type (e.g. arrow Utf8 →
        // Daft Utf8 → arrow LargeUtf8). Any other mismatch is an error.
        // For Extension types, use the inner storage dtype since Extension itself
        // doesn't have a direct arrow mapping (the registry stores the original type for export).
        let expected = match &physical_field.dtype {
            DataType::Extension(_, inner, _) => inner.to_arrow().ok(),
            dt => dt.to_arrow().ok(),
        };
        let arrow_arr = if let Some(expected) = expected {
            let actual = arrow_arr.data_type();
            if expected != *actual {
                // Check if the actual arrow type is a valid Daft coercion:
                // convert actual → Daft DataType → arrow. If it matches expected,
                // this is a known coercion (e.g. Utf8→LargeUtf8, Binary→LargeBinary).
                let is_coercible = DataType::try_from(actual)
                    .and_then(|dt| dt.to_arrow())
                    .is_ok_and(|roundtripped| roundtripped == expected);

                if is_coercible {
                    cast(arrow_arr.as_ref(), &expected)
                        .map_err(|e| {
                            DaftError::TypeError(format!(
                                "Failed to auto-cast arrow array from {:?} to {:?} for field '{}' ({}): {}",
                                actual, expected, physical_field.name, physical_field.dtype, e,
                            ))
                        })?
                        .into()
                } else {
                    return Err(DaftError::TypeError(format!(
                        "Arrow array type mismatch for field '{}': expected {:?} but got {:?}",
                        physical_field.name, expected, actual,
                    )));
                }
            } else {
                arrow_arr
            }
        } else {
            arrow_arr
        };

        let nulls = arrow_arr.nulls().cloned().map(Into::into);
        Ok(Self {
            field: physical_field,
            data: arrow_arr,
            nulls,
            marker_: PhantomData,
        })
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn null_count(&self) -> usize {
        self.data.logical_null_count()
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn with_nulls_slice(&self, nulls: &[bool]) -> DaftResult<Self> {
        if nulls.len() != self.len() {
            return Err(DaftError::ValueError(format!(
                "nulls length does not match DataArray length, {} vs {}",
                nulls.len(),
                self.len()
            )));
        }
        self.with_nulls(Some(NullBuffer::from(nulls)))
    }

    pub fn with_nulls(&self, nulls: Option<NullBuffer>) -> DaftResult<Self> {
        if let Some(v) = &nulls
            && v.len() != self.len()
        {
            return Err(DaftError::ValueError(format!(
                "validity mask length does not match DataArray length, {} vs {}",
                v.len(),
                self.len()
            )));
        }
        let array_data = self.data.to_data();
        let mut builder = array_data.into_builder();
        builder = builder.nulls(nulls);

        // SAFETY: we only are changing the null mask so this is safe
        let data = unsafe { builder.build_unchecked() };
        let arr = make_array(data);

        Self::from_arrow(self.field.clone(), arr)
    }

    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        if start > end {
            return Err(DaftError::ValueError(format!(
                "Trying to slice array with negative length, start: {start} vs end: {end}"
            )));
        }

        let sliced = self.data.slice(start, end - start);
        Self::from_arrow(self.field.clone(), sliced)
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        self.slice(0, num)
    }

    pub fn to_data(&self) -> arrow::array::ArrayData {
        self.data.to_data()
    }

    pub fn to_arrow(&self) -> arrow::array::ArrayRef {
        self.data.clone()
    }

    pub fn name(&self) -> &str {
        self.field.name.as_str()
    }

    pub fn rename(&self, name: &str) -> Self {
        Self {
            field: Arc::new(self.field.rename(name)),
            data: self.data.clone(),
            nulls: self.nulls.clone(),
            marker_: self.marker_,
        }
    }

    pub fn field(&self) -> &FieldRef {
        &self.field
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use daft_schema::{dtype::DataType, field::Field};

    use crate::series::Series;

    #[test]
    fn from_small_utf8_arrow() {
        let data = vec![Some("hello"), Some("world")];
        let data = Arc::new(StringArray::from_iter(data.into_iter()));
        let daft_fld = Arc::new(Field::new("test", DataType::Utf8));

        let s = Series::from_arrow(daft_fld, data);
        assert!(s.is_ok());
        assert_eq!(s.unwrap().data_type(), &DataType::Utf8);
    }

    #[test]
    fn from_small_binary_arrow() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"hello"), Some(b"world")];
        let data = Arc::new(arrow::array::BinaryArray::from_iter(data.into_iter()));
        let daft_fld = Arc::new(Field::new("test", DataType::Binary));

        let s = Series::from_arrow(daft_fld, data);
        assert!(s.is_ok());
        assert_eq!(s.unwrap().data_type(), &DataType::Binary);
    }

    #[test]
    fn from_small_list_arrow() {
        use arrow::{
            array::{Int32Array, ListArray as ArrowListArray},
            buffer::OffsetBuffer,
        };

        // Create a List (i32 offsets) array: [[1, 2], [3]]
        let values = Int32Array::from(vec![1, 2, 3]);
        let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
        let list = ArrowListArray::new(
            Arc::new(arrow::datatypes::Field::new(
                "item",
                arrow::datatypes::DataType::Int32,
                true,
            )),
            offsets,
            Arc::new(values),
            None,
        );
        let daft_fld = Arc::new(Field::new(
            "test",
            DataType::List(Box::new(DataType::Int32)),
        ));

        let s = Series::from_arrow(daft_fld, Arc::new(list));
        assert!(s.is_ok());
        let s = s.unwrap();
        assert_eq!(s.data_type(), &DataType::List(Box::new(DataType::Int32)));
        assert_eq!(s.len(), 2);
    }

    /// Helper: build a Daft Field from an arrow field with extension metadata.
    /// This populates the EXTENSION_TYPE_REGISTRY so to_arrow() can reverse coercion.
    fn ext_field(
        name: &str,
        ext_name: &str,
        storage_type: arrow::datatypes::DataType,
    ) -> Arc<Field> {
        use std::collections::HashMap;

        let mut metadata = HashMap::new();
        metadata.insert("ARROW:extension:name".to_string(), ext_name.to_string());
        let arrow_field =
            arrow::datatypes::Field::new(name, storage_type, true).with_metadata(metadata);
        Arc::new(Field::try_from(&arrow_field).unwrap())
    }

    #[test]
    fn from_small_list_u64_to_large_list_u64() {
        use arrow::{
            array::{ListArray as ArrowListArray, UInt64Array},
            buffer::OffsetBuffer,
        };

        let values = UInt64Array::from(vec![10u64, 20, 30]);
        let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
        let list = ArrowListArray::new(
            Arc::new(arrow::datatypes::Field::new(
                "item",
                arrow::datatypes::DataType::UInt64,
                true,
            )),
            offsets,
            Arc::new(values),
            None,
        );
        let daft_fld = Arc::new(Field::new(
            "test",
            DataType::List(Box::new(DataType::UInt64)),
        ));

        let s = Series::from_arrow(daft_fld, Arc::new(list));
        assert!(s.is_ok());
        let s = s.unwrap();
        assert_eq!(s.data_type(), &DataType::List(Box::new(DataType::UInt64)));
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn from_small_list_u64_rejects_large_list_u32() {
        use arrow::{
            array::{ListArray as ArrowListArray, UInt64Array},
            buffer::OffsetBuffer,
        };

        // Build a List<UInt64> arrow array
        let values = UInt64Array::from(vec![10u64, 20, 30]);
        let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
        let list = ArrowListArray::new(
            Arc::new(arrow::datatypes::Field::new(
                "item",
                arrow::datatypes::DataType::UInt64,
                true,
            )),
            offsets,
            Arc::new(values),
            None,
        );

        // Try to load it as List<UInt32> — inner types don't match, should fail
        let daft_fld = Arc::new(Field::new(
            "test",
            DataType::List(Box::new(DataType::UInt32)),
        ));

        let result = Series::from_arrow(daft_fld, Arc::new(list));
        assert!(
            result.is_err(),
            "Expected type mismatch error for List<u64> → List<u32>"
        );
    }

    #[test]
    fn extension_binary_roundtrip() {
        // Extension with Binary storage: Binary arrow array → from_arrow (casts to LargeBinary)
        // → to_arrow (casts back to Binary)
        let data: Vec<Option<&[u8]>> = vec![Some(b"foo"), None, Some(b"bar")];
        let arr = Arc::new(arrow::array::BinaryArray::from_iter(data.into_iter()));
        let field = ext_field("test", "test_ext_bin", arrow::datatypes::DataType::Binary);

        let s = Series::from_arrow(field, arr).unwrap();
        assert!(matches!(s.data_type(), DataType::Extension(..)));

        // to_arrow should reverse the coercion back to Binary
        let out = s.to_arrow().unwrap();
        assert_eq!(out.data_type(), &arrow::datatypes::DataType::Binary);
        assert_eq!(out.len(), 3);
        assert!(out.is_null(1));
    }

    #[test]
    fn extension_utf8_roundtrip() {
        // Extension with Utf8 storage: Utf8 arrow array → from_arrow (casts to LargeUtf8)
        // → to_arrow (casts back to Utf8)
        let arr = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
        let field = ext_field("test", "test_ext_str", arrow::datatypes::DataType::Utf8);

        let s = Series::from_arrow(field, arr).unwrap();
        assert!(matches!(s.data_type(), DataType::Extension(..)));

        let out = s.to_arrow().unwrap();
        assert_eq!(out.data_type(), &arrow::datatypes::DataType::Utf8);
        assert_eq!(out.len(), 2);
    }
}
