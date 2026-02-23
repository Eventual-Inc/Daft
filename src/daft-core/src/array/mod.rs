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

        if let Ok(expected_arrow_physical_type) = physical_field.dtype.to_arrow() {
            // since daft's Utf8 always maps to Arrow's LargeUtf8, we need to handle this special case
            // If the expected physical type is LargeUtf8, but the actual Arrow type is Utf8, we need to convert it
            if expected_arrow_physical_type == arrow::datatypes::DataType::LargeUtf8
                && arrow_arr.data_type() == &arrow::datatypes::DataType::Utf8
            {
                let arr = cast(arrow_arr.as_ref(), &arrow::datatypes::DataType::LargeUtf8)?;
                let nulls = arr.nulls().cloned().map(Into::into);

                return Ok(Self {
                    field: physical_field,
                    data: arr.into(),
                    nulls,
                    marker_: PhantomData,
                });
            }
        }

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
}
