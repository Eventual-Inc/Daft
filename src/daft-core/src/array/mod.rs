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
pub mod utf8;

use arrow::{array::make_array, compute::cast};
use daft_arrow::{
    array::to_data,
    buffer::{NullBuffer, wrap_null_buffer},
    compute::cast::utf8_to_large_utf8,
};
pub use fixed_size_list_array::FixedSizeListArray;
pub use list_array::ListArray;
pub use struct_array::StructArray;
mod boolean;
mod from_iter;
pub mod prelude;
use std::{marker::PhantomData, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_schema::field::{DaftField, FieldRef};

use crate::datatypes::{DaftArrayType, DaftPhysicalType, DataType, Field};

#[derive(Debug)]
pub struct DataArray<T> {
    pub field: Arc<Field>,
    data: Box<dyn daft_arrow::array::Array>,
    nulls: Option<daft_arrow::buffer::NullBuffer>,
    marker_: PhantomData<T>,
}

impl<T: DaftPhysicalType> Clone for DataArray<T> {
    fn clone(&self) -> Self {
        Self::new(self.field.clone(), self.data.clone()).unwrap()
    }
}

impl<T: DaftPhysicalType> DaftArrayType for DataArray<T> {
    fn data_type(&self) -> &DataType {
        &self.field.as_ref().dtype
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
            // let maybe_coerced = physical_field.
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
            data: arrow_arr.into(),
            nulls,
            marker_: PhantomData,
        })
    }

    pub fn new(
        physical_field: Arc<DaftField>,
        arrow_array: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        assert!(
            physical_field.dtype.is_physical(),
            "Can only construct DataArray for PhysicalTypes, got {}",
            physical_field.dtype
        );

        if let Ok(expected_arrow_physical_type) = physical_field.dtype.to_arrow2() {
            // since daft's Utf8 always maps to Arrow's LargeUtf8, we need to handle this special case
            // If the expected physical type is LargeUtf8, but the actual Arrow type is Utf8, we need to convert it
            if expected_arrow_physical_type == daft_arrow::datatypes::DataType::LargeUtf8
                && arrow_array.data_type() == &daft_arrow::datatypes::DataType::Utf8
            {
                let utf8_arr = arrow_array
                    .as_any()
                    .downcast_ref::<daft_arrow::array::Utf8Array<i32>>()
                    .unwrap();

                let arr = Box::new(utf8_to_large_utf8(utf8_arr));
                let nulls = arr.validity().cloned().map(Into::into);
                return Ok(Self {
                    field: physical_field,
                    data: arr,
                    nulls,
                    marker_: PhantomData,
                });
            }
            let arrow_data_type = arrow_array.data_type();
            if !matches!(physical_field.dtype, DataType::Extension(..)) {
                assert!(
                    !(&expected_arrow_physical_type != arrow_data_type),
                    "Mismatch between expected and actual Arrow types for DataArray.\n\
                    Field name: '{}'\n\
                    Logical type: {}\n\
                    Physical type: {}\n\
                    Expected Arrow physical type: {:?}\n\
                    Actual Arrow Logical type: {:?}

                    This error typically occurs when there's a discrepancy between the Daft DataType \
                    and the underlying Arrow representation. Please ensure that the physical type \
                    of the Daft DataType matches the Arrow type of the provided data.",
                    physical_field.name,
                    physical_field.dtype,
                    physical_field.dtype.to_physical(),
                    expected_arrow_physical_type,
                    arrow_data_type
                );
            }
        }

        let nulls = arrow_array.validity().cloned().map(Into::into);
        Ok(Self {
            field: physical_field,
            data: arrow_array,
            nulls,
            marker_: PhantomData,
        })
    }

    pub fn len(&self) -> usize {
        self.data().len()
    }

    pub fn null_count(&self) -> usize {
        self.data().null_count()
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn with_nulls_slice(&self, nulls: &[bool]) -> DaftResult<Self> {
        if nulls.len() != self.data.len() {
            return Err(DaftError::ValueError(format!(
                "nulls length does not match DataArray length, {} vs {}",
                nulls.len(),
                self.data.len()
            )));
        }
        let with_bitmap = self
            .data
            .with_validity(wrap_null_buffer(Some(NullBuffer::from(nulls))));
        Self::new(self.field.clone(), with_bitmap)
    }

    pub fn with_nulls(&self, nulls: Option<NullBuffer>) -> DaftResult<Self> {
        if let Some(v) = &nulls
            && v.len() != self.data.len()
        {
            return Err(DaftError::ValueError(format!(
                "validity mask length does not match DataArray length, {} vs {}",
                v.len(),
                self.data.len()
            )));
        }
        let with_bitmap = self.data.with_validity(wrap_null_buffer(nulls));
        Self::new(self.field.clone(), with_bitmap)
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
        let sliced = self.data.sliced(start, end - start);
        Self::new(self.field.clone(), sliced)
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        self.slice(0, num)
    }

    pub fn data(&self) -> &dyn daft_arrow::array::Array {
        self.data.as_ref()
    }

    pub fn to_data(&self) -> arrow::array::ArrayData {
        to_data(self.data())
    }

    pub fn to_arrow(&self) -> arrow::array::ArrayRef {
        make_array(self.to_data())
    }

    pub fn name(&self) -> &str {
        self.field.name.as_str()
    }

    pub fn rename(&self, name: &str) -> Self {
        Self::new(Arc::new(self.field.rename(name)), self.data.clone()).unwrap()
    }

    pub fn field(&self) -> &Field {
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
