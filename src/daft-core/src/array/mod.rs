mod fixed_size_list_array;
pub mod from;
pub mod growable;
pub mod image_array;
pub mod iterator;
mod list_array;
pub mod ops;
pub mod pseudo_arrow;
mod serdes;
mod struct_array;
use arrow2::{bitmap::Bitmap, compute::cast::utf8_to_large_utf8};
pub use fixed_size_list_array::FixedSizeListArray;
pub use list_array::ListArray;
pub use struct_array::StructArray;
mod boolean;
mod from_iter;
pub mod prelude;
use std::{marker::PhantomData, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_schema::field::DaftField;

use crate::datatypes::{DaftArrayType, DaftPhysicalType, DataType, Field};

#[derive(Debug)]
pub struct DataArray<T> {
    pub field: Arc<Field>,
    pub data: Box<dyn arrow2::array::Array>,
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
    pub fn new(
        physical_field: Arc<DaftField>,
        arrow_array: Box<dyn arrow2::array::Array>,
    ) -> DaftResult<Self> {
        assert!(
            physical_field.dtype.is_physical(),
            "Can only construct DataArray for PhysicalTypes, got {}",
            physical_field.dtype
        );

        if let Ok(expected_arrow_physical_type) = physical_field.dtype.to_arrow() {
            // since daft's Utf8 always maps to Arrow's LargeUtf8, we need to handle this special case
            // If the expected physical type is LargeUtf8, but the actual Arrow type is Utf8, we need to convert it
            if expected_arrow_physical_type == arrow2::datatypes::DataType::LargeUtf8
                && arrow_array.data_type() == &arrow2::datatypes::DataType::Utf8
            {
                let utf8_arr = arrow_array
                    .as_any()
                    .downcast_ref::<arrow2::array::Utf8Array<i32>>()
                    .unwrap();

                let arr = Box::new(utf8_to_large_utf8(utf8_arr));

                return Ok(Self {
                    field: physical_field,
                    data: arr,
                    marker_: PhantomData,
                });
            }
            let arrow_data_type = arrow_array.data_type();

            assert!(
                !(&expected_arrow_physical_type != arrow_data_type),
                "Mismatch between expected and actual Arrow types for DataArray.\n\
                Field name: {}\n\
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

        Ok(Self {
            field: physical_field,
            data: arrow_array,
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

    pub fn with_validity_slice(&self, validity: &[bool]) -> DaftResult<Self> {
        if validity.len() != self.data.len() {
            return Err(DaftError::ValueError(format!(
                "validity mask length does not match DataArray length, {} vs {}",
                validity.len(),
                self.data.len()
            )));
        }
        let with_bitmap = self.data.with_validity(Some(Bitmap::from(validity)));
        Self::new(self.field.clone(), with_bitmap)
    }

    pub fn with_validity(&self, validity: Option<Bitmap>) -> DaftResult<Self> {
        if let Some(v) = &validity
            && v.len() != self.data.len()
        {
            return Err(DaftError::ValueError(format!(
                "validity mask length does not match DataArray length, {} vs {}",
                v.len(),
                self.data.len()
            )));
        }
        let with_bitmap = self.data.with_validity(validity);
        Self::new(self.field.clone(), with_bitmap)
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.data.validity()
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

    pub fn data(&self) -> &dyn arrow2::array::Array {
        self.data.as_ref()
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

impl<T> DataArray<T>
where
    T: DaftPhysicalType + 'static,
{
    pub fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_schema::{dtype::DataType, field::Field};

    use crate::series::Series;

    #[test]
    fn from_small_utf8_arrow() {
        let data = vec![Some("hello"), Some("world")];
        let data = Box::new(arrow2::array::Utf8Array::<i32>::from(data.as_slice()));
        let daft_fld = Arc::new(Field::new("test", DataType::Utf8));

        let s = Series::from_arrow(daft_fld, data);
        assert!(s.is_ok())
    }
}
