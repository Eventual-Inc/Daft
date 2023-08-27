use std::{iter::repeat, sync::Arc};

#[cfg(feature = "python")]
use pyo3::Python;

use crate::{
    array::{pseudo_arrow::PseudoArrowArray, DataArray},
    datatypes::{
        logical::LogicalArray, nested_arrays::FixedSizeListArray, DaftDataType, DaftLogicalType,
        DaftPhysicalType, DataType, Field,
    },
    with_match_daft_types, IntoSeries,
};

pub trait FullNull {
    fn full_null(name: &str, dtype: &DataType, length: usize) -> Self;
    fn empty(name: &str, dtype: &DataType) -> Self;
}

impl<T> FullNull for DataArray<T>
where
    T: DaftPhysicalType,
{
    /// Creates a DataArray<T> of size `length` that is filled with all nulls.
    fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        let field = Field::new(name, dtype.clone());
        #[cfg(feature = "python")]
        if dtype.is_python() {
            let py_none = Python::with_gil(|py: Python| py.None());

            return DataArray::new(
                field.into(),
                Box::new(PseudoArrowArray::from_pyobj_vec(vec![py_none; length])),
            )
            .unwrap();
        }

        let arrow_dtype = dtype.to_arrow();
        match arrow_dtype {
            Ok(arrow_dtype) => DataArray::<T>::new(
                Arc::new(Field::new(name.to_string(), dtype.clone())),
                arrow2::array::new_null_array(arrow_dtype, length),
            )
            .unwrap(),
            Err(e) => panic!("Cannot create DataArray from non-arrow dtype: {e}"),
        }
    }

    fn empty(name: &str, dtype: &DataType) -> Self {
        let field = Field::new(name, dtype.clone());
        #[cfg(feature = "python")]
        if dtype.is_python() {
            return DataArray::new(
                field.into(),
                Box::new(PseudoArrowArray::from_pyobj_vec(vec![])),
            )
            .unwrap();
        }

        let arrow_dtype = dtype.to_arrow();
        match arrow_dtype {
            Ok(arrow_dtype) => DataArray::<T>::new(
                Arc::new(Field::new(name.to_string(), dtype.clone())),
                arrow2::array::new_empty_array(arrow_dtype),
            )
            .unwrap(),
            Err(e) => panic!("Cannot create DataArray from non-arrow dtype: {e}"),
        }
    }
}

impl<L: DaftLogicalType> FullNull for LogicalArray<L>
where
    <L::PhysicalType as DaftDataType>::ArrayType: FullNull,
{
    fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        let physical = <L::PhysicalType as DaftDataType>::ArrayType::full_null(name, dtype, length);
        Self::new(Field::new(name, dtype.clone()), physical)
    }

    fn empty(field_name: &str, dtype: &DataType) -> Self {
        let physical =
            <L::PhysicalType as DaftDataType>::ArrayType::empty(field_name, &dtype.to_physical());
        let field = Field::new(field_name, dtype.clone());
        Self::new(field, physical)
    }
}

impl FullNull for FixedSizeListArray {
    fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        let empty = Self::empty(name, dtype);
        let validity = arrow2::bitmap::Bitmap::from_iter(repeat(false).take(length));
        Self::new(empty.field, empty.flat_child, Some(validity))
    }

    fn empty(name: &str, dtype: &DataType) -> Self {
        match dtype {
            DataType::FixedSizeList(child, _) => {
                let field = Field::new(name, dtype.clone());
                let empty_child = with_match_daft_types!(&child.dtype, |$T| {
                    <$T as DaftDataType>::ArrayType::empty(name, &child.dtype).into_series()
                });
                Self::new(field, empty_child, None)
            }
            _ => panic!(
                "Cannot create empty FixedSizeListArray with dtype: {}",
                dtype
            ),
        }
    }
}
