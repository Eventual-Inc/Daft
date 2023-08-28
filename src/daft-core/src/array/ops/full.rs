use std::{iter::repeat, sync::Arc};

#[cfg(feature = "python")]
use pyo3::Python;

use crate::{
    array::{pseudo_arrow::PseudoArrowArray, DataArray, FixedSizeListArray},
    datatypes::{
        logical::LogicalArray, DaftDataType, DaftLogicalType, DaftPhysicalType, DataType, Field,
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
        let validity = arrow2::bitmap::Bitmap::from_iter(repeat(false).take(length));

        match dtype {
            DataType::FixedSizeList(child, size) => {
                let flat_child = with_match_daft_types!(&child.dtype, |$T| {
                    <<$T as DaftDataType>::ArrayType as FullNull>::full_null(name, &child.dtype, length * size).into_series()
                });
                Self::new(Field::new(name, dtype.clone()), flat_child, Some(validity))
            }
            _ => panic!(
                "Cannot create FixedSizeListArray::full_null from datatype: {}",
                dtype
            ),
        }
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

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::{
        array::{ops::full::FullNull, FixedSizeListArray},
        datatypes::Field,
        DataType,
    };

    #[test]
    fn create_fixed_size_list_full_null() -> DaftResult<()> {
        let arr = FixedSizeListArray::full_null(
            "foo",
            &DataType::FixedSizeList(Box::new(Field::new("foo", DataType::Int64)), 3),
            3,
        );
        assert_eq!(arr.len(), 3);
        assert!(!arr.is_valid(0));
        assert!(!arr.is_valid(1));
        assert!(!arr.is_valid(2));
        Ok(())
    }

    #[test]
    fn create_fixed_size_list_full_null_empty() -> DaftResult<()> {
        let arr = FixedSizeListArray::full_null(
            "foo",
            &DataType::FixedSizeList(Box::new(Field::new("foo", DataType::Int64)), 3),
            0,
        );
        assert_eq!(arr.len(), 0);
        Ok(())
    }

    #[test]
    fn create_fixed_size_list_empty() -> DaftResult<()> {
        let arr = FixedSizeListArray::empty(
            "foo",
            &DataType::FixedSizeList(Box::new(Field::new("foo", DataType::Int64)), 3),
        );
        assert_eq!(arr.len(), 0);
        Ok(())
    }
}
