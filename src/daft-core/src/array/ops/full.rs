use std::{iter::repeat, sync::Arc};

use arrow2::offset::OffsetsBuffer;
#[cfg(feature = "python")]
use pyo3::Python;

use crate::{
    array::{
        pseudo_arrow::PseudoArrowArray, DataArray, FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::{
        logical::LogicalArray, DaftDataType, DaftLogicalType, DaftPhysicalType, DataType, Field,
    },
    Series,
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
            DataType::FixedSizeList(child_dtype, size) => {
                let flat_child = Series::full_null("item", child_dtype, length * size);
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
            DataType::FixedSizeList(child_dtype, _) => {
                let field = Field::new(name, dtype.clone());
                let empty_child = Series::empty("item", child_dtype.as_ref());
                Self::new(field, empty_child, None)
            }
            _ => panic!(
                "Cannot create empty FixedSizeListArray with dtype: {}",
                dtype
            ),
        }
    }
}

impl FullNull for ListArray {
    fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        let validity = arrow2::bitmap::Bitmap::from_iter(repeat(false).take(length));

        match dtype {
            DataType::List(child_dtype) => {
                let empty_flat_child = Series::empty("list", child_dtype.as_ref());
                Self::new(
                    Field::new(name, dtype.clone()),
                    empty_flat_child,
                    OffsetsBuffer::try_from(repeat(0).take(length + 1).collect::<Vec<_>>())
                        .unwrap(),
                    Some(validity),
                )
            }
            _ => panic!(
                "Cannot create ListArray::full_null from datatype: {}",
                dtype
            ),
        }
    }

    fn empty(name: &str, dtype: &DataType) -> Self {
        match dtype {
            DataType::List(child_dtype) => {
                let field = Field::new(name, dtype.clone());
                let empty_child = Series::empty("list", child_dtype.as_ref());
                Self::new(field, empty_child, OffsetsBuffer::default(), None)
            }
            _ => panic!("Cannot create empty ListArray with dtype: {}", dtype),
        }
    }
}

impl FullNull for StructArray {
    fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        let validity = arrow2::bitmap::Bitmap::from_iter(repeat(false).take(length));
        match dtype {
            DataType::Struct(children) => {
                let field = Field::new(name, dtype.clone());
                let empty_children = children
                    .iter()
                    .map(|f| Series::full_null(f.name.as_str(), &f.dtype, length))
                    .collect::<Vec<_>>();
                Self::new(field, empty_children, Some(validity))
            }
            _ => panic!("Cannot create empty StructArray with dtype: {}", dtype),
        }
    }

    fn empty(name: &str, dtype: &DataType) -> Self {
        match dtype {
            DataType::Struct(children) => {
                let field = Field::new(name, dtype.clone());
                let empty_children = children
                    .iter()
                    .map(|f| Series::empty(f.name.as_str(), &f.dtype))
                    .collect::<Vec<_>>();
                Self::new(field, empty_children, None)
            }
            _ => panic!("Cannot create empty StructArray with dtype: {}", dtype),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::{
        array::{ops::full::FullNull, FixedSizeListArray, StructArray},
        datatypes::Field,
        DataType,
    };

    #[test]
    fn create_fixed_size_list_full_null() -> DaftResult<()> {
        let arr = FixedSizeListArray::full_null(
            "foo",
            &DataType::FixedSizeList(Box::new(DataType::Int64), 3),
            3,
        );
        assert_eq!(arr.len(), 3);
        assert!(!arr.is_valid(0));
        assert!(!arr.is_valid(1));
        assert!(!arr.is_valid(2));
        Ok(())
    }

    #[test]
    fn create_struct_full_null() -> DaftResult<()> {
        let arr = StructArray::full_null(
            "foo",
            &DataType::Struct(vec![Field::new("bar", DataType::Int64)]),
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
            &DataType::FixedSizeList(Box::new(DataType::Int64), 3),
            0,
        );
        assert_eq!(arr.len(), 0);
        Ok(())
    }

    #[test]
    fn create_struct_full_null_empty() -> DaftResult<()> {
        let arr = StructArray::full_null(
            "foo",
            &DataType::Struct(vec![Field::new("bar", DataType::Int64)]),
            0,
        );
        assert_eq!(arr.len(), 0);
        Ok(())
    }

    #[test]
    fn create_fixed_size_list_empty() -> DaftResult<()> {
        let arr = FixedSizeListArray::empty(
            "foo",
            &DataType::FixedSizeList(Box::new(DataType::Int64), 3),
        );
        assert_eq!(arr.len(), 0);
        Ok(())
    }

    #[test]
    fn create_struct_empty() -> DaftResult<()> {
        let arr = StructArray::empty(
            "foo",
            &DataType::Struct(vec![Field::new("bar", DataType::Int64)]),
        );
        assert_eq!(arr.len(), 0);
        Ok(())
    }
}
