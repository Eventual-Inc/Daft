use crate::{array::ListArray, datatypes::logical::TensorArray};

impl TensorArray {
    pub fn data_array(&self) -> &ListArray {
        const DATA_IDX: usize = 0;
        let array = self.physical.children.get(DATA_IDX).unwrap();
        array.list().unwrap()
    }

    pub fn shape_array(&self) -> &ListArray {
        const SHAPE_IDX: usize = 1;
        let array = self.physical.children.get(SHAPE_IDX).unwrap();
        array.list().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use common_error::DaftResult;

    use crate::{array::prelude::*, datatypes::prelude::*, series::IntoSeries};

    #[test]
    fn test_tensor_to_sparse_roundtrip() -> DaftResult<()> {
        let raw_validity = vec![true, false, true];
        let validity = arrow2::bitmap::Bitmap::from(raw_validity.as_slice());

        let list_array = ListArray::new(
            Field::new("data", DataType::List(Box::new(DataType::Int64))),
            Int64Array::from((
                "item",
                Box::new(arrow2::array::Int64Array::from_iter(
                    [
                        Some(0),
                        Some(1),
                        Some(2),
                        Some(100),
                        Some(101),
                        Some(102),
                        Some(0),
                        Some(0),
                        Some(3),
                    ]
                    .iter(),
                )),
            ))
            .into_series(),
            arrow2::offset::OffsetsBuffer::<i64>::try_from(vec![0, 3, 6, 9])?,
            Some(validity.clone()),
        )
        .into_series();
        let shapes_array = ListArray::new(
            Field::new("shape", DataType::List(Box::new(DataType::UInt64))),
            UInt64Array::from((
                "item",
                Box::new(arrow2::array::UInt64Array::from_iter(
                    [Some(3), Some(3), Some(3)].iter(),
                )),
            ))
            .into_series(),
            arrow2::offset::OffsetsBuffer::<i64>::try_from(vec![0, 1, 2, 3])?,
            Some(validity.clone()),
        )
        .into_series();
        let dtype = DataType::Tensor(Box::new(DataType::Int64));
        let struct_array = StructArray::new(
            Field::new("tensor", dtype.to_physical()),
            vec![list_array, shapes_array],
            Some(validity),
        );
        let tensor_array =
            TensorArray::new(Field::new(struct_array.name(), dtype.clone()), struct_array);
        let sparse_tensor_dtype = DataType::SparseTensor(Box::new(DataType::Int64));
        let sparse_tensor_array = tensor_array.cast(&sparse_tensor_dtype)?;
        let roundtrip_tensor = sparse_tensor_array.cast(&dtype)?;
        assert!(tensor_array.to_arrow().eq(&roundtrip_tensor.to_arrow()));
        Ok(())
    }

    #[test]
    fn test_fixed_shape_tensor_to_fixed_shape_sparse_roundtrip() -> DaftResult<()> {
        let raw_validity = vec![true, false, true];
        let validity = arrow2::bitmap::Bitmap::from(raw_validity.as_slice());
        let field = Field::new("foo", DataType::FixedSizeList(Box::new(DataType::Int64), 3));
        let flat_child = Int64Array::from(("foo", (0..9).collect::<Vec<i64>>()));
        let arr = FixedSizeListArray::new(field, flat_child.into_series(), Some(validity));
        let dtype = DataType::FixedShapeTensor(Box::new(DataType::Int64), vec![3]);
        let tensor_array = FixedShapeTensorArray::new(Field::new("data", dtype.clone()), arr);
        let sparse_tensor_dtype =
            DataType::FixedShapeSparseTensor(Box::new(DataType::Int64), vec![3]);
        let sparse_tensor_array = tensor_array.cast(&sparse_tensor_dtype)?;
        let roundtrip_tensor = sparse_tensor_array.cast(&dtype)?;
        assert!(tensor_array.to_arrow().eq(&roundtrip_tensor.to_arrow()));
        Ok(())
    }
}
