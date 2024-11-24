use crate::{
    array::ListArray,
    datatypes::logical::{FixedShapeSparseTensorArray, SparseTensorArray},
};

impl SparseTensorArray {
    pub fn values_array(&self) -> &ListArray {
        const VALUES_IDX: usize = 0;
        let array = self.physical.children.get(VALUES_IDX).unwrap();
        array.list().unwrap()
    }

    pub fn indices_array(&self) -> &ListArray {
        const INDICES_IDX: usize = 1;
        let array = self.physical.children.get(INDICES_IDX).unwrap();
        array.list().unwrap()
    }

    pub fn shape_array(&self) -> &ListArray {
        const SHAPE_IDX: usize = 2;
        let array = self.physical.children.get(SHAPE_IDX).unwrap();
        array.list().unwrap()
    }
}

impl FixedShapeSparseTensorArray {
    pub fn values_array(&self) -> &ListArray {
        const VALUES_IDX: usize = 0;
        let array = self.physical.children.get(VALUES_IDX).unwrap();
        array.list().unwrap()
    }

    pub fn indices_array(&self) -> &ListArray {
        const INDICES_IDX: usize = 1;
        let array = self.physical.children.get(INDICES_IDX).unwrap();
        array.list().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use common_error::DaftResult;

    use crate::{array::prelude::*, datatypes::prelude::*, series::IntoSeries};

    #[test]
    fn test_sparse_tensor_to_fixed_shape_sparse_tensor_roundtrip() -> DaftResult<()> {
        let raw_validity = vec![true, false, true];
        let validity = arrow2::bitmap::Bitmap::from(raw_validity.as_slice());

        let values_array = ListArray::new(
            Field::new("values", DataType::List(Box::new(DataType::Int64))),
            Int64Array::from((
                "item",
                Box::new(arrow2::array::Int64Array::from_iter(
                    [Some(1), Some(2), Some(0), Some(3)].iter(),
                )),
            ))
            .into_series(),
            arrow2::offset::OffsetsBuffer::<i64>::try_from(vec![0, 2, 3, 4])?,
            Some(validity.clone()),
        )
        .into_series();

        let indices_array = ListArray::new(
            Field::new("indices", DataType::List(Box::new(DataType::UInt64))),
            UInt64Array::from((
                "item",
                Box::new(arrow2::array::UInt64Array::from_iter(
                    [Some(1), Some(2), Some(0), Some(2)].iter(),
                )),
            ))
            .into_series(),
            arrow2::offset::OffsetsBuffer::<i64>::try_from(vec![0, 2, 3, 4])?,
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

        let dtype = DataType::SparseTensor(Box::new(DataType::Int64));
        let struct_array = StructArray::new(
            Field::new("tensor", dtype.to_physical()),
            vec![values_array, indices_array, shapes_array],
            Some(validity),
        );
        let sparse_tensor_array =
            SparseTensorArray::new(Field::new(struct_array.name(), dtype.clone()), struct_array);
        let fixed_shape_sparse_tensor_dtype =
            DataType::FixedShapeSparseTensor(Box::new(DataType::Int64), vec![3]);
        let fixed_shape_sparse_tensor_array =
            sparse_tensor_array.cast(&fixed_shape_sparse_tensor_dtype)?;
        let roundtrip_tensor = fixed_shape_sparse_tensor_array.cast(&dtype)?;

        let round_trip_tensor_arrow = roundtrip_tensor.to_arrow();
        let sparse_tensor_array_arrow = sparse_tensor_array.to_arrow();

        assert_eq!(round_trip_tensor_arrow, sparse_tensor_array_arrow);

        Ok(())
    }

    #[test]
    fn test_fixed_shape_sparse_datatype() -> DaftResult<()> {
        const INDICES_IDX: usize = 1;
        let element_counts = [
            2u64.pow(8) - 1,
            2u64.pow(16) - 1,
            2u64.pow(32) - 1,
            2u64.pow(64) - 1,
        ];
        let indices_minimal_dtype = [
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ];

        for (n_elements, minimal_dtype) in element_counts.iter().zip(indices_minimal_dtype.iter()) {
            let dtype =
                DataType::FixedShapeSparseTensor(Box::new(DataType::Float32), vec![*n_elements]);
            let physical_dtype = dtype.to_physical();
            if let DataType::Struct(fields) = physical_dtype {
                assert_eq!(fields.len(), 2, "Expected exactly 2 fields in Struct");

                let indices_field = &fields[INDICES_IDX];
                assert_eq!(indices_field.name, "indices");
                assert_eq!(
                    indices_field.dtype,
                    DataType::List(Box::new(minimal_dtype.clone()))
                );
            } else {
                panic!("Expected Struct DataType, got {:?}", physical_dtype);
            }
        }

        Ok(())
    }
}
