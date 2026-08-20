use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    buffer::{NullBuffer, ScalarBuffer},
};
use common_error::{DaftError, DaftResult};

use crate::{
    array::ops::from_arrow::FromArrow,
    datatypes::{DaftArrayType, DataType, Field},
    series::Series,
};

#[derive(Clone, Debug)]
pub struct UnionArray {
    pub field: Arc<Field>,
    pub children: Vec<Series>,
    ids: Vec<i8>,
    offsets: Option<Vec<i32>>,
    id_map: [usize; 128],
}

impl DaftArrayType for UnionArray {
    fn data_type(&self) -> &DataType {
        &self.field.as_ref().dtype
    }
}

impl UnionArray {
    pub fn new<F: Into<Arc<Field>>>(
        field: F,
        ids: Vec<i8>,
        children: Vec<Series>,
        offsets: Option<Vec<i32>>,
    ) -> Self {
        let field: Arc<Field> = field.into();
        match &field.as_ref().dtype {
            DataType::Union(fields, type_ids, mode) => {
                assert!(
                    fields.len() == children.len(),
                    "StructArray::new received {} children arrays but expected {} for specified dtype: {}",
                    children.len(),
                    fields.len(),
                    &field.as_ref().dtype
                );
                for (dtype_field, series) in fields.iter().zip(children.iter()) {
                    assert!(
                        !(&dtype_field.dtype != series.data_type()),
                        "StructArray::new received an array with dtype: {} but expected child field: {}",
                        series.data_type(),
                        dtype_field
                    );
                    assert!(
                        dtype_field.name.as_ref() == series.name(),
                        "StructArray::new received a series with name: {} but expected name: {}",
                        series.name(),
                        &dtype_field.name
                    );
                }

                assert!(
                    offsets.is_some() == mode.is_dense(),
                    "UnionArray can only have offsets if mode is dense"
                );

                if let Some(offsets) = &offsets {
                    assert!(
                        offsets.len() == ids.len(),
                        "Type Ids and Offsets lengths must match"
                    );
                } else {
                    for child in &children {
                        assert!(
                            child.len() == ids.len(),
                            "Sparse union child arrays must be equal in length to the length of the union"
                        );
                    }
                }

                let mut id_map = [0; 128];
                for (i, type_id) in type_ids.iter().enumerate() {
                    assert!(*type_id >= 0, "Union Type Ids must be >= 0");
                    id_map[*type_id as usize] = i;
                }

                Self {
                    field,
                    children,
                    ids,
                    offsets,
                    id_map,
                }
            }
            _ => {
                panic!(
                    "UnionArray::new expected Union datatype, but received field: {}",
                    field
                )
            }
        }
    }

    pub fn ids(&self) -> &Vec<i8> {
        &self.ids
    }

    pub fn nulls(&self) -> Option<&NullBuffer> {
        None
    }

    pub fn null_count(&self) -> usize {
        0
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn field(&self) -> &Arc<Field> {
        &self.field
    }

    pub fn offsets(&self) -> &Option<Vec<i32>> {
        &self.offsets
    }

    pub fn type_idx(&self, idx: i8) -> usize {
        assert!(idx >= 0, "Union Type Ids must be >= 0");
        self.id_map[idx as usize]
    }

    pub fn name(&self) -> &str {
        &self.field.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    pub fn rename(&self, name: &str) -> Self {
        Self {
            field: Arc::new(Field::new(name, self.data_type().clone())),
            ids: self.ids.clone(),
            children: self.children.clone(),
            offsets: self.offsets.clone(),
            id_map: self.id_map,
        }
    }

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 UnionArray to concat".to_string(),
            ));
        }

        if arrays.len() == 1 {
            return Ok((*arrays.first().unwrap()).clone());
        }

        let first_array = arrays.first().unwrap();
        let field = first_array.field.clone();

        let arrow_arrs = arrays
            .iter()
            .map(|arr| arr.to_arrow())
            .collect::<DaftResult<Vec<_>>>()?;
        let arrow_refs = arrow_arrs
            .iter()
            .map(|arr| arr.as_ref())
            .collect::<Vec<_>>();
        let concatenated = arrow::compute::concat(&arrow_refs)?;
        Self::from_arrow(field, concatenated)
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        if start > end {
            return Err(DaftError::ValueError(format!(
                "Trying to slice array with negative length, start: {start} vs end: {end}"
            )));
        }

        let (offsets, children) = match self.offsets.as_ref() {
            // If dense union, slice offsets
            Some(offsets) => {
                let sliced_offsets = offsets[start..end].to_vec();
                (Some(sliced_offsets), self.children.clone())
            }
            // Otherwise need to slice sparse children
            None => {
                let children = self
                    .children
                    .iter()
                    .map(|x| x.slice(start, end))
                    .collect::<DaftResult<Vec<Series>>>()?;
                (None, children)
            }
        };

        Ok(Self::new(
            self.field.clone(),
            self.ids[start..end].to_vec(),
            children,
            offsets,
        ))
    }

    pub fn to_arrow(&self) -> DaftResult<ArrayRef> {
        let field = self.field().to_arrow()?;

        let arrow::datatypes::DataType::Union(fields, _) = field.data_type() else {
            return Err(DaftError::TypeError(format!(
                "Expected UnionArray, got {:?}",
                field.data_type()
            )));
        };

        let children = self
            .children
            .iter()
            .map(|x| x.to_arrow())
            .collect::<DaftResult<Vec<ArrayRef>>>()?;

        let ids: ScalarBuffer<i8> = ScalarBuffer::from(self.ids.clone());
        let offsets: Option<ScalarBuffer<i32>> = self.offsets.clone().map(ScalarBuffer::from);

        Ok(Arc::new(unsafe {
            arrow::array::UnionArray::new_unchecked(fields.clone(), ids, offsets, children)
        }) as _)
    }

    pub fn with_nulls(&self, _nulls: Option<arrow::buffer::NullBuffer>) -> DaftResult<Self> {
        Ok(Self::new(
            self.field.clone(),
            self.ids.clone(),
            self.children.clone(),
            self.offsets.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_schema::union_mode::UnionMode;

    use super::*;
    use crate::{
        array::{ListArray, StructArray, ops::from_arrow::FromArrow},
        datatypes::{DataType, Field, Float64Array, Int32Array},
        series::IntoSeries,
    };

    fn make_sparse_union() -> UnionArray {
        // Two children: Int32 ("a") and Float64 ("b"), type IDs [0, 1]
        // ids buffer selects alternating children across 4 rows
        let child_a = Int32Array::from_vec("a", vec![1, 2, 3, 4]).into_series();
        let child_b = Float64Array::from_vec("b", vec![10.0, 20.0, 30.0, 40.0]).into_series();

        let dtype = DataType::Union(
            vec![
                Field::new("a", DataType::Int32),
                Field::new("b", DataType::Float64),
            ],
            vec![0, 1],
            UnionMode::Sparse,
        );
        UnionArray::new(
            Field::new("test", dtype),
            vec![0i8, 1, 0, 1],
            vec![child_a, child_b],
            None,
        )
    }

    fn make_dense_union() -> UnionArray {
        // Two children: Int32 ("a") and Float64 ("b"), type IDs [0, 1]
        // Dense: each child only holds its own values; offsets index into child arrays
        let child_a = Int32Array::from_vec("a", vec![1, 2]).into_series();
        let child_b = Float64Array::from_vec("b", vec![10.0, 20.0]).into_series();

        let dtype = DataType::Union(
            vec![
                Field::new("a", DataType::Int32),
                Field::new("b", DataType::Float64),
            ],
            vec![0, 1],
            UnionMode::Dense,
        );
        UnionArray::new(
            Field::new("test", dtype),
            vec![0i8, 1, 0, 1],
            vec![child_a, child_b],
            Some(vec![0i32, 0, 1, 1]),
        )
    }

    #[test]
    fn test_arrow_roundtrip_sparse_union() -> DaftResult<()> {
        let arr = make_sparse_union();

        let arrow_arr = arr.to_arrow()?;
        let new_arr = UnionArray::from_arrow(arr.field().clone(), arrow_arr)?;

        assert_eq!(arr.field(), new_arr.field());
        assert_eq!(arr.ids(), new_arr.ids());
        assert_eq!(arr.offsets(), new_arr.offsets());
        assert_eq!(arr.children, new_arr.children);

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_dense_union() -> DaftResult<()> {
        let arr = make_dense_union();

        let arrow_arr = arr.to_arrow()?;
        let new_arr = UnionArray::from_arrow(arr.field().clone(), arrow_arr)?;

        assert_eq!(arr.field(), new_arr.field());
        assert_eq!(arr.ids(), new_arr.ids());
        assert_eq!(arr.offsets(), new_arr.offsets());
        assert_eq!(arr.children, new_arr.children);

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_sparse_union_empty() -> DaftResult<()> {
        let dtype = DataType::Union(
            vec![
                Field::new("a", DataType::Int32),
                Field::new("b", DataType::Float64),
            ],
            vec![0, 1],
            UnionMode::Sparse,
        );
        let arr = UnionArray::new(
            Field::new("test", dtype),
            vec![],
            vec![
                Int32Array::from_vec("a", vec![]).into_series(),
                Float64Array::from_vec("b", vec![]).into_series(),
            ],
            None,
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr = UnionArray::from_arrow(arr.field().clone(), arrow_arr)?;

        assert_eq!(arr.field(), new_arr.field());
        assert_eq!(arr.ids(), new_arr.ids());
        assert_eq!(arr.offsets(), new_arr.offsets());
        assert!(new_arr.is_empty());

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_union_with_list_child() -> DaftResult<()> {
        // Child "a" is List(Int32), child "b" is Float64
        let list_child = ListArray::from_series(
            "a",
            vec![
                Some(Int32Array::from_vec("a", vec![1, 2]).into_series()),
                Some(Int32Array::from_vec("a", vec![3]).into_series()),
                Some(Int32Array::from_vec("a", vec![4, 5]).into_series()),
                Some(Int32Array::from_vec("a", vec![6]).into_series()),
            ],
        )?
        .into_series();
        let scalar_child = Float64Array::from_vec("b", vec![10.0, 20.0, 30.0, 40.0]).into_series();

        let dtype = DataType::Union(
            vec![
                Field::new("a", DataType::List(Box::new(DataType::Int32))),
                Field::new("b", DataType::Float64),
            ],
            vec![0, 1],
            UnionMode::Sparse,
        );
        let arr = UnionArray::new(
            Field::new("test", dtype),
            vec![0i8, 1, 0, 1],
            vec![list_child, scalar_child],
            None,
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr = UnionArray::from_arrow(arr.field().clone(), arrow_arr)?;

        assert_eq!(arr.field(), new_arr.field());
        assert_eq!(arr.ids(), new_arr.ids());
        assert_eq!(arr.offsets(), new_arr.offsets());
        assert_eq!(arr.children, new_arr.children);

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_union_with_struct_child() -> DaftResult<()> {
        // Child "a" is Struct({x: Int32, y: Float64}), child "b" is Int32
        // Dense union: type_ids = [0, 1, 0, 1], offsets = [0, 0, 1, 1]
        // so "a" child has 2 rows, "b" child has 2 rows
        let struct_dtype = DataType::Struct(vec![
            Field::new("x", DataType::Int32),
            Field::new("y", DataType::Float64),
        ]);
        let struct_child = StructArray::new(
            Field::new("a", struct_dtype.clone()),
            vec![
                Int32Array::from_vec("x", vec![1, 3]).into_series(),
                Float64Array::from_vec("y", vec![1.0, 3.0]).into_series(),
            ],
            None,
        )
        .into_series();
        let scalar_child = Int32Array::from_vec("b", vec![10, 30]).into_series();

        let dtype = DataType::Union(
            vec![
                Field::new("a", struct_dtype),
                Field::new("b", DataType::Int32),
            ],
            vec![0, 1],
            UnionMode::Dense,
        );
        let arr = UnionArray::new(
            Field::new("test", dtype),
            vec![0i8, 1, 0, 1],
            vec![struct_child, scalar_child],
            Some(vec![0i32, 0, 1, 1]),
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr = UnionArray::from_arrow(arr.field().clone(), arrow_arr)?;

        assert_eq!(arr.field(), new_arr.field());
        assert_eq!(arr.ids(), new_arr.ids());
        assert_eq!(arr.offsets(), new_arr.offsets());
        assert_eq!(arr.children, new_arr.children);

        Ok(())
    }
}
