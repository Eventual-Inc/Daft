use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    buffer::{NullBuffer, ScalarBuffer},
};
use common_error::{DaftError, DaftResult};

use crate::{
    array::growable::{Growable, GrowableArray},
    datatypes::{DaftArrayType, DataType, Field},
    series::Series,
};

#[derive(Clone, Debug)]
pub struct UnionArray {
    pub field: Arc<Field>,
    pub ids: ScalarBuffer<i8>,
    pub children: Vec<Series>,
    pub offsets: Option<ScalarBuffer<i32>>,
}

impl DaftArrayType for UnionArray {
    fn data_type(&self) -> &DataType {
        &self.field.as_ref().dtype
    }
}

impl UnionArray {
    pub fn new<F: Into<Arc<Field>>>(
        field: F,
        ids: ScalarBuffer<i8>,
        children: Vec<Series>,
        offsets: Option<ScalarBuffer<i32>>,
    ) -> Self {
        let field: Arc<Field> = field.into();
        match &field.as_ref().dtype {
            DataType::Union(fields, _, mode) => {
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

                Self {
                    field,
                    ids,
                    children,
                    offsets,
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

    pub fn ids(&self) -> &ScalarBuffer<i8> {
        &self.ids
    }

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 UnionArray to concat".to_string(),
            ));
        }

        let first_array = arrays.first().unwrap();
        let mut growable = <Self as GrowableArray>::make_growable(
            first_array.field.name.as_ref(),
            &first_array.field.dtype,
            arrays.to_vec(),
            false,
            arrays.iter().map(|a| a.len()).sum(),
        );

        for (i, arr) in arrays.iter().enumerate() {
            growable.extend(i, 0, arr.len());
        }

        growable
            .build()
            .map(|s| s.downcast::<Self>().unwrap().clone())
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

    pub fn offsets(&self) -> &Option<ScalarBuffer<i32>> {
        &self.offsets
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
        }
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
                let sliced_offsets = offsets.slice(start, end - start);
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
            self.ids.slice(start, end - start),
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

        Ok(Arc::new(unsafe {
            arrow::array::UnionArray::new_unchecked(
                fields.clone(),
                self.ids.clone(),
                self.offsets.clone(),
                children,
            )
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
