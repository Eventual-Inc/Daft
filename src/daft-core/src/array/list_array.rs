use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef},
    buffer::ScalarBuffer,
    compute::kernels::concat::concat,
};
use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::{DaftArrayType, DataType, Field},
    prelude::FromArrow,
    series::Series,
};

#[derive(Clone, Debug, PartialEq)]
pub struct ListArray {
    pub field: Arc<Field>,
    pub flat_child: Series,

    /// Where each row starts and ends. Null rows usually have the same start/end index, but this is not guaranteed.
    offsets: daft_arrow::offset::OffsetsBuffer<i64>,
    validity: Option<daft_arrow::buffer::NullBuffer>,
}

impl DaftArrayType for ListArray {
    fn data_type(&self) -> &DataType {
        &self.field.as_ref().dtype
    }
}

impl ListArray {
    pub fn new<F: Into<Arc<Field>>>(
        field: F,
        flat_child: Series,
        offsets: daft_arrow::offset::OffsetsBuffer<i64>,
        validity: Option<daft_arrow::buffer::NullBuffer>,
    ) -> Self {
        let field: Arc<Field> = field.into();
        match &field.as_ref().dtype {
            DataType::List(child_dtype) => {
                if let Some(validity) = validity.as_ref()
                    && validity.len() != offsets.len_proxy()
                {
                    panic!(
                        "ListArray::new validity length does not match computed length from offsets"
                    )
                }
                assert!(
                    !(child_dtype.as_ref() != flat_child.data_type()),
                    "ListArray::new expects the child series to have field {}, but received: {}",
                    child_dtype,
                    flat_child.data_type(),
                );
                assert!(
                    *offsets.last() <= flat_child.len() as i64,
                    "ListArray::new received offsets with last value {}, but child series has length {}",
                    offsets.last(),
                    flat_child.len()
                );
            }
            _ => panic!(
                "ListArray::new expected List datatype, but received field: {}",
                field
            ),
        }
        Self {
            field,
            flat_child,
            offsets,
            validity,
        }
    }

    pub fn offsets(&self) -> &daft_arrow::offset::OffsetsBuffer<i64> {
        &self.offsets
    }

    pub fn validity(&self) -> Option<&daft_arrow::buffer::NullBuffer> {
        self.validity.as_ref()
    }

    pub fn null_count(&self) -> usize {
        match self.validity() {
            None => 0,
            Some(validity) => validity.null_count(),
        }
    }

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 ListArray to concat".to_string(),
            ));
        }
        let first_field = arrays[0].field().clone();

        let arc_vec = arrays
            .iter()
            .map(|arr| {
                let mut arr = (*arr).clone();
                // arrow-rs concat does a deep equality on the field names, which arrow2 did not.
                // so to make sure we can `concat`, we need to rename both the child and the array itself
                arr.flat_child = arr.flat_child.rename(&first_field.name);
                arr.rename(&first_field.name).to_arrow()
            })
            .collect::<DaftResult<Vec<ArrayRef>>>()?;
        let ref_vec: Vec<&dyn Array> = arc_vec.iter().map(|x| x.as_ref()).collect();

        let res = concat(&ref_vec)?;
        Self::from_arrow(first_field, res)
    }

    pub fn len(&self) -> usize {
        self.offsets.len_proxy()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn name(&self) -> &str {
        &self.field.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    pub fn child_data_type(&self) -> &DataType {
        match &self.field.dtype {
            DataType::List(child_dtype) => child_dtype.as_ref(),
            _ => unreachable!("ListArray must have DataType::List(..)"),
        }
    }

    pub fn rename(&self, name: &str) -> Self {
        Self::new(
            Field::new(name, self.data_type().clone()),
            self.flat_child.clone(),
            self.offsets.clone(),
            self.validity.clone(),
        )
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        if start > end {
            return Err(DaftError::ValueError(format!(
                "Trying to slice array with negative length, start: {start} vs end: {end}"
            )));
        }
        let mut new_offsets = self.offsets.clone();
        new_offsets.slice(start, end - start + 1);

        let new_validity = self
            .validity
            .as_ref()
            .map(|v| v.clone().slice(start, end - start));
        Ok(Self::new(
            self.field.clone(),
            self.flat_child.clone(),
            new_offsets,
            new_validity,
        ))
    }
    #[deprecated(note = "arrow2 migration")]
    pub fn to_arrow2(&self) -> Box<dyn daft_arrow::array::Array> {
        let arrow_dtype = self.data_type().to_arrow2().unwrap();
        Box::new(daft_arrow::array::ListArray::new(
            arrow_dtype,
            self.offsets().clone(),
            self.flat_child.to_arrow2(),
            daft_arrow::buffer::wrap_null_buffer(self.validity.clone()),
        ))
    }

    pub fn to_arrow(&self) -> DaftResult<ArrayRef> {
        let mut field = self.flat_child.field().to_arrow()?;
        field = field.with_name("item");
        let offsets = self.offsets().clone();
        let arrow_offsets: arrow::buffer::Buffer = offsets.buffer().clone().into();

        let offsets = arrow::buffer::OffsetBuffer::new(ScalarBuffer::from(arrow_offsets));
        let values = self.flat_child.to_arrow()?;
        let nulls = self.validity.clone();
        Ok(Arc::new(arrow::array::LargeListArray::new(
            Arc::new(field),
            offsets,
            values,
            nulls,
        )))
    }

    pub fn with_validity(
        &self,
        validity: Option<daft_arrow::buffer::NullBuffer>,
    ) -> DaftResult<Self> {
        if let Some(v) = &validity
            && v.len() != self.len()
        {
            return Err(DaftError::ValueError(format!(
                "validity mask length does not match ListArray length, {} vs {}",
                v.len(),
                self.len()
            )));
        }

        Ok(Self::new(
            self.field.clone(),
            self.flat_child.clone(),
            self.offsets.clone(),
            validity,
        ))
    }
}

impl<'a> IntoIterator for &'a ListArray {
    type Item = Option<Series>;

    type IntoIter = ListArrayIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ListArrayIter {
            array: self,
            idx: 0,
        }
    }
}

impl ListArray {
    pub fn iter(&self) -> ListArrayIter<'_> {
        ListArrayIter {
            array: self,
            idx: 0,
        }
    }
}

#[derive(Clone)]
pub struct ListArrayIter<'a> {
    array: &'a ListArray,
    idx: usize,
}

impl Iterator for ListArrayIter<'_> {
    type Item = Option<Series>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.array.len() {
            if let Some(validity) = self.array.validity()
                && validity.is_null(self.idx)
            {
                self.idx += 1;
                Some(None)
            } else {
                let start = *self.array.offsets().get(self.idx).unwrap() as usize;
                let end = *self.array.offsets().get(self.idx + 1).unwrap() as usize;

                self.idx += 1;
                Some(Some(self.array.flat_child.slice(start, end).unwrap()))
            }
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.idx,
            Some(self.array.len() - self.idx),
        )
    }
}

impl ExactSizeIterator for ListArrayIter<'_> {
    fn len(&self) -> usize {
        self.array.len() - self.idx
    }
}
