use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef},
    compute::kernels::concat::concat,
};
use common_error::{DaftError, DaftResult};
use daft_arrow::offset::OffsetsBuffer;

use crate::{
    datatypes::{DaftArrayType, DataType, Field},
    prelude::{FromArrow, ListArray},
    series::Series,
};

#[derive(Clone, Debug, PartialEq)]
pub struct FixedSizeListArray {
    pub field: Arc<Field>,
    /// contains all the elements of the nested lists flattened into a single contiguous array.
    pub flat_child: Series,
    nulls: Option<daft_arrow::buffer::NullBuffer>,
}

impl DaftArrayType for FixedSizeListArray {
    fn data_type(&self) -> &DataType {
        &self.field.as_ref().dtype
    }
}

impl FixedSizeListArray {
    pub fn new<F: Into<Arc<Field>>>(
        field: F,
        flat_child: Series,
        nulls: Option<daft_arrow::buffer::NullBuffer>,
    ) -> Self {
        let field: Arc<Field> = field.into();
        match &field.as_ref().dtype {
            DataType::FixedSizeList(child_dtype, size) => {
                if let Some(nulls) = nulls.as_ref()
                    && (nulls.len() * size) != flat_child.len()
                {
                    panic!(
                        "FixedSizeListArray::new received values with len {} but expected it to match len of validity {} * size: {}",
                        flat_child.len(),
                        nulls.len(),
                        nulls.len() * size,
                    )
                }
                assert!(
                    !(child_dtype.as_ref() != flat_child.data_type()),
                    "FixedSizeListArray::new expects the child series to have dtype {}, but received: {}",
                    child_dtype,
                    flat_child.data_type(),
                );
            }
            _ => panic!(
                "FixedSizeListArray::new expected FixedSizeList datatype, but received field: {}",
                field
            ),
        }
        Self {
            field,
            flat_child,
            nulls,
        }
    }

    pub fn nulls(&self) -> Option<&daft_arrow::buffer::NullBuffer> {
        self.nulls.as_ref()
    }

    pub fn null_count(&self) -> usize {
        match self.nulls() {
            None => 0,
            Some(nulls) => nulls.null_count(),
        }
    }

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 FixedSizeListArray to concat".to_string(),
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
        self.flat_child.len() / self.fixed_element_len()
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

    #[must_use]
    pub fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    #[must_use]
    pub fn child_data_type(&self) -> &DataType {
        match &self.field.dtype {
            DataType::FixedSizeList(child, _) => child.as_ref(),
            _ => unreachable!("FixedSizeListArray must have DataType::FixedSizeList(..)"),
        }
    }

    #[must_use]
    pub fn rename(&self, name: &str) -> Self {
        Self::new(
            Field::new(name, self.data_type().clone()),
            self.flat_child.clone(),
            self.nulls.clone(),
        )
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        if start > end {
            return Err(DaftError::ValueError(format!(
                "Trying to slice array with negative length, start: {start} vs end: {end}"
            )));
        }
        let size = self.fixed_element_len();
        Ok(Self::new(
            self.field.clone(),
            self.flat_child.slice(start * size, end * size)?,
            self.nulls
                .as_ref()
                .map(|v| v.clone().slice(start, end - start)),
        ))
    }

    #[deprecated(note = "arrow2 migration")]
    pub fn to_arrow2(&self) -> Box<dyn daft_arrow::array::Array> {
        let arrow_dtype = self.data_type().to_arrow2().unwrap();
        Box::new(daft_arrow::array::FixedSizeListArray::new(
            arrow_dtype,
            self.flat_child.to_arrow2(),
            daft_arrow::buffer::wrap_null_buffer(self.nulls.clone()),
        ))
    }

    pub fn to_arrow(&self) -> DaftResult<ArrayRef> {
        let field = Arc::new(self.flat_child.field().to_arrow()?);
        let size = self.fixed_element_len() as i32;
        let values = self.flat_child.to_arrow()?;
        let nulls = self.nulls.clone();

        Ok(Arc::new(arrow::array::FixedSizeListArray::try_new(
            field, size, values, nulls,
        )?))
    }

    pub fn fixed_element_len(&self) -> usize {
        let dtype = &self.field.as_ref().dtype;
        match dtype {
            DataType::FixedSizeList(_, s) => *s,
            _ => unreachable!("FixedSizeListArray should always have FixedSizeList datatype"),
        }
    }

    pub fn with_nulls(&self, nulls: Option<daft_arrow::buffer::NullBuffer>) -> DaftResult<Self> {
        if let Some(v) = &nulls
            && v.len() != self.len()
        {
            return Err(DaftError::ValueError(format!(
                "validity mask length does not match FixedSizeListArray length, {} vs {}",
                v.len(),
                self.len()
            )));
        }

        Ok(Self::new(
            self.field.clone(),
            self.flat_child.clone(),
            nulls,
        ))
    }

    fn generate_offsets(&self) -> OffsetsBuffer<i64> {
        let size = self.fixed_element_len();
        let len = self.len();

        // Create new offsets
        let offsets: Vec<i64> = (0..=len)
            .map(|i| i64::try_from(i * size).unwrap())
            .collect();

        OffsetsBuffer::try_from(offsets).expect("Failed to create OffsetsBuffer")
    }

    pub fn to_list(&self) -> ListArray {
        let field = &self.field;

        let DataType::FixedSizeList(inner_type, _) = &field.dtype else {
            unreachable!("Expected FixedSizeListArray, got {:?}", field.dtype);
        };

        let datatype = DataType::List(inner_type.clone());
        let mut field = (**field).clone();
        field.dtype = datatype;

        ListArray::new(
            field,
            self.flat_child.clone(),
            self.generate_offsets(),
            self.nulls.clone(),
        )
    }
}

impl<'a> IntoIterator for &'a FixedSizeListArray {
    type Item = Option<Series>;

    type IntoIter = FixedSizeListArrayIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        FixedSizeListArrayIter {
            array: self,
            idx: 0,
        }
    }
}

#[derive(Clone)]
pub struct FixedSizeListArrayIter<'a> {
    array: &'a FixedSizeListArray,
    idx: usize,
}

impl Iterator for FixedSizeListArrayIter<'_> {
    type Item = Option<Series>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.array.len() {
            if let Some(nulls) = self.array.nulls()
                && nulls.is_null(self.idx)
            {
                self.idx += 1;
                Some(None)
            } else {
                let step = self.array.fixed_element_len();

                let start = self.idx * step;
                let end = (self.idx + 1) * step;

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

impl ExactSizeIterator for FixedSizeListArrayIter<'_> {
    fn len(&self) -> usize {
        self.array.len() - self.idx
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;

    use super::FixedSizeListArray;
    use crate::{
        datatypes::{DataType, Field, Int32Array},
        series::IntoSeries,
    };

    /// Helper that returns a FixedSizeListArray, with each list element at len=3
    fn get_i32_fixed_size_list_array(nulls: &[bool]) -> FixedSizeListArray {
        let field = Field::new("foo", DataType::FixedSizeList(Box::new(DataType::Int32), 3));
        let flat_child = Int32Array::from_values(
            "foo",
            (0i32..(nulls.len() * 3) as i32).collect::<Vec<i32>>(),
        );
        FixedSizeListArray::new(
            field,
            flat_child.into_series(),
            Some(daft_arrow::buffer::NullBuffer::from(nulls)),
        )
    }

    #[test]
    fn test_rename() -> DaftResult<()> {
        let arr = get_i32_fixed_size_list_array(vec![true, true, false].as_slice());
        let renamed_arr = arr.rename("bar");

        assert_eq!(renamed_arr.name(), "bar");
        assert_eq!(renamed_arr.flat_child.len(), arr.flat_child.len());
        assert_eq!(
            renamed_arr
                .flat_child
                .i32()?
                .into_iter()
                .collect::<Vec<_>>(),
            arr.flat_child.i32()?.into_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            renamed_arr.nulls.unwrap().into_iter().collect::<Vec<_>>(),
            arr.nulls.unwrap().into_iter().collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn test_fixed_size_list_to_arrow() -> DaftResult<()> {
        let arr = get_i32_fixed_size_list_array(vec![true, true, false].as_slice());
        let arrow_arr = arr.to_arrow()?;

        assert_eq!(
            arrow_arr.data_type(),
            &arrow::datatypes::DataType::FixedSizeList(
                Arc::new(arrow::datatypes::Field::new(
                    "foo",
                    arrow::datatypes::DataType::Int32,
                    true
                )),
                3
            )
        );
        assert_eq!(arrow_arr.len(), arr.len());
        assert_eq!(arrow_arr.null_count(), arr.null_count());

        assert_eq!(
            arrow_arr
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeListArray>()
                .unwrap()
                .values()
                .as_ref()
                .len(),
            arr.flat_child.len()
        );

        Ok(())
    }
}
