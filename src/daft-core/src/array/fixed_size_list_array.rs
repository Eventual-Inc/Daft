use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::array::growable::{Growable, GrowableArray};
use crate::datatypes::{DaftArrayType, Field};
use crate::series::Series;
use crate::DataType;

#[derive(Clone, Debug)]
pub struct FixedSizeListArray {
    pub field: Arc<Field>,
    pub flat_child: Series,
    validity: Option<arrow2::bitmap::Bitmap>,
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
        validity: Option<arrow2::bitmap::Bitmap>,
    ) -> Self {
        let field: Arc<Field> = field.into();
        match &field.as_ref().dtype {
            DataType::FixedSizeList(child_dtype, size) => {
                if let Some(validity) = validity.as_ref()
                    && (validity.len() * size) != flat_child.len()
                {
                    panic!(
                        "FixedSizeListArray::new received values with len {} but expected it to match len of validity {} * size: {}",
                        flat_child.len(),
                        validity.len(),
                        (validity.len() * size),
                    )
                }
                if child_dtype.as_ref() != flat_child.data_type() {
                    panic!(
                        "FixedSizeListArray::new expects the child series to have dtype {}, but received: {}",
                        child_dtype,
                        flat_child.data_type(),
                    )
                }
            }
            _ => panic!(
                "FixedSizeListArray::new expected FixedSizeList datatype, but received field: {}",
                field
            ),
        }
        FixedSizeListArray {
            field,
            flat_child,
            validity,
        }
    }

    pub fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
        self.validity.as_ref()
    }

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 FixedSizeListArray to concat".to_string(),
            ));
        }

        let first_array = arrays.first().unwrap();
        let mut growable = <Self as GrowableArray>::make_growable(
            first_array.field.name.as_str(),
            &first_array.field.dtype,
            arrays.to_vec(),
            arrays
                .iter()
                .map(|a| a.validity.as_ref().map_or(0usize, |v| v.unset_bits()))
                .sum::<usize>()
                > 0,
            arrays.iter().map(|a| a.len()).sum(),
        );

        for (i, arr) in arrays.iter().enumerate() {
            growable.extend(i, 0, arr.len());
        }

        growable
            .build()
            .map(|s| s.downcast::<FixedSizeListArray>().unwrap().clone())
    }

    pub fn len(&self) -> usize {
        self.flat_child.len() / self.fixed_element_len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn name(&self) -> &str {
        &self.field.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    pub fn child_data_type(&self) -> &DataType {
        match &self.field.dtype {
            DataType::FixedSizeList(child, _) => child.as_ref(),
            _ => unreachable!("FixedSizeListArray must have DataType::FixedSizeList(..)"),
        }
    }

    pub fn rename(&self, name: &str) -> Self {
        Self::new(
            Field::new(name, self.data_type().clone()),
            self.flat_child.clone(),
            self.validity.clone(),
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
            self.validity
                .as_ref()
                .map(|v| v.clone().sliced(start, end - start)),
        ))
    }

    pub fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        let arrow_dtype = self.data_type().to_arrow().unwrap();
        Box::new(arrow2::array::FixedSizeListArray::new(
            arrow_dtype,
            self.flat_child.to_arrow(),
            self.validity.clone(),
        ))
    }

    pub fn fixed_element_len(&self) -> usize {
        let dtype = &self.field.as_ref().dtype;
        match dtype {
            DataType::FixedSizeList(_, s) => *s,
            _ => unreachable!("FixedSizeListArray should always have FixedSizeList datatype"),
        }
    }

    pub fn with_validity(&self, validity: Option<arrow2::bitmap::Bitmap>) -> DaftResult<Self> {
        if let Some(v) = &validity
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
            validity,
        ))
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

pub struct FixedSizeListArrayIter<'a> {
    array: &'a FixedSizeListArray,
    idx: usize,
}

impl Iterator for FixedSizeListArrayIter<'_> {
    type Item = Option<Series>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.array.len() {
            if let Some(validity) = self.array.validity()
                && !validity.get_bit(self.idx)
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
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::{
        datatypes::{Field, Int32Array},
        DataType, IntoSeries,
    };

    use super::FixedSizeListArray;

    /// Helper that returns a FixedSizeListArray, with each list element at len=3
    fn get_i32_fixed_size_list_array(validity: &[bool]) -> FixedSizeListArray {
        let field = Field::new("foo", DataType::FixedSizeList(Box::new(DataType::Int32), 3));
        let flat_child = Int32Array::from((
            "foo",
            (0i32..(validity.len() * 3) as i32).collect::<Vec<i32>>(),
        ));
        FixedSizeListArray::new(
            field,
            flat_child.into_series(),
            Some(arrow2::bitmap::Bitmap::from(validity)),
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
            renamed_arr
                .validity
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            arr.validity.unwrap().into_iter().collect::<Vec<_>>()
        );
        Ok(())
    }
}
