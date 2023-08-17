use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::datatypes::{BooleanArray, DaftArrayType, Field};
use crate::series::Series;
use crate::DataType;

#[derive(Clone)]
pub struct FixedSizeListArray {
    pub field: Arc<Field>,
    pub flat_child: Series,
    pub(crate) validity: Option<BooleanArray>,
}

impl DaftArrayType for FixedSizeListArray {}

impl FixedSizeListArray {
    pub fn new<F: Into<Arc<Field>>>(
        field: F,
        flat_child: Series,
        validity: Option<BooleanArray>,
    ) -> Self {
        let field: Arc<Field> = field.into();
        if !matches!(&field.as_ref().dtype, &DataType::FixedSizeList(..)) {
            panic!(
                "FixedSizeListArray::new expected FixedSizeList datatype, but received field: {}",
                field
            )
        }
        FixedSizeListArray {
            field,
            flat_child,
            validity,
        }
    }

    pub fn empty(field_name: &str, dtype: &DataType) -> Self {
        match dtype {
            DataType::FixedSizeList(child, _) => {
                let field = Field::new(field_name, dtype.clone());
                let empty_child = Series::empty(field_name, &child.dtype);
                Self::new(field, empty_child, None)
            }
            _ => panic!(
                "Cannot create empty FixedSizeListArray with dtype: {}",
                dtype
            ),
        }
    }

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 FixedSizeListArray to concat".to_string(),
            ));
        }
        let _flat_children: Vec<_> = arrays.iter().map(|a| &a.flat_child).collect();
        let _validities = arrays.iter().map(|a| &a.validity);
        let _lens = arrays.iter().map(|a| a.len());

        // TODO(FixedSizeList)
        todo!();
    }

    pub fn len(&self) -> usize {
        match &self.validity {
            None => self.flat_child.len() / self.fixed_element_len(),
            Some(validity) => validity.len(),
        }
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
            DataType::FixedSizeList(child, _) => &child.dtype,
            _ => unreachable!("FixedSizeListArray must have DataType::FixedSizeList(..)"),
        }
    }

    pub fn rename(&self, name: &str) -> Self {
        Self::new(
            Field::new(name, self.data_type().clone()),
            self.flat_child.rename(name),
            self.validity.clone(),
        )
    }

    pub fn slice(&self, _start: usize, _end: usize) -> DaftResult<Self> {
        // TODO(FixedSizeList)
        todo!()
    }

    pub fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        Box::new(arrow2::array::FixedSizeListArray::new(
            self.data_type().to_arrow().unwrap(),
            self.flat_child.to_arrow(),
            self.validity.as_ref().map(|validity| {
                arrow2::bitmap::Bitmap::from_iter(validity.into_iter().map(|v| v.unwrap()))
            }),
        ))
    }

    pub fn fixed_element_len(&self) -> usize {
        let dtype = &self.field.as_ref().dtype;
        match dtype {
            DataType::FixedSizeList(_, s) => *s,
            _ => unreachable!("FixedSizeListArray should always have FixedSizeList datatype"),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::{
        datatypes::{BooleanArray, Field, Int32Array},
        DataType, IntoSeries,
    };

    use super::FixedSizeListArray;

    /// Helper that returns a FixedSizeListArray, with each list element at len=3
    fn get_i32_fixed_size_list_array(validity: &[bool]) -> FixedSizeListArray {
        let field = Field::new(
            "foo",
            DataType::FixedSizeList(Box::new(Field::new("foo", DataType::Int32)), 3),
        );
        let num_valid_elements = validity.iter().map(|v| if *v { 1 } else { 0 }).sum();
        let flat_child = Int32Array::from(("foo", (0..num_valid_elements).collect::<Vec<i32>>()));
        let validity = Some(BooleanArray::from(("foo", validity)));
        FixedSizeListArray::new(field, flat_child.into_series(), validity)
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
