use std::iter::repeat;
use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::datatypes::{DaftArrayType, Field};
use crate::series::Series;
use crate::DataType;

#[derive(Clone)]
pub struct FixedSizeListArray {
    pub field: Arc<Field>,
    pub flat_child: Series,
    pub validity: Option<arrow2::bitmap::Bitmap>,
}

impl DaftArrayType for FixedSizeListArray {}

impl FixedSizeListArray {
    pub fn new<F: Into<Arc<Field>>>(
        field: F,
        flat_child: Series,
        validity: Option<arrow2::bitmap::Bitmap>,
    ) -> Self {
        let field: Arc<Field> = field.into();
        match &field.as_ref().dtype {
            DataType::FixedSizeList(_, size) => {
                if let Some(validity) = validity.as_ref() && (validity.len() * size) != flat_child.len() {
                    panic!(
                        "FixedSizeListArray::new received values with len {} but expected it to match len of validity * size: {}",
                        flat_child.len(),
                        (validity.len() * size),
                    )
                }
            }
            _ => panic!(
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

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 FixedSizeListArray to concat".to_string(),
            ));
        }
        let flat_children: Vec<&Series> = arrays.iter().map(|a| &a.flat_child).collect();
        let concatted_flat_children = Series::concat(&flat_children)?;

        let validities: Vec<_> = arrays.iter().map(|a| &a.validity).collect();
        let validity = if validities.iter().filter(|v| v.is_some()).count() == 0 {
            None
        } else {
            let lens = arrays.iter().map(|a| a.len());
            let concatted_validities = validities.iter().zip(lens).flat_map(|(v, l)| {
                let x: Box<dyn Iterator<Item = bool>> = match v {
                    None => Box::new(repeat(true).take(l)),
                    Some(v) => Box::new(v.into_iter()),
                };
                x
            });
            Some(arrow2::bitmap::Bitmap::from_iter(concatted_validities))
        };

        Ok(Self::new(
            arrays.first().unwrap().field.clone(),
            concatted_flat_children,
            validity,
        ))
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
        let field = Field::new(
            "foo",
            DataType::FixedSizeList(Box::new(Field::new("foo", DataType::Int32)), 3),
        );
        let flat_child = Int32Array::from(("foo", (0..validity.len() * 3).collect::<Vec<i32>>()));
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
