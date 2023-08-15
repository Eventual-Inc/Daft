use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::datatypes::{BooleanArray, DaftArrayType, Field};
use crate::series::Series;
use crate::DataType;

#[derive(Clone)]
pub struct FixedSizeListArray {
    field: Arc<Field>,
    flat_child: Series,
    pub(crate) validity: Option<BooleanArray>,
}

impl DaftArrayType for FixedSizeListArray {
    fn len(&self) -> usize {
        match &self.validity {
            None => self.flat_child.len() / self.fixed_element_len(),
            Some(validity) => validity.len(),
        }
    }
}

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

    pub fn name(&self) -> &str {
        &self.field.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    fn fixed_element_len(&self) -> usize {
        let dtype = &self.field.as_ref().dtype;
        match dtype {
            DataType::FixedSizeList(_, s) => *s,
            _ => unreachable!("FixedSizeListArray should always have FixedSizeList datatype"),
        }
    }
}
