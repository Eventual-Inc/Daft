use std::iter::repeat;

use crate::{
    array::{DataArray, FixedSizeListArray, StructArray},
    datatypes::{BooleanArray, DaftArrowBackedType},
};
use common_error::DaftResult;

use super::as_arrow::AsArrow;

impl<T> DataArray<T>
where
    T: DaftArrowBackedType,
{
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.data(), mask.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

#[cfg(feature = "python")]
impl crate::datatypes::PythonArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        use arrow2::array::Array;
        use pyo3::PyObject;

        use crate::array::pseudo_arrow::PseudoArrowArray;
        use crate::datatypes::PythonType;

        let mask = mask.as_arrow();

        // Apply the filter mask to the data values, regardless of validity.
        let new_values = {
            mask.iter()
                .map(|x| x.unwrap_or(false))
                .zip(self.as_arrow().values().iter())
                .filter_map(|(f, item)| if f { Some(item.clone()) } else { None })
                .collect::<Vec<PyObject>>()
        };

        // Apply the filter mask to the validity bitmap.
        let new_validity = {
            self.as_arrow()
                .validity()
                .map(|old_validity| {
                    let old_validity_array = {
                        &arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            old_validity.clone(),
                            None,
                        )
                    };
                    arrow2::compute::filter::filter(old_validity_array, mask)
                })
                .transpose()?
                .map(|new_validity_dynarray| {
                    let new_validity_iter = new_validity_dynarray
                        .as_any()
                        .downcast_ref::<arrow2::array::BooleanArray>()
                        .unwrap()
                        .iter();
                    arrow2::bitmap::Bitmap::from_iter(
                        new_validity_iter.map(|valid| valid.unwrap_or(false)),
                    )
                })
        };

        let arrow_array: Box<dyn arrow2::array::Array> =
            Box::new(PseudoArrowArray::new(new_values.into(), new_validity));

        DataArray::<PythonType>::new(self.field().clone().into(), arrow_array)
    }
}

impl FixedSizeListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let size = self.fixed_element_len();
        let expanded_filter: Vec<bool> = mask
            .into_iter()
            .flat_map(|pred| repeat(pred.unwrap_or(false)).take(size))
            .collect();
        let expanded_filter = BooleanArray::from(("", expanded_filter.as_slice()));
        let filtered_child = self.flat_child.filter(&expanded_filter)?;
        let filtered_validity = self.validity.as_ref().map(|validity| {
            arrow2::bitmap::Bitmap::from_iter(mask.into_iter().zip(validity.iter()).filter_map(
                |(keep, valid)| match keep {
                    None => None,
                    Some(false) => None,
                    Some(true) => Some(valid),
                },
            ))
        });
        Ok(Self::new(
            self.field.clone(),
            filtered_child,
            filtered_validity,
        ))
    }
}

impl StructArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let filtered_children = self
            .children
            .iter()
            .map(|s| s.filter(&mask))
            .collect::<DaftResult<Vec<_>>>()?;
        let filtered_validity = self.validity.as_ref().map(|validity| {
            arrow2::bitmap::Bitmap::from_iter(mask.into_iter().zip(validity.iter()).filter_map(
                |(keep, valid)| match keep {
                    None => None,
                    Some(false) => None,
                    Some(true) => Some(valid),
                },
            ))
        });
        Ok(Self::new(
            self.field.clone(),
            filtered_children,
            filtered_validity,
        ))
    }
}
