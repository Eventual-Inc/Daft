use crate::{
    array::{
        growable::{Growable, GrowableArray},
        DataArray, FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::{BooleanArray, DaftArrowBackedType},
};
use arrow2::bitmap::utils::SlicesIterator;
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

impl ListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let child_capacity = SlicesIterator::new(mask.as_arrow().values())
            .map(|(start_valid, len_valid)| {
                self.offsets().start_end(start_valid + len_valid - 1).1
                    - self.offsets().start_end(start_valid).0
            })
            .sum();
        let mut growable = <ListArray as GrowableArray>::GrowableType::new(
            self.name().to_string(),
            self.data_type(),
            vec![self],
            self.validity().is_some(),
            mask.len(),
            child_capacity,
        );

        for (start_keep, len_keep) in SlicesIterator::new(mask.as_arrow().values()) {
            growable.extend(0, start_keep, len_keep);
        }

        Ok(growable.build()?.downcast::<ListArray>()?.clone())
    }
}

impl FixedSizeListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let mut growable = FixedSizeListArray::make_growable(
            self.name().to_string(),
            self.data_type(),
            vec![self],
            self.validity().is_some(),
            mask.len(),
        );

        for (start_keep, len_keep) in SlicesIterator::new(mask.as_arrow().values()) {
            growable.extend(0, start_keep, len_keep);
        }

        Ok(growable.build()?.downcast::<FixedSizeListArray>()?.clone())
    }
}

impl StructArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let mut growable = StructArray::make_growable(
            self.name().to_string(),
            self.data_type(),
            vec![self],
            self.validity().is_some(),
            mask.len(),
        );

        for (start_keep, len_keep) in SlicesIterator::new(mask.as_arrow().values()) {
            growable.extend(0, start_keep, len_keep);
        }

        Ok(growable.build()?.downcast::<StructArray>()?.clone())
    }
}
