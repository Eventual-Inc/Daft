use std::borrow::Cow;

use crate::{
    array::{
        growable::{Growable, GrowableArray},
        DataArray, FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::{BooleanArray, DaftArrayType, DaftArrowBackedType},
    DataType,
};
use arrow2::bitmap::utils::SlicesIterator;
use common_error::DaftResult;

use super::{as_arrow::AsArrow, full::FullNull};

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

fn generic_filter<Arr>(
    arr: &Arr,
    mask: &BooleanArray,
    arr_name: &str,
    arr_dtype: &DataType,
) -> DaftResult<Arr>
where
    Arr: FullNull + Clone + GrowableArray + DaftArrayType,
{
    let keep_bitmap = match mask.as_arrow().validity() {
        None => Cow::Borrowed(mask.as_arrow().values()),
        Some(validity) => Cow::Owned(mask.as_arrow().values() & validity),
    };

    let num_invalid = keep_bitmap.as_ref().unset_bits();
    if num_invalid == 0 {
        return Ok(arr.clone());
    } else if num_invalid == mask.len() {
        return Ok(Arr::empty(arr_name, arr_dtype));
    }

    let slice_iter = SlicesIterator::new(keep_bitmap.as_ref());
    let mut growable =
        Arr::make_growable(arr_name, arr_dtype, vec![arr], false, slice_iter.slots());

    for (start_keep, len_keep) in slice_iter {
        growable.extend(0, start_keep, len_keep);
    }

    Ok(growable.build()?.downcast::<Arr>()?.clone())
}

impl ListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        generic_filter(self, mask, self.name(), self.data_type())
    }
}

impl FixedSizeListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        generic_filter(self, mask, self.name(), self.data_type())
    }
}

impl StructArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        generic_filter(self, mask, self.name(), self.data_type())
    }
}
