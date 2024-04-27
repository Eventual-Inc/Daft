use std::{iter::repeat, sync::Arc};

use crate::{
    array::{ListArray, StructArray},
    count_mode::CountMode,
    datatypes::*,
};
use common_error::DaftResult;

use super::{DaftCountAggable, GroupIndices};

/// Helper to perform a grouped count on a validity map of type arrow2::bitmap::Bitmap
fn grouped_count_arrow_bitmap(
    groups: &GroupIndices,
    mode: &CountMode,
    arrow_bitmap: Option<&arrow2::bitmap::Bitmap>,
) -> Vec<u64> {
    match mode {
        CountMode::All => groups.iter().map(|g| g.len() as u64).collect(),
        CountMode::Valid => match arrow_bitmap {
            None => groups.iter().map(|g| g.len() as u64).collect(), // Equivalent to CountMode::All
            Some(validity) => groups
                .iter()
                .map(|g| g.iter().map(|i| validity.get_bit(*i as usize) as u64).sum())
                .collect(),
        },
        CountMode::Null => match arrow_bitmap {
            None => repeat(0).take(groups.len()).collect(), // None of the values are Null
            Some(validity) => groups
                .iter()
                .map(|g| {
                    g.iter()
                        .map(|i| !validity.get_bit(*i as usize) as u64)
                        .sum()
                })
                .collect(),
        },
    }
}

/// Helper to perform a count on a validity map of type arrow2::bitmap::Bitmap
fn count_arrow_bitmap(
    mode: &CountMode,
    arrow_bitmap: Option<&arrow2::bitmap::Bitmap>,
    arr_len: usize,
) -> u64 {
    match mode {
        CountMode::All => arr_len as u64,
        CountMode::Valid => match arrow_bitmap {
            None => arr_len as u64,
            Some(validity) => (validity.len() - validity.unset_bits()) as u64,
        },
        CountMode::Null => match arrow_bitmap {
            None => 0,
            Some(validity) => validity.unset_bits() as u64,
        },
    }
}

impl<T> DaftCountAggable for &DataArray<T>
where
    T: DaftPhysicalType,
{
    type Output = DaftResult<DataArray<UInt64Type>>;

    fn count(&self, mode: CountMode) -> Self::Output {
        let count = if self.data_type() == &DataType::Null {
            match &mode {
                CountMode::All => self.len() as u64,
                CountMode::Valid => 0u64,
                CountMode::Null => self.len() as u64,
            }
        } else {
            count_arrow_bitmap(&mode, self.data().validity(), self.len())
        };
        let result_arrow_array = Box::new(arrow2::array::PrimitiveArray::from([Some(count)]));
        DataArray::<UInt64Type>::new(
            Arc::new(Field::new(self.field.name.clone(), DataType::UInt64)),
            result_arrow_array,
        )
    }
    fn grouped_count(&self, groups: &GroupIndices, mode: CountMode) -> Self::Output {
        let counts_per_group: Vec<u64> = if self.data_type() == &DataType::Null {
            match &mode {
                CountMode::All => groups.iter().map(|g| g.len() as u64).collect(),
                CountMode::Valid => repeat(0).take(groups.len()).collect(),
                CountMode::Null => groups.iter().map(|g| g.len() as u64).collect(),
            }
        } else {
            grouped_count_arrow_bitmap(groups, &mode, self.data().validity())
        };
        Ok(DataArray::<UInt64Type>::from((
            self.field.name.as_ref(),
            counts_per_group,
        )))
    }
}

macro_rules! impl_daft_count_aggable_nested_array {
    ($arr:ident) => {
        impl DaftCountAggable for &$arr {
            type Output = DaftResult<DataArray<UInt64Type>>;

            fn count(&self, mode: CountMode) -> Self::Output {
                let count = count_arrow_bitmap(&mode, self.validity(), self.len());
                let result_arrow_array =
                    Box::new(arrow2::array::PrimitiveArray::from([Some(count)]));
                DataArray::<UInt64Type>::new(
                    Arc::new(Field::new(self.field.name.clone(), DataType::UInt64)),
                    result_arrow_array,
                )
            }

            fn grouped_count(&self, groups: &GroupIndices, mode: CountMode) -> Self::Output {
                let counts_per_group: Vec<_> =
                    grouped_count_arrow_bitmap(groups, &mode, self.validity());
                Ok(DataArray::<UInt64Type>::from((
                    self.field.name.as_ref(),
                    counts_per_group,
                )))
            }
        }
    };
}

impl_daft_count_aggable_nested_array!(FixedSizeListArray);
impl_daft_count_aggable_nested_array!(ListArray);
impl_daft_count_aggable_nested_array!(StructArray);
