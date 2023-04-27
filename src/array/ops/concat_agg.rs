use std::vec;

use arrow2::{array::Array, offset::OffsetsBuffer};

use crate::{array::BaseArray, datatypes::ListArray, error::DaftResult};

use super::{downcast::Downcastable, DaftConcatAggable};

impl DaftConcatAggable for ListArray {
    type Output = DaftResult<Self>;
    fn concat(&self) -> Self::Output {
        let array = self.downcast();
        if array.null_count() == 0 {
            let values = array.values();
            let new_offsets = OffsetsBuffer::<i64>::try_from(vec![0, values.len() as i64])?;
            let result = Box::new(arrow2::array::ListArray::<i64>::new(
                self.data_type().to_arrow()?,
                new_offsets,
                values.clone(),
                None,
            ));
            return ListArray::new(self.field.clone(), result);
        }

        let valid_arrays = array.into_iter().flatten().collect::<Vec<_>>();
        let result = arrow2::compute::concatenate::concatenate(
            valid_arrays
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        ListArray::new(self.field.clone(), result)
    }
    fn grouped_concat(&self, groups: &super::GroupIndices) -> Self::Output {
        let child_array = self.downcast();

        let old_offsets = child_array.offsets();
        let mut offsets = Vec::with_capacity(groups.len() + 1);
        offsets.push(0);

        for g in groups {
            let total_elems: i64 = g
                .iter()
                .map(|g_idx| {
                    let g_idx = *g_idx as usize;
                    old_offsets.get(g_idx + 1 as usize).unwrap()
                        - old_offsets.get(g_idx as usize).unwrap()
                })
                .sum();

            offsets.push(offsets.last().unwrap() + total_elems as i64);
        }

        let total_capacity = *offsets.last().unwrap();

        let offsets: OffsetsBuffer<i64> = arrow2::offset::OffsetsBuffer::try_from(offsets)?;

        let mut growable =
            arrow2::array::growable::make_growable(&[child_array], true, total_capacity as usize);
        for g in groups {
            for idx in g {
                let idx = *idx as usize;
                let start = *old_offsets.get(idx).unwrap();
                let len = old_offsets.get(idx + 1).unwrap() - start;
                growable.extend(0, start as usize, len as usize);
            }
        }

        let nested_array = Box::new(arrow2::array::ListArray::<i64>::try_new(
            self.data_type().to_arrow()?,
            offsets,
            growable.as_box(),
            None,
        )?);

        ListArray::new(self.field.clone(), nested_array)
    }
}
