use std::vec;

use arrow2::{array::Array, offset::OffsetsBuffer};

use crate::{array::BaseArray, datatypes::ListArray, error::DaftResult};

use super::{downcast::Downcastable, DaftConcatAggable};

#[cfg(feature = "python")]
impl DaftConcatAggable for crate::datatypes::PythonArray {
    type Output = DaftResult<Self>;
    fn concat(&self) -> Self::Output {
        todo!()
    }
    fn grouped_concat(&self, _groups: &super::GroupIndices) -> Self::Output {
        todo!()
    }
}

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

        let old_offsets: &OffsetsBuffer<i64> = array.offsets();

        let len = array.len();
        let capacity: i64 = (0..len)
            .map(|i| match array.is_valid(i) {
                false => 0,
                true => old_offsets.get(i + 1_usize).unwrap() - old_offsets.get(i).unwrap(),
            })
            .sum();
        let mut growable = arrow2::array::growable::make_growable(
            &[array.values().as_ref()],
            true,
            capacity as usize,
        );

        (0..len).for_each(|i| {
            if array.is_valid(i) {
                let start = *old_offsets.get(i).unwrap();
                let len = old_offsets.get(i + 1).unwrap() - start;
                growable.extend(0, start as usize, len as usize);
            }
        });

        let nested_array = Box::new(arrow2::array::ListArray::<i64>::try_new(
            self.data_type().to_arrow()?,
            arrow2::offset::OffsetsBuffer::try_from(vec![0, capacity])?,
            growable.as_box(),
            None,
        )?);

        ListArray::new(self.field.clone(), nested_array)
    }

    fn grouped_concat(&self, groups: &super::GroupIndices) -> Self::Output {
        let arrow_array = self.downcast();

        let old_offsets: &OffsetsBuffer<i64> = arrow_array.offsets();
        let mut offsets = Vec::with_capacity(groups.len() + 1);
        offsets.push(0);

        for g in groups {
            let total_elems: i64 = g
                .iter()
                .map(|g_idx| {
                    let g_idx = *g_idx as usize;
                    let is_valid = arrow_array.is_valid(g_idx);
                    match is_valid {
                        false => 0,
                        true => {
                            old_offsets.get(g_idx + 1_usize).unwrap()
                                - old_offsets.get(g_idx).unwrap()
                        }
                    }
                })
                .sum();

            offsets.push(offsets.last().unwrap() + total_elems);
        }

        let total_capacity = *offsets.last().unwrap();

        let offsets: OffsetsBuffer<i64> = arrow2::offset::OffsetsBuffer::try_from(offsets)?;

        let mut growable = arrow2::array::growable::make_growable(
            &[arrow_array.values().as_ref()],
            true,
            total_capacity as usize,
        );
        for g in groups {
            for idx in g {
                let idx = *idx as usize;
                if arrow_array.is_valid(idx) {
                    let start = *old_offsets.get(idx).unwrap();
                    let len = old_offsets.get(idx + 1).unwrap() - start;
                    growable.extend(0, start as usize, len as usize);
                }
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
