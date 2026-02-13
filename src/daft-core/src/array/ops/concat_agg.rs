#![allow(deprecated, reason = "arrow2->arrow migration")]
use arrow::buffer::OffsetBuffer;
use common_error::DaftResult;
use daft_arrow::{
    array::{Array, Utf8Array},
    offset::OffsetsBuffer,
    types::Index,
};

use super::{DaftConcatAggable, as_arrow::AsArrow};
use crate::{
    array::{DataArray, ListArray},
    datatypes::{DataType, Field},
    prelude::Utf8Type,
    series::Series,
};

impl DaftConcatAggable for ListArray {
    type Output = DaftResult<Self>;
    fn concat(&self) -> Self::Output {
        if self.null_count() == 0 {
            let new_offsets = OffsetBuffer::new(vec![0, *self.offsets().last().unwrap()].into());

            return Ok(Self::new(
                self.field.clone(),
                self.flat_child.clone(),
                new_offsets,
                None,
            ));
        }

        // Only the all-null case leads to a null result. If any single element is non-null (e.g. an empty list []),
        // The concat will successfully return a single non-null element.
        let new_nulls = match self.nulls() {
            Some(nulls) if nulls.null_count() == self.len() => {
                Some(daft_arrow::buffer::NullBuffer::new_null(1))
            }
            _ => None,
        };

        // Collect slices of the child array where the parent is valid, then concatenate them
        let child_slices: Vec<Series> = self
            .nulls()
            .unwrap()
            .valid_slices()
            .map(|(start_valid, end_valid)| {
                let child_start = self.offsets()[start_valid];
                let child_end = self.offsets()[end_valid];
                self.flat_child
                    .slice(child_start as _, child_end as _)
                    .unwrap()
            })
            .collect();

        let new_child = if child_slices.is_empty() {
            self.flat_child.slice(0, 0)?
        } else {
            Series::concat(&child_slices.iter().collect::<Vec<_>>())?
        };
        let new_offsets = OffsetBuffer::new(vec![0i64, new_child.len() as i64].into());

        Ok(Self::new(
            self.field.clone(),
            new_child,
            new_offsets,
            new_nulls,
        ))
    }

    fn grouped_concat(&self, groups: &super::GroupIndices) -> Self::Output {
        let all_valid = self.null_count() == 0;

        // Collect all child slices for each group
        let mut all_slices: Vec<Series> = vec![];
        let mut group_lens: Vec<usize> = vec![];
        let mut group_valids: Vec<bool> = vec![];

        for group in groups {
            let mut group_valid = false;
            let mut group_len: usize = 0;
            for idx in group {
                if all_valid || self.is_valid(idx.to_usize()) {
                    let start = self.offsets()[*idx as usize] as usize;
                    let end = self.offsets()[*idx as usize + 1] as usize;
                    let len = end - start;
                    if len > 0 {
                        all_slices.push(self.flat_child.slice(start, end)?);
                    }
                    group_len += len;
                    group_valid = true;
                }
            }
            group_valids.push(group_valid);
            group_lens.push(if group_valid { group_len } else { 0 });
        }

        let new_child = if all_slices.is_empty() {
            self.flat_child.slice(0, 0)?
        } else {
            Series::concat(&all_slices.iter().collect::<Vec<_>>())?
        };

        let new_offsets = OffsetBuffer::from_lengths(group_lens.iter().copied());
        let new_validities = if all_valid {
            None
        } else {
            Some(daft_arrow::buffer::NullBuffer::from(group_valids))
        };

        Ok(Self::new(
            self.field.clone(),
            new_child,
            new_offsets,
            new_validities,
        ))
    }
}

impl DaftConcatAggable for DataArray<Utf8Type> {
    type Output = DaftResult<Self>;

    fn concat(&self) -> Self::Output {
        let new_nulls = match self.nulls() {
            Some(nulls) if nulls.null_count() == self.len() => {
                Some(daft_arrow::buffer::NullBuffer::new_null(1))
            }
            _ => None,
        };

        let arrow_array = self.as_arrow2();
        let new_offsets = OffsetsBuffer::<i64>::try_from(vec![0, *arrow_array.offsets().last()])?;
        let output = Utf8Array::new(
            arrow_array.data_type().clone(),
            new_offsets,
            arrow_array.values().clone(),
            daft_arrow::buffer::wrap_null_buffer(new_nulls),
        );

        let result_box = Box::new(output);
        Self::new(self.field().clone().into(), result_box)
    }

    fn grouped_concat(&self, groups: &super::GroupIndices) -> Self::Output {
        let arrow_array = self.as_arrow2();
        let concat_per_group = if arrow_array.null_count() > 0 {
            Box::new(Utf8Array::<i64>::from_trusted_len_iter(groups.iter().map(
                |g| {
                    let to_concat = g
                        .iter()
                        .filter_map(|index| {
                            let idx = *index as usize;
                            arrow_array.get(idx)
                        })
                        .collect::<Vec<&str>>();
                    if to_concat.is_empty() {
                        None
                    } else {
                        Some(to_concat.concat())
                    }
                },
            )))
        } else {
            Box::new(Utf8Array::from_trusted_len_values_iter(groups.iter().map(
                |g| {
                    g.iter()
                        .map(|index| {
                            let idx = *index as usize;
                            arrow_array.value(idx)
                        })
                        .collect::<String>()
                },
            )))
        };

        Ok(Self::new(
            Field::new(self.field.name.clone(), DataType::Utf8).into(),
            concat_per_group,
        )
        .unwrap())
    }
}

#[cfg(test)]
mod test {
    use std::iter::{self, repeat_n};

    use arrow::buffer::OffsetBuffer;
    use common_error::DaftResult;

    use crate::{
        array::{ListArray, ops::DaftConcatAggable},
        datatypes::{DataType, Field, Int64Array},
        series::IntoSeries,
    };

    #[test]
    fn test_list_concat_agg_all_null() -> DaftResult<()> {
        // [None, None, None]
        let list_array = ListArray::new(
            Field::new("foo", DataType::List(Box::new(DataType::Int64))),
            Int64Array::new(
                Field::new("item", DataType::Int64).into(),
                Box::new(daft_arrow::array::Int64Array::from_iter(iter::empty::<
                    &Option<i64>,
                >(
                ))),
            )
            .unwrap()
            .into_series(),
            OffsetBuffer::new_zeroed(3),
            Some(daft_arrow::buffer::NullBuffer::from_iter(repeat_n(
                false, 3,
            ))),
        );

        // Expected: [None]
        let concatted = list_array.concat()?;
        assert_eq!(concatted.len(), 1);
        assert_eq!(
            concatted.nulls(),
            Some(&daft_arrow::buffer::NullBuffer::from_iter(repeat_n(
                false, 1
            )))
        );
        Ok(())
    }

    #[test]
    fn test_list_concat_agg_with_nulls() -> DaftResult<()> {
        // [[0], [1, 1], [2, None], [None], [], None, None]
        let list_array = ListArray::new(
            Field::new("foo", DataType::List(Box::new(DataType::Int64))),
            Int64Array::new(
                Field::new("item", DataType::Int64).into(),
                Box::new(daft_arrow::array::Int64Array::from_iter(
                    [Some(0), Some(1), Some(1), Some(2), None, None, Some(10000)].iter(),
                )),
            )
            .unwrap()
            .into_series(),
            OffsetBuffer::new(vec![0, 1, 3, 5, 6, 6, 6, 7].into()),
            Some(daft_arrow::buffer::NullBuffer::from(vec![
                true, true, true, true, true, false, false,
            ])),
        );

        // Expected: [[0, 1, 1, 2, None, None]]
        let concatted = list_array.concat()?;
        assert_eq!(concatted.len(), 1);
        assert_eq!(concatted.nulls(), None);
        let element = concatted.get(0).unwrap();
        assert_eq!(
            element
                .downcast::<Int64Array>()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&i64>>>(),
            vec![Some(&0), Some(&1), Some(&1), Some(&2), None, None]
        );
        Ok(())
    }

    #[test]
    fn test_grouped_list_concat_agg() -> DaftResult<()> {
        // [[0], [0, 0], [1, None], [None], [2, None], None, None, None]
        //  |  group0 |  |     group1    |  | group 2     |  group 3   |
        let list_array = ListArray::new(
            Field::new("foo", DataType::List(Box::new(DataType::Int64))),
            Int64Array::new(
                Field::new("item", DataType::Int64).into(),
                Box::new(daft_arrow::array::Int64Array::from_iter(
                    [
                        Some(0),
                        Some(0),
                        Some(0),
                        Some(1),
                        None,
                        None,
                        Some(2),
                        None,
                        Some(1000),
                    ]
                    .iter(),
                )),
            )
            .unwrap()
            .into_series(),
            OffsetBuffer::new(vec![0, 1, 3, 5, 6, 8, 8, 8, 9].into()),
            Some(daft_arrow::buffer::NullBuffer::from(vec![
                true, true, true, true, true, false, false, false,
            ])),
        );

        let concatted =
            list_array.grouped_concat(&vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7]])?;

        // Expected: [[0, 0, 0], [1, None, None], [2, None], None]
        assert_eq!(concatted.len(), 4);
        assert_eq!(
            concatted.nulls(),
            Some(&daft_arrow::buffer::NullBuffer::from(vec![
                true, true, true, false
            ]))
        );

        let element_0 = concatted.get(0).unwrap();
        assert_eq!(
            element_0
                .downcast::<Int64Array>()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&i64>>>(),
            vec![Some(&0), Some(&0), Some(&0)]
        );

        let element_1 = concatted.get(1).unwrap();
        assert_eq!(
            element_1
                .downcast::<Int64Array>()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&i64>>>(),
            vec![Some(&1), None, None]
        );

        let element_2 = concatted.get(2).unwrap();
        assert_eq!(
            element_2
                .downcast::<Int64Array>()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&i64>>>(),
            vec![Some(&2), None]
        );

        let element_3 = concatted.get(3);
        assert!(element_3.is_none());
        Ok(())
    }
}
