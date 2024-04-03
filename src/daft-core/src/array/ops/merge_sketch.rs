use super::as_arrow::AsArrow;
use super::DaftMergeSketchAggable;
use crate::array::ops::GroupIndices;
use crate::utils::sketch::Sketch;
use crate::{array::DataArray, datatypes::*};
use arrow2;
use arrow2::array::Array;
use common_error::{DaftError, DaftResult};

impl DaftMergeSketchAggable for &DataArray<BinaryType> {
    type Output = DaftResult<DataArray<BinaryType>>;

    fn merge_sketch(&self) -> Self::Output {
        let primitive_arr = self.as_arrow();
        let sketch = if primitive_arr.null_count() > 0 {
            primitive_arr
                .iter()
                .try_fold(None, |acc, value| match (acc, value) {
                    (acc, None) => Ok::<_, DaftError>(acc),
                    (None, Some(v)) => Ok(Some(Sketch::from_binary(v)?)),
                    (Some(mut acc), Some(v)) => {
                        let s = Sketch::from_binary(v)?;
                        acc.merge(&s)?;
                        Ok(Some(acc))
                    }
                })?
        } else {
            Some(
                primitive_arr
                    .values_iter()
                    .try_fold(Sketch::new(), |mut acc, value| {
                        let s = Sketch::from_binary(value)?;
                        acc.merge(&s)?;
                        Ok::<_, DaftError>(acc)
                    })?,
            )
        };

        let binary = match sketch {
            Some(s) => Some(s.to_binary()?),
            None => None,
        };

        let arrow_array = Box::new(arrow2::array::BinaryArray::<i64>::from([binary]));

        DataArray::new(self.field.clone(), arrow_array)
    }

    fn grouped_merge_sketch(&self, groups: &GroupIndices) -> Self::Output {
        let arrow_array = self.as_arrow();
        let sketch_per_group = if arrow_array.null_count() > 0 {
            let sketches: Vec<Option<Vec<u8>>> = groups
                .iter()
                .map(|g| {
                    let sketch = g.iter().try_fold(None, |acc, index| {
                        let idx = *index as usize;
                        match (acc, arrow_array.is_null(idx)) {
                            (acc, true) => Ok::<_, DaftError>(acc),
                            (None, false) => Ok(Some(Sketch::from_binary(arrow_array.value(idx))?)),
                            (Some(mut acc), false) => {
                                let sketch = Sketch::from_binary(arrow_array.value(idx))?;
                                acc.merge(&sketch)?;
                                Ok(Some(acc))
                            }
                        }
                    })?;

                    match sketch {
                        Some(s) => Ok(Some(s.to_binary()?)),
                        None => Ok(None),
                    }
                })
                .collect::<DaftResult<Vec<_>>>()?;

            Box::new(arrow2::array::BinaryArray::<i64>::from_trusted_len_iter(
                sketches.into_iter(),
            ))
        } else {
            let sketches: Vec<Vec<u8>> = groups
                .iter()
                .map(|g| {
                    let sketch = g.iter().try_fold(Sketch::new(), |mut acc, index| {
                        let idx = *index as usize;
                        let sketch = Sketch::from_binary(arrow_array.value(idx))?;
                        acc.merge(&sketch)?;
                        Ok::<_, DaftError>(acc)
                    })?;

                    sketch.to_binary()
                })
                .collect::<DaftResult<Vec<_>>>()?;

            Box::new(
                arrow2::array::BinaryArray::<i64>::from_trusted_len_values_iter(
                    sketches.into_iter(),
                ),
            )
        };

        DataArray::new(self.field.clone(), sketch_per_group)
    }
}
