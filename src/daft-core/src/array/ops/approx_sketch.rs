use super::as_arrow::AsArrow;
use super::DaftApproxSketchAggable;
use crate::array::ops::GroupIndices;
use crate::utils::sketch::Sketch;
use crate::{array::DataArray, datatypes::*};
use arrow2;
use arrow2::array::Array;
use common_error::DaftResult;

impl DaftApproxSketchAggable for &DataArray<Float64Type> {
    type Output = DaftResult<DataArray<BinaryType>>;

    fn approx_sketch(&self) -> Self::Output {
        let primitive_arr = self.as_arrow();
        let sketch = if primitive_arr.null_count() > 0 {
            primitive_arr
                .iter()
                .fold(None, |acc, value| match (acc, value) {
                    (acc, None) => acc,
                    (None, Some(v)) => Some(Sketch::from_value(*v)),
                    (Some(mut acc), Some(v)) => {
                        acc.add(*v);
                        Some(acc)
                    }
                })
        } else {
            Some(
                primitive_arr
                    .values_iter()
                    .fold(Sketch::new(), |mut acc, value| {
                        acc.add(*value);
                        acc
                    }),
            )
        };

        let binary = match sketch {
            Some(s) => Some(s.to_binary()?),
            None => None,
        };

        let arrow_array = Box::new(arrow2::array::BinaryArray::<i64>::from([binary]));

        DataArray::new(
            Field::new(&self.field.name, DataType::Binary).into(),
            arrow_array,
        )
    }

    fn grouped_approx_sketch(&self, groups: &GroupIndices) -> Self::Output {
        let arrow_array = self.as_arrow();
        let sketch_per_group = if arrow_array.null_count() > 0 {
            let sketches: Vec<Option<Vec<u8>>> = groups
                .iter()
                .map(|g| {
                    let sketch = g.iter().fold(None, |acc, index| {
                        let idx = *index as usize;
                        match (acc, arrow_array.is_null(idx)) {
                            (acc, true) => acc,
                            (None, false) => Some(Sketch::from_value(arrow_array.value(idx))),
                            (Some(mut acc), false) => {
                                acc.add(arrow_array.value(idx));
                                Some(acc)
                            }
                        }
                    });

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
                    let sketch = g.iter().fold(Sketch::new(), |mut acc, index| {
                        let idx = *index as usize;
                        acc.add(arrow_array.value(idx));
                        acc
                    });

                    sketch.to_binary()
                })
                .collect::<DaftResult<Vec<_>>>()?;

            Box::new(
                arrow2::array::BinaryArray::<i64>::from_trusted_len_values_iter(
                    sketches.into_iter(),
                ),
            )
        };

        DataArray::new(
            Field::new(&self.field.name, DataType::Binary).into(),
            sketch_per_group,
        )
    }
}
