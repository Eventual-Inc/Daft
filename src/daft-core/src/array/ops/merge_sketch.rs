use super::from_arrow::FromArrow;
use super::DaftMergeSketchAggable;
use crate::array::ops::GroupIndices;
use crate::{array::StructArray, datatypes::*};
use common_error::{DaftError, DaftResult};

impl DaftMergeSketchAggable for &StructArray {
    type Output = DaftResult<StructArray>;

    fn merge_sketch(&self) -> Self::Output {
        let sketches_array = daft_sketch::from_arrow2(self.to_arrow())?;
        let sketch =
            sketches_array
                .into_iter()
                .try_fold(None, |acc, value| match (acc, value) {
                    (acc, None) => Ok::<_, DaftError>(acc),
                    (None, Some(v)) => Ok(Some(v)),
                    (Some(mut acc), Some(v)) => {
                        acc.merge(&v).map_err(|err| {
                            DaftError::ComputeError(format!("Error merging sketches: {}", err))
                        })?;
                        Ok(Some(acc))
                    }
                })?;
        let arrow_array = daft_sketch::into_arrow2(vec![sketch]);

        StructArray::from_arrow(
            Field::new(
                &self.field.name,
                DataType::from(&*daft_sketch::ARROW2_DDSKETCH_DTYPE),
            )
            .into(),
            arrow_array,
        )
    }

    fn grouped_merge_sketch(&self, groups: &GroupIndices) -> Self::Output {
        let sketches_array = daft_sketch::from_arrow2(self.to_arrow())?;

        let sketch_per_group = groups
            .iter()
            .map(|g| {
                g.iter().try_fold(None, |acc, index| {
                    let idx = *index as usize;
                    match (acc, sketches_array[idx].is_none()) {
                        (acc, true) => Ok::<_, DaftError>(acc),
                        (None, false) => Ok(sketches_array[idx].clone()),
                        (Some(mut acc), false) => {
                            acc.merge(sketches_array[idx].as_ref().unwrap())
                                .map_err(|err| {
                                    DaftError::ComputeError(format!(
                                        "Error merging sketches: {}",
                                        err
                                    ))
                                })?;
                            Ok(Some(acc))
                        }
                    }
                })
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let arrow_array = daft_sketch::into_arrow2(sketch_per_group);

        StructArray::from_arrow(
            Field::new(
                &self.field.name,
                DataType::from(&*daft_sketch::ARROW2_DDSKETCH_DTYPE),
            )
            .into(),
            arrow_array,
        )
    }
}
