use std::sync::Arc;

use common_error::DaftResult;
use sketches_ddsketch::{Config, DDSketch};

use super::{DaftApproxSketchAggable, from_arrow::FromArrow};
use crate::{
    array::{StructArray, ops::GroupIndices},
    datatypes::*,
};

impl DaftApproxSketchAggable for &DataArray<Float64Type> {
    type Output = DaftResult<StructArray>;

    fn approx_sketch(&self) -> Self::Output {
        let arrow_array = if self.is_empty() {
            daft_sketch::into_arrow(vec![])
        } else if self.null_count() > 0 {
            let sketch = self
                .into_iter()
                .fold(None, |acc, value| match (acc, value) {
                    (acc, None) => acc,
                    (None, Some(v)) => {
                        let mut sketch = DDSketch::new(Config::defaults());
                        sketch.add(*v);
                        Some(sketch)
                    }
                    (Some(mut acc), Some(v)) => {
                        acc.add(*v);
                        Some(acc)
                    }
                });
            daft_sketch::into_arrow(vec![sketch])
        } else {
            let sketch =
                self.values()
                    .iter()
                    .fold(DDSketch::new(Config::defaults()), |mut acc, value| {
                        acc.add(*value);
                        acc
                    });

            daft_sketch::into_arrow(vec![Some(sketch)])
        };

        StructArray::from_arrow(
            Arc::new(Field::new(
                &self.field.name,
                DataType::try_from(&*daft_sketch::ARROW_DDSKETCH_DTYPE)?,
            )),
            arrow_array,
        )
    }

    fn grouped_approx_sketch(&self, groups: &GroupIndices) -> Self::Output {
        let sketch_per_group = if self.is_empty() {
            daft_sketch::into_arrow(vec![])
        } else if self.null_count() > 0 {
            let sketches: Vec<Option<DDSketch>> = groups
                .iter()
                .map(|g| {
                    g.iter().fold(None, |acc, index| {
                        let idx = *index as usize;
                        match (acc, self.get(idx)) {
                            (acc, None) => acc,
                            (None, Some(idx)) => {
                                let mut sketch = DDSketch::new(Config::defaults());
                                sketch.add(idx);
                                Some(sketch)
                            }
                            (Some(mut acc), Some(idx)) => {
                                acc.add(idx);
                                Some(acc)
                            }
                        }
                    })
                })
                .collect();

            daft_sketch::into_arrow(sketches)
        } else {
            let sketches = groups
                .iter()
                .map(|g| {
                    Some(
                        g.iter()
                            .fold(DDSketch::new(Config::defaults()), |mut acc, index| {
                                let idx = *index as usize;
                                acc.add(self.get(idx).expect("we already checked null counts"));
                                acc
                            }),
                    )
                })
                .collect();

            daft_sketch::into_arrow(sketches)
        };

        StructArray::from_arrow(
            Arc::new(Field::new(
                &self.field.name,
                DataType::try_from(&*daft_sketch::ARROW_DDSKETCH_DTYPE)?,
            )),
            sketch_per_group,
        )
    }
}
