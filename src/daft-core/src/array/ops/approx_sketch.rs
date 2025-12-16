use common_error::DaftResult;
use daft_arrow::array::Array;
use sketches_ddsketch::{Config, DDSketch};

use super::{DaftApproxSketchAggable, as_arrow::AsArrow, from_arrow::FromArrow};
use crate::{
    array::{StructArray, ops::GroupIndices},
    datatypes::*,
};

impl DaftApproxSketchAggable for &DataArray<Float64Type> {
    type Output = DaftResult<StructArray>;

    fn approx_sketch(&self) -> Self::Output {
        let primitive_arr = self.as_arrow2();
        let arrow_array = if primitive_arr.is_empty() {
            daft_sketch::into_arrow2(vec![])
        } else if primitive_arr.null_count() > 0 {
            let sketch = primitive_arr
                .iter()
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
            daft_sketch::into_arrow2(vec![sketch])
        } else {
            let sketch = primitive_arr.values().iter().fold(
                DDSketch::new(Config::defaults()),
                |mut acc, value| {
                    acc.add(*value);
                    acc
                },
            );

            daft_sketch::into_arrow2(vec![Some(sketch)])
        };

        StructArray::from_arrow(
            Field::new(
                &self.field.name,
                DataType::from(&*daft_sketch::ARROW2_DDSKETCH_DTYPE),
            )
            .into(),
            arrow_array,
        )
    }

    fn grouped_approx_sketch(&self, groups: &GroupIndices) -> Self::Output {
        let arrow_array = self.as_arrow2();
        let sketch_per_group = if arrow_array.is_empty() {
            daft_sketch::into_arrow2(vec![])
        } else if arrow_array.null_count() > 0 {
            let sketches: Vec<Option<DDSketch>> = groups
                .iter()
                .map(|g| {
                    g.iter().fold(None, |acc, index| {
                        let idx = *index as usize;
                        match (acc, arrow_array.is_null(idx)) {
                            (acc, true) => acc,
                            (None, false) => {
                                let mut sketch = DDSketch::new(Config::defaults());
                                sketch.add(arrow_array.value(idx));
                                Some(sketch)
                            }
                            (Some(mut acc), false) => {
                                acc.add(arrow_array.value(idx));
                                Some(acc)
                            }
                        }
                    })
                })
                .collect();

            daft_sketch::into_arrow2(sketches)
        } else {
            let sketches = groups
                .iter()
                .map(|g| {
                    Some(
                        g.iter()
                            .fold(DDSketch::new(Config::defaults()), |mut acc, index| {
                                let idx = *index as usize;
                                acc.add(arrow_array.value(idx));
                                acc
                            }),
                    )
                })
                .collect();

            daft_sketch::into_arrow2(sketches)
        };

        StructArray::from_arrow(
            Field::new(
                &self.field.name,
                DataType::from(&*daft_sketch::ARROW2_DDSKETCH_DTYPE),
            )
            .into(),
            sketch_per_group,
        )
    }
}
