use super::as_arrow::AsArrow;
use super::DaftApproxSketchAggable;
use crate::array::ops::GroupIndices;
use crate::{array::DataArray, datatypes::*};
use arrow2;
use arrow2::array::Array;
use common_error::DaftResult;
use sketches_ddsketch::{Config, DDSketch};

impl DaftApproxSketchAggable for &DataArray<Float64Type> {
    type Output = DaftResult<DataArray<BinaryType>>;

    fn approx_sketch(&self) -> Self::Output {
        let config = Config::defaults();
        let mut sketch = DDSketch::new(config);

        let primitive_arr = self.as_arrow();
        for value in primitive_arr.iter().flatten() {
            sketch.add(*value);
        }

        let sketch_str = &serde_json::to_string(&sketch);

        let result = match sketch_str {
            Ok(s) => Some(s.as_bytes()),
            Err(..) => None,
        };

        let arrow_array = Box::new(arrow2::array::BinaryArray::<i64>::from([result]));

        DataArray::new(
            Field::new(&self.field.name, DataType::Binary).into(),
            arrow_array,
        )
    }

    fn grouped_approx_sketch(&self, groups: &GroupIndices) -> Self::Output {
        let arrow_array = self.as_arrow();
        let sketch_per_group = if arrow_array.null_count() > 0 {
            let sketches = groups.iter().map(|g| {
                let sketch = g.iter().fold(None, |acc, index| {
                    let idx = *index as usize;
                    match (acc, arrow_array.is_null(idx)) {
                        (acc, true) => acc,
                        (None, false) => {
                            let config = Config::defaults();
                            let mut sketch = DDSketch::new(config);
                            sketch.add(arrow_array.value(idx));
                            Some(sketch)
                        }
                        (Some(mut acc), false) => {
                            acc.add(arrow_array.value(idx));
                            Some(acc)
                        }
                    }
                });

                let sketch_str = serde_json::to_string(&sketch);
                println!("SKECTH {:?}", sketch_str);

                match sketch_str {
                    Ok(s) => Some(s),
                    Err(..) => None,
                }
            });

            Box::new(arrow2::array::BinaryArray::<i64>::from_trusted_len_iter(
                sketches,
            ))
        } else {
            let sketches = groups.iter().map(|g| {
                let sketch = g.iter().fold(None, |acc, index| {
                    let idx = *index as usize;
                    match (acc, arrow_array.is_null(idx)) {
                        (acc, true) => acc,
                        (None, false) => {
                            let config = Config::defaults();
                            let mut sketch = DDSketch::new(config);
                            sketch.add(arrow_array.value(idx));
                            Some(sketch)
                        }
                        (Some(mut acc), false) => {
                            acc.add(arrow_array.value(idx));
                            Some(acc)
                        }
                    }
                });

                let sketch_str = serde_json::to_string(&sketch);

                match sketch_str {
                    Ok(s) => Some(s),
                    Err(..) => None,
                }
            });

            Box::new(arrow2::array::BinaryArray::<i64>::from_trusted_len_iter(
                sketches,
            ))
        };

        DataArray::new(
            Field::new(&self.field.name, DataType::Binary).into(),
            sketch_per_group,
        )
    }
}
