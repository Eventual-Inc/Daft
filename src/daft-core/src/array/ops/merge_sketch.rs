use super::as_arrow::AsArrow;
use super::DaftMergeSketchAggable;
use crate::array::ops::GroupIndices;
use crate::{array::DataArray, datatypes::*};
use arrow2;
use arrow2::array::Array;
use common_error::DaftResult;
use sketches_ddsketch::{Config, DDSketch};

impl DaftMergeSketchAggable for &DataArray<BinaryType> {
    type Output = DaftResult<DataArray<BinaryType>>;

    fn merge_sketch(&self) -> Self::Output {
        let config = Config::defaults();
        let mut sketch = DDSketch::new(config);

        let primitive_arr = self.as_arrow();
        for value in primitive_arr.iter().flatten() {
            let str = std::str::from_utf8(value).unwrap();
            let s: DDSketch = serde_json::from_str(str).unwrap();
            sketch.merge(&s).unwrap();
        }

        let sketch_str = &serde_json::to_string(&sketch);

        let result = match sketch_str {
            Ok(s) => Some(s.as_bytes()),
            Err(..) => None,
        };

        let arrow_array = Box::new(arrow2::array::BinaryArray::<i64>::from([result]));

        DataArray::new(self.field.clone(), arrow_array)
    }

    fn grouped_merge_sketch(&self, groups: &GroupIndices) -> Self::Output {
        let arrow_array = self.as_arrow();
        let sketch_per_group = if arrow_array.null_count() > 0 {
            let sketches = groups.iter().map(|g| {
                let sketch = g.iter().fold(None, |acc, index| {
                    let idx = *index as usize;
                    match (acc, arrow_array.is_null(idx)) {
                        (acc, true) => acc,
                        (None, false) => {
                            let str = std::str::from_utf8(arrow_array.value(idx)).unwrap();
                            let sketch: DDSketch = serde_json::from_str(str).unwrap();
                            Some(sketch)
                        }
                        (Some(mut acc), false) => {
                            let str = std::str::from_utf8(arrow_array.value(idx)).unwrap();
                            let sketch: DDSketch = serde_json::from_str(str).unwrap();
                            acc.merge(&sketch).unwrap();
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
        } else {
            let sketches = groups.iter().map(|g| {
                let sketch = g.iter().fold(None, |acc, index| {
                    let idx = *index as usize;
                    match (acc, arrow_array.is_null(idx)) {
                        (acc, true) => acc,
                        (None, false) => {
                            let str = std::str::from_utf8(arrow_array.value(idx)).unwrap();
                            let sketch: DDSketch = serde_json::from_str(str).unwrap();
                            Some(sketch)
                        }
                        (Some(mut acc), false) => {
                            let str = std::str::from_utf8(arrow_array.value(idx)).unwrap();
                            let sketch: DDSketch = serde_json::from_str(str).unwrap();
                            acc.merge(&sketch).unwrap();
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

        DataArray::new(self.field.clone(), sketch_per_group)
    }
}
