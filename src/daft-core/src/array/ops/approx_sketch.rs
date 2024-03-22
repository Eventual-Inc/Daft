use sketches_ddsketch::{Config, DDSketch};

use arrow2;

use crate::{array::DataArray, datatypes::*};

use common_error::DaftResult;

use super::DaftApproxSketchAggable;

use super::as_arrow::AsArrow;

use crate::array::ops::GroupIndices;
impl DaftApproxSketchAggable for &DataArray<Float64Type> {
    type Output = DaftResult<DataArray<BinaryType>>;

    fn approx_sketch(&self) -> Self::Output {
        let config = Config::defaults();
        let mut sketch = DDSketch::new(config);

        let primitive_arr = self.as_arrow();
        for value in primitive_arr.iter() {
            match value {
                Some(v) => {
                    println!("VALUE {}", v);
                    sketch.add(*v)
                }
                None => {
                    println!("VALUE __None__");
                }
            }
        }

        let sketch_str = &serde_json::to_string(&sketch);
        println!("SKETCH_STR {:?}", sketch_str);

        let result = match sketch_str {
            Ok(s) => Some(s.as_bytes()),
            Err(..) => None,
        };

        let arrow_array = Box::new(arrow2::array::BinaryArray::<i64>::from([result]));
        println!("ARROW_ARRAY");

        DataArray::new(
            Into::into(Field::new(&self.field.name, DataType::Binary)),
            arrow_array,
        )
    }

    fn grouped_approx_sketch(&self, _groups: &GroupIndices) -> Self::Output {
        panic!("TODO")
    }
}
