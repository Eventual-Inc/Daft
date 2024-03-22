use sketches_ddsketch::{Config, DDSketch};

use arrow2;

use crate::{array::DataArray, datatypes::*};

use common_error::DaftResult;

use super::DaftApproxQuantileAggable;

use super::as_arrow::AsArrow;

use crate::array::ops::GroupIndices;
impl DaftApproxQuantileAggable for &DataArray<BinaryType> {
    type Output = DaftResult<DataArray<Float64Type>>;

    fn approx_quantile(&self) -> Self::Output {
        let config = Config::defaults();
        let mut sketch = DDSketch::new(config);

        let primitive_arr = self.as_arrow();
        for value in primitive_arr.iter() {
            let str = std::str::from_utf8(value.unwrap()).unwrap();
            println!("VALUE2 {}", str);
            let s: DDSketch = serde_json::from_str(str)?;
            sketch.merge(&s).unwrap();
        }

        let median = sketch.quantile(0.50).unwrap();
        println!("MEDIAN {:?}", median);

        let arrow_array = Box::new(arrow2::array::PrimitiveArray::from([median]));
        println!("ARROW_ARRAY2");

        DataArray::new(
            Into::into(Field::new(&self.field.name, DataType::Float64)),
            arrow_array,
        )
    }

    fn grouped_approx_quantile(&self, _groups: &GroupIndices) -> Self::Output {
        panic!("TODO")
    }
}
