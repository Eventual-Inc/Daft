use sketches_ddsketch::DDSketch;

use crate::datatypes::DataType;
use crate::datatypes::Field;
use crate::{array::DataArray, datatypes::DaftArrowBackedType, datatypes::Float64Array};

use arrow2::array::BinaryArray;
use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

impl<T: DaftArrowBackedType> DataArray<T>
where
    T: DaftArrowBackedType,
{
    pub fn approx_quantile(&self, q: &Float64Array) -> DaftResult<Float64Array> {
        let arr: &BinaryArray<i64> = self.data().as_any().downcast_ref().unwrap();
        let result_arr = PrimitiveArray::from_trusted_len_values_iter(arr.values_iter().map(|v| {
            let str = std::str::from_utf8(v).unwrap();
            // println!("VALUE2 {}", str);
            let sketch: DDSketch = serde_json::from_str(str).unwrap();
            sketch.quantile(q.get(0).unwrap()).unwrap().unwrap() // TODO handle
        }))
        .with_validity(arr.validity().cloned());

        DataArray::new(
            Field::new(&self.field.name, DataType::Float64).into(),
            Box::new(result_arr),
        )
    }
}
