use crate::datatypes::DataType;
use crate::datatypes::Field;
use crate::{array::DataArray, array::StructArray, datatypes::Float64Array};

use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

impl StructArray {
    pub fn sketch_percentile(&self, q: &Float64Array) -> DaftResult<Float64Array> {
        let quantile = q
            .get(0)
            .expect("q parameter of sketch_percentile must have one non-null element");
        let sketches_array = daft_sketch::from_arrow2(self.to_arrow())?;

        let percentiles_arr = sketches_array
            .iter()
            .map(|sketch| match sketch {
                None => Ok(None),
                Some(s) => Ok(s.quantile(quantile)?),
            })
            .collect::<DaftResult<Vec<Option<f64>>>>()?;

        let result_arr = PrimitiveArray::from_trusted_len_iter(percentiles_arr.into_iter());

        DataArray::new(
            Field::new(&self.field.name, DataType::Float64).into(),
            Box::new(result_arr),
        )
    }
}
