use common_error::DaftError;

use crate::datatypes::DataType;
use crate::datatypes::Field;
use crate::utils::sketch::Sketch;
use crate::{array::DataArray, datatypes::DaftArrowBackedType, datatypes::Float64Array};

use arrow2::array::Array;
use arrow2::array::BinaryArray;
use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

impl<T: DaftArrowBackedType> DataArray<T>
where
    T: DaftArrowBackedType,
{
    pub fn sketch_quantile(&self, q: &Float64Array) -> DaftResult<Float64Array> {
        if q.len() != 1 || q.get(0).is_none() {
            return Err(DaftError::ValueError(String::from(
                "q parameter of sketch_quantile must be defined",
            )));
        }

        let quantile = q.get(0).unwrap();
        let primitive_arr: &BinaryArray<i64> = self.data().as_any().downcast_ref().unwrap();
        let quantiles_arr = if primitive_arr.null_count() > 0 {
            primitive_arr
                .iter()
                .map(|value| match value {
                    None => Ok(None),
                    Some(v) => Sketch::from_binary(v)?.quantile(quantile),
                })
                .collect::<DaftResult<Vec<_>>>()?
        } else {
            primitive_arr
                .values_iter()
                .map(|value| Sketch::from_binary(value)?.quantile(quantile))
                .collect::<DaftResult<Vec<_>>>()?
        };
        let result_arr = PrimitiveArray::from_trusted_len_iter(quantiles_arr.into_iter());

        DataArray::new(
            Field::new(&self.field.name, DataType::Float64).into(),
            Box::new(result_arr),
        )
    }
}
