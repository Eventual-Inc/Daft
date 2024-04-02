use common_error::DaftError;

use crate::datatypes::DataType;
use crate::datatypes::Field;
use crate::utils::sketch::sketch_from_binary;
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
        if q.len() != 1 {
            return Err(DaftError::ValueError(String::from(
                "q parameter of sketch_quantile must be defined",
            )));
        }

        let primitive_arr: &BinaryArray<i64> = self.data().as_any().downcast_ref().unwrap();
        let quantiles_arr = if primitive_arr.null_count() > 0 {
            primitive_arr
                .iter()
                .map(|value| match value {
                    None => Ok(None),
                    Some(v) => {
                        let s = sketch_from_binary(v)?;
                        match s.quantile(q.get(0).unwrap()) {
                            Ok(q) => Ok(q),
                            Err(err) => Err(DaftError::ValueError(err.to_string())),
                        }
                    }
                })
                .collect::<DaftResult<Vec<_>>>()?
        } else {
            primitive_arr
                .values_iter()
                .map(|value| {
                    let s = sketch_from_binary(value)?;
                    match s.quantile(q.get(0).unwrap()) {
                        Ok(q) => Ok(q),
                        Err(err) => Err(DaftError::ValueError(err.to_string())),
                    }
                })
                .collect::<DaftResult<Vec<_>>>()?
        };

        let result_arr = PrimitiveArray::from_trusted_len_iter(quantiles_arr.into_iter());

        DataArray::new(
            Field::new(&self.field.name, DataType::Float64).into(),
            Box::new(result_arr),
        )
    }
}
