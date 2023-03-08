use std::sync::Arc;

use arrow2;

use crate::{array::DataArray, datatypes::{DaftIntegerType, DaftNumericType, NumericNative, Int64Array}, error::DaftResult};

use super::DaftNumericAgg;

impl DaftNumericAgg for &Int64Array {
    type Output = DaftResult<Int64Array>;

    fn sum(&self) -> Self::Output {
        let primitive_arr = self.downcast();

        let result = match primitive_arr.validity() {
            None => Some(primitive_arr.values_iter().sum() as i64),
            Some(_) => primitive_arr.iter().fold(None, |acc, v| match v {
                Some(v) => match acc {
                    Some(acc) => Some(acc + *v as i64),
                    None => Some(*v as i64),
                },
                None => acc,
            }),
        };

        println!("{:?}", result);

        todo!();
        // let scalar = res.unwrap();

        let arrow_array = arrow2::array::PrimitiveArray::from([result]);
        DataArray::new(self.field.clone(), Arc::new(arrow_array))
    }
}

/*
// All other types sum to their own type.
impl<T> arrow2::array::PrimitiveArray<T>
where
    T: NumericNative,
{
    fn sum_unguarded(&self) -> T {
        self.values_iter().sum()
    }
    fn sum_guarded(&self) -> Option<T> {
        self.iter().fold(None, |acc, v| match v {
            Some(v) => match acc {
                Some(acc) => Some(acc + *v),
                None => Some(*v),
            },
            None => acc,
        })
    }
}
*/
