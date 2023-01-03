use std::{any::Any, sync::Arc};

use crate::{
    array::data_array::{BaseArray, DataArray},
    datatypes,
    datatypes::DataType,
    dsl::Operator,
    error::DaftResult,
};

#[derive(Debug, Clone)]
pub struct Series {
    array: Arc<dyn BaseArray>,
}

impl From<Box<dyn arrow2::array::Array>> for Series {
    fn from(item: Box<dyn arrow2::array::Array>) -> Self {
        match item.data_type().into() {
            DataType::Int64 => Series {
                array: Arc::from(DataArray::<datatypes::Int64Type>::from(item)),
            },
            DataType::Utf8 => Series {
                array: Arc::from(DataArray::<datatypes::Utf8Type>::from(item)),
            },
            _ => panic!("help!"),
        }

        // Series {
        //     array: Arc::from(DataArray<T>::from(item)),
        // }
    }
}

impl Series {
    // #[inline]
    // pub fn binary_op(&self, other: &Series, op: Operator) -> DaftResult<Series> {
    //     Ok(Series {
    //         array: Arc::from(self.array.binary_op(other.array.as_ref(), op)?),
    //     })
    // }
    // pub fn add(&self, other: &Series) -> DaftResult<Series> {
    //     self.binary_op(other, Operator::Plus)
    // }
}

#[cfg(test)]
mod tests {

    use arrow2::array::Array;

    use super::*;

    // #[test]
    // fn add_series() -> DaftResult<()> {
    //     let s =
    //         Series::from(arrow2::array::PrimitiveArray::<i64>::from_slice([1, 2, 3, 4]).boxed());

    //     let s2 =
    //         Series::from(arrow2::array::Utf8Array::<i64>::from_slice(["1", "2", "3", "4"]).boxed());

    //     println!("{:?}", s.add(&s2)?);

    //     Ok(())
    // }
}
