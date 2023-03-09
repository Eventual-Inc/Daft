use std::sync::Arc;

use arrow2;

use crate::{array::DataArray, datatypes::*, error::DaftResult};

use super::DaftNumericAgg;

impl DaftNumericAgg for &Int64Array {
    type Output = DaftResult<Int64Array>;

    fn sum(&self) -> Self::Output {
        let primitive_arr = self.downcast();

        let result = match primitive_arr.validity() {
            None => Some(primitive_arr.values_iter().sum()),
            Some(_) => primitive_arr.iter().fold(None, |acc, v| match v {
                Some(v) => match acc {
                    Some(acc) => Some(acc + *v),
                    None => Some(*v),
                },
                None => acc,
            }),
        };

        let arrow_array = arrow2::array::PrimitiveArray::from([result]);
        DataArray::new(self.field.clone(), Arc::new(arrow_array))
    }
}

impl DaftNumericAgg for &UInt64Array {
    type Output = DaftResult<UInt64Array>;

    fn sum(&self) -> Self::Output {
        let primitive_arr = self.downcast();

        let result = match primitive_arr.validity() {
            None => Some(primitive_arr.values_iter().sum()),
            Some(_) => primitive_arr.iter().fold(None, |acc, v| match v {
                Some(v) => match acc {
                    Some(acc) => Some(acc + *v),
                    None => Some(*v),
                },
                None => acc,
            }),
        };

        let arrow_array = arrow2::array::PrimitiveArray::from([result]);
        DataArray::new(self.field.clone(), Arc::new(arrow_array))
    }
}

impl DaftNumericAgg for &Float32Array {
    type Output = DaftResult<Float32Array>;

    fn sum(&self) -> Self::Output {
        let primitive_arr = self.downcast();

        let result = match primitive_arr.validity() {
            None => Some(primitive_arr.values_iter().sum()),
            Some(_) => primitive_arr.iter().fold(None, |acc, v| match v {
                Some(v) => match acc {
                    Some(acc) => Some(acc + *v),
                    None => Some(*v),
                },
                None => acc,
            }),
        };

        let arrow_array = arrow2::array::PrimitiveArray::from([result]);
        DataArray::new(self.field.clone(), Arc::new(arrow_array))
    }
}

impl DaftNumericAgg for &Float64Array {
    type Output = DaftResult<Float64Array>;

    fn sum(&self) -> Self::Output {
        let primitive_arr = self.downcast();

        let result = match primitive_arr.validity() {
            None => Some(primitive_arr.values_iter().sum()),
            Some(_) => primitive_arr.iter().fold(None, |acc, v| match v {
                Some(v) => match acc {
                    Some(acc) => Some(acc + *v),
                    None => Some(*v),
                },
                None => acc,
            }),
        };

        let arrow_array = arrow2::array::PrimitiveArray::from([result]);
        DataArray::new(self.field.clone(), Arc::new(arrow_array))
    }
}
