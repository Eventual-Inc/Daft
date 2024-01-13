use std::ops::Rem;

use common_error::DaftResult;
use num_traits::ToPrimitive;

use crate::{
    array::DataArray,
    datatypes::{
        logical::Decimal128Array, DaftNumericType, Int16Type, Int32Type, Int64Type, Int8Type,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type, Utf8Array,
    },
};

use super::as_arrow::AsArrow;

macro_rules! impl_int_truncate {
    ($DT:ty) => {
        impl DataArray<$DT> {
            pub fn iceberg_truncate(&self, w: i64) -> DaftResult<DataArray<$DT>> {
                let as_arrowed = self.as_arrow();

                let trun_value = as_arrowed.into_iter().map(|v| {
                    v.map(|v| {
                        let i = v.to_i64().unwrap();
                        let t = (i - (((i.rem(w)) + w).rem(w)));
                        t as <$DT as DaftNumericType>::Native
                    })
                });
                let array = Box::new(arrow2::array::PrimitiveArray::from_iter(trun_value));
                Ok(<DataArray<$DT>>::from((self.name(), array)))
            }
        }
    };
}

impl_int_truncate!(Int8Type);
impl_int_truncate!(Int16Type);
impl_int_truncate!(Int32Type);
impl_int_truncate!(Int64Type);

impl_int_truncate!(UInt8Type);
impl_int_truncate!(UInt16Type);
impl_int_truncate!(UInt32Type);
impl_int_truncate!(UInt64Type);

impl Decimal128Array {
    pub fn iceberg_truncate(&self, w: i64) -> DaftResult<Decimal128Array> {
        let as_arrow = self.as_arrow();
        let trun_value = as_arrow.into_iter().map(|v| {
            v.map(|i| {
                let w = w as i128;
                let remainder = ((i.rem(w)) + w).rem(w);
                i - remainder
            })
        });
        let array = Box::new(arrow2::array::PrimitiveArray::from_iter(trun_value));
        Ok(Decimal128Array::new(
            self.field.clone(),
            DataArray::from((self.name(), array)),
        ))
    }
}

impl Utf8Array {
    pub fn iceberg_truncate(&self, w: i64) -> DaftResult<Utf8Array> {
        let as_arrow = self.as_arrow();
        let substring = arrow2::compute::substring::utf8_substring(as_arrow, 0, &Some(w));
        Ok(Utf8Array::from((self.name(), Box::new(substring))))
    }
}
