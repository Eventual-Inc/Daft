use std::ops::Rem;

use common_error::DaftResult;
use num_traits::ToPrimitive;

use super::as_arrow::AsArrow;
use crate::{
    array::DataArray,
    datatypes::{
        DaftDataType, DaftNumericType, DataType, Decimal128Array, Field, Int8Type, Int16Type,
        Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type, Utf8Array,
    },
    prelude::BinaryArray,
};

macro_rules! impl_int_truncate {
    ($DT:ty) => {
        impl DataArray<$DT> {
            pub fn iceberg_truncate(&self, w: i64) -> DaftResult<DataArray<$DT>> {
                let trun_value = self.into_iter().map(|v| {
                    v.map(|v| {
                        let i = v.to_i64().unwrap();
                        let t = (i - (((i.rem(w)) + w).rem(w)));
                        t as <$DT as DaftNumericType>::Native
                    })
                });
                Ok(Self::from_iter(
                    Field::new(self.name(), <$DT>::get_dtype()),
                    trun_value,
                ))
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
    pub fn iceberg_truncate(&self, w: i64) -> DaftResult<Self> {
        let trun_value = self.into_iter().map(|v| {
            v.map(|i| {
                let w = w as i128;
                let remainder = ((i.rem(w)) + w).rem(w);
                i - remainder
            })
        });
        Ok(Self::from_iter(self.field.clone(), trun_value))
    }
}

impl Utf8Array {
    pub fn iceberg_truncate(&self, w: i64) -> DaftResult<Self> {
        let substring = arrow::compute::kernels::substring::substring(
            self.to_arrow().as_ref(),
            0,
            Some(w as _),
        )?;

        Self::from_arrow(Field::new(self.name(), DataType::Utf8), substring)
    }
}

impl BinaryArray {
    pub fn iceberg_truncate(&self, w: i64) -> DaftResult<Self> {
        let as_arrow = self.as_arrow2();
        let substring = daft_arrow::compute::substring::binary_substring(as_arrow, 0, &Some(w));
        Ok(Self::new(
            Field::new(self.name(), DataType::Binary).into(),
            Box::new(substring),
        )
        .unwrap())
    }
}
