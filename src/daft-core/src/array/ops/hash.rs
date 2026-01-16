use std::sync::Arc;

use arrow::array::Array;
use common_error::DaftResult;
use daft_hash::HashFunctionKind;
use daft_schema::{dtype::DataType, field::Field};

use super::as_arrow::AsArrow;
use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftPrimitiveType, Decimal128Array, FixedSizeBinaryArray,
        Int8Array, Int16Array, Int32Array, Int64Array, NullArray, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array, Utf8Array,
        logical::{DateArray, TimeArray, TimestampArray},
    },
    kernels,
};

impl<T> DataArray<T>
where
    T: DaftPrimitiveType,
{
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        self.hash_with(seed, HashFunctionKind::XxHash3_64)
    }

    pub fn hash_with(
        &self,
        seed: Option<&UInt64Array>,
        hash_function: HashFunctionKind,
    ) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow()?;
        let seed_arr = seed.map(|v| v.as_arrow()).transpose()?;
        let result = kernels::hashing::hash(&as_arrowed, seed_arr.as_ref(), hash_function)?;
        let field = Arc::new(Field::new(self.name(), DataType::UInt64));
        UInt64Array::from_arrow(field, Arc::new(result))
    }
}

impl Utf8Array {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        self.hash_with(seed, HashFunctionKind::XxHash3_64)
    }

    pub fn hash_with(
        &self,
        seed: Option<&UInt64Array>,
        hash_function: HashFunctionKind,
    ) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow()?;
        let seed_arr = seed.map(|v| v.as_arrow()).transpose()?;
        let result = kernels::hashing::hash(&as_arrowed, seed_arr.as_ref(), hash_function)?;
        let field = Arc::new(Field::new(self.name(), DataType::UInt64));
        UInt64Array::from_arrow(field, Arc::new(result))
    }

    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let as_arrowed = self.as_arrow()?;
        let has_nulls = as_arrowed
            .nulls()
            .map(|v| v.null_count() > 0)
            .unwrap_or(false);
        if has_nulls {
            murmur3_32_hash_from_iter_with_nulls(
                self.name(),
                as_arrowed.iter().map(|v| v.map(|v| v.as_bytes())),
            )
        } else {
            murmur3_32_hash_from_iter_no_nulls(
                self.name(),
                as_arrowed.iter().map(|v| v.unwrap().as_bytes()),
            )
        }
    }
}

impl BinaryArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        self.hash_with(seed, HashFunctionKind::XxHash3_64)
    }

    pub fn hash_with(
        &self,
        seed: Option<&UInt64Array>,
        hash_function: HashFunctionKind,
    ) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow()?;
        let seed_arr = seed.map(|v| v.as_arrow()).transpose()?;
        let result = kernels::hashing::hash(&as_arrowed, seed_arr.as_ref(), hash_function)?;
        let field = Arc::new(Field::new(self.name(), DataType::UInt64));
        UInt64Array::from_arrow(field, Arc::new(result))
    }

    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let as_arrowed = self.as_arrow()?;
        let has_nulls = as_arrowed
            .nulls()
            .map(|v| v.null_count() > 0)
            .unwrap_or(false);
        if has_nulls {
            murmur3_32_hash_from_iter_with_nulls(self.name(), as_arrowed.iter())
        } else {
            murmur3_32_hash_from_iter_no_nulls(self.name(), as_arrowed.iter().map(|v| v.unwrap()))
        }
    }
}

impl FixedSizeBinaryArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        self.hash_with(seed, HashFunctionKind::XxHash3_64)
    }

    pub fn hash_with(
        &self,
        seed: Option<&UInt64Array>,
        hash_function: HashFunctionKind,
    ) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow()?;
        let seed_arr = seed.map(|v| v.as_arrow()).transpose()?;
        let result = kernels::hashing::hash(&as_arrowed, seed_arr.as_ref(), hash_function)?;
        let field = Arc::new(Field::new(self.name(), DataType::UInt64));
        UInt64Array::from_arrow(field, Arc::new(result))
    }

    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let as_arrowed = self.as_arrow()?;
        let has_nulls = as_arrowed
            .nulls()
            .map(|v| v.null_count() > 0)
            .unwrap_or(false);
        if has_nulls {
            murmur3_32_hash_from_iter_with_nulls(self.name(), as_arrowed.iter())
        } else {
            murmur3_32_hash_from_iter_no_nulls(self.name(), as_arrowed.iter().map(|v| v.unwrap()))
        }
    }
}

impl BooleanArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        self.hash_with(seed, HashFunctionKind::XxHash3_64)
    }

    pub fn hash_with(
        &self,
        seed: Option<&UInt64Array>,
        hash_function: HashFunctionKind,
    ) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow()?;
        let seed_arr = seed.map(|v| v.as_arrow()).transpose()?;
        let result = kernels::hashing::hash(&as_arrowed, seed_arr.as_ref(), hash_function)?;
        let field = Arc::new(Field::new(self.name(), DataType::UInt64));
        UInt64Array::from_arrow(field, Arc::new(result))
    }
}

impl NullArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        self.hash_with(seed, HashFunctionKind::XxHash3_64)
    }
    pub fn hash_with(
        &self,
        seed: Option<&UInt64Array>,
        hash_function: HashFunctionKind,
    ) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow()?;
        let seed_arr = seed.map(|v| v.as_arrow()).transpose()?;
        let result = kernels::hashing::hash(&as_arrowed, seed_arr.as_ref(), hash_function)?;
        let field = Arc::new(Field::new(self.name(), DataType::UInt64));
        UInt64Array::from_arrow(field, Arc::new(result))
    }
}

macro_rules! impl_primitive_murmur3_32 {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
                let as_arrowed = self.as_arrow()?;
                let has_nulls = as_arrowed
                    .nulls()
                    .map(|v| v.null_count() > 0)
                    .unwrap_or(false);
                if has_nulls {
                    murmur3_32_hash_from_iter_with_nulls(
                        self.name(),
                        as_arrowed
                            .iter()
                            .map(|v| v.map(|v| (v as i64).to_le_bytes())),
                    )
                } else {
                    murmur3_32_hash_from_iter_no_nulls(
                        self.name(),
                        as_arrowed
                            .values()
                            .iter()
                            .map(|v| (*v as i64).to_le_bytes()),
                    )
                }
            }
        }
    };
}

impl_primitive_murmur3_32!(Int8Array);
impl_primitive_murmur3_32!(Int16Array);
impl_primitive_murmur3_32!(Int32Array);
impl_primitive_murmur3_32!(Int64Array);
impl_primitive_murmur3_32!(UInt8Array);
impl_primitive_murmur3_32!(UInt16Array);
impl_primitive_murmur3_32!(UInt32Array);
impl_primitive_murmur3_32!(UInt64Array);

impl DateArray {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        self.physical.murmur3_32()
    }
}

impl TimeArray {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let us = self.cast(&crate::datatypes::DataType::Time(
            crate::datatypes::TimeUnit::Microseconds,
        ))?;
        us.time()?.physical.murmur3_32()
    }
}

impl TimestampArray {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let us = self.cast(&crate::datatypes::DataType::Timestamp(
            crate::datatypes::TimeUnit::Microseconds,
            None,
        ))?;
        us.timestamp()?.physical.murmur3_32()
    }
}

impl Decimal128Array {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let arr = self
            .data()
            .as_any()
            .downcast_ref::<daft_arrow::array::PrimitiveArray<i128>>()
            .expect("this should be a decimal array");
        let hashes = arr.into_iter().map(|d| {
            d.map(|d| {
                let twos_compliment = u128::from_ne_bytes(d.to_ne_bytes());
                let bits_needed = u128::BITS - twos_compliment.leading_zeros();
                let bytes_needed = bits_needed.div_ceil(8) as usize;
                let be_bytes = twos_compliment.to_be_bytes();
                let unsigned =
                    mur3::murmurhash3_x86_32(&be_bytes[(be_bytes.len() - bytes_needed)..], 0);
                i32::from_ne_bytes(unsigned.to_ne_bytes())
            })
        });
        let array = Box::new(daft_arrow::array::Int32Array::from_iter(hashes));
        Ok(Int32Array::from((self.name(), array)))
    }
}

fn murmur3_32_hash_from_iter_with_nulls<B: AsRef<[u8]>>(
    name: &str,
    byte_iter: impl Iterator<Item = Option<B>>,
) -> DaftResult<Int32Array> {
    let hashes = byte_iter.map(|b| {
        b.map(|v| {
            let unsigned = mur3::murmurhash3_x86_32(v.as_ref(), 0);
            i32::from_ne_bytes(unsigned.to_ne_bytes())
        })
    });
    let array = Box::new(daft_arrow::array::Int32Array::from_iter(hashes));
    Ok(Int32Array::from((name, array)))
}

fn murmur3_32_hash_from_iter_no_nulls<B: AsRef<[u8]>>(
    name: &str,
    byte_iter: impl Iterator<Item = B>,
) -> DaftResult<Int32Array> {
    let hashes = byte_iter
        .map(|b| {
            let unsigned = mur3::murmurhash3_x86_32(b.as_ref(), 0);
            i32::from_ne_bytes(unsigned.to_ne_bytes())
        })
        .collect::<Vec<_>>();
    Ok(Int32Array::from((name, hashes)))
}
