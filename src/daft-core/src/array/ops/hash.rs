use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftNumericType, Int128Array, Int16Array, Int32Array,
        Int64Array, Int8Array, NullArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
        Utf8Array,
    },
    kernels,
    utils::arrow,
};

use arrow2::types::NativeType;
use common_error::DaftResult;

use super::as_arrow::AsArrow;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow();

        let seed = seed.map(|v| v.as_arrow());

        let result = kernels::hashing::hash(as_arrowed, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

impl Utf8Array {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow();

        let seed = seed.map(|v| v.as_arrow());

        let result = kernels::hashing::hash(as_arrowed, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

impl BinaryArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow();

        let seed = seed.map(|v| v.as_arrow());

        let result = kernels::hashing::hash(as_arrowed, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

impl BooleanArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow();

        let seed = seed.map(|v| v.as_arrow());

        let result = kernels::hashing::hash(as_arrowed, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

impl NullArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let as_arrowed = self.data();

        let seed = seed.map(|v| v.as_arrow());

        let result = kernels::hashing::hash(as_arrowed, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

// impl Int64Array {
//     pub fn murmur3_32(&self) -> DaftResult<UInt32Array> {
//         let as_arrowed = self.as_arrow();
//         let has_nulls = as_arrowed.validity().map(|v| v.unset_bits() > 0).unwrap_or(false);
//         if has_nulls {
//             murmur3_32_hash_from_iter_with_nulls(self.name(), as_arrowed.into_iter().map(|v| v.map(|v| v.to_le_bytes())))
//         } else {
//             murmur3_32_hash_from_iter_no_nulls(self.name(), as_arrowed.values_iter().map(|v| v.to_le_bytes()))
//         }
//     }
// }

macro_rules! impl_int_murmur3_32 {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn murmur3_32(&self) -> DaftResult<UInt32Array> {
                let as_arrowed = self.as_arrow();
                let has_nulls = as_arrowed
                    .validity()
                    .map(|v| v.unset_bits() > 0)
                    .unwrap_or(false);
                if has_nulls {
                    murmur3_32_hash_from_iter_with_nulls(
                        self.name(),
                        as_arrowed
                            .into_iter()
                            .map(|v| v.map(|v| (*v as i64).to_le_bytes())),
                    )
                } else {
                    murmur3_32_hash_from_iter_no_nulls(
                        self.name(),
                        as_arrowed.values_iter().map(|v| (*v as i64).to_le_bytes()),
                    )
                }
            }
        }
    };
}

impl_int_murmur3_32!(Int8Array);
impl_int_murmur3_32!(Int16Array);
impl_int_murmur3_32!(Int32Array);
impl_int_murmur3_32!(Int64Array);

impl_int_murmur3_32!(UInt8Array);
impl_int_murmur3_32!(UInt16Array);
impl_int_murmur3_32!(UInt32Array);
impl_int_murmur3_32!(UInt64Array);

fn murmur3_32_hash_from_iter_with_nulls<B: AsRef<[u8]>>(
    name: &str,
    byte_iter: impl Iterator<Item = Option<B>>,
) -> DaftResult<UInt32Array> {
    let hashes = byte_iter.map(|b| b.map(|v| mur3::murmurhash3_x86_32(v.as_ref(), 0)));
    let array = Box::new(arrow2::array::UInt32Array::from_iter(hashes));
    Ok(UInt32Array::from((name, array)))
}

fn murmur3_32_hash_from_iter_no_nulls<B: AsRef<[u8]>>(
    name: &str,
    byte_iter: impl Iterator<Item = B>,
) -> DaftResult<UInt32Array> {
    let hashes = byte_iter
        .map(|b| mur3::murmurhash3_x86_32(b.as_ref(), 0))
        .collect::<Vec<_>>();
    Ok(UInt32Array::from((name, hashes)))
}
