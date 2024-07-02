use crate::{
    array::{DataArray, ListArray},
    datatypes::{
        logical::{DateArray, Decimal128Array, TimeArray, TimestampArray},
        BinaryArray, BooleanArray, DaftNumericType, FixedSizeBinaryArray, Int16Array, Int32Array,
        Int64Array, Int8Array, NullArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
        Utf8Array,
    },
    kernels,
    utils::arrow::arrow_bitmap_and_helper,
    with_match_hashable_daft_types,
};

use common_error::DaftResult;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

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

impl FixedSizeBinaryArray {
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

impl ListArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        // first we recursively call hashing on the sublists
        // turning [[stuff], [stuff, stuff], ...] into [[hash], [hash, hash], ...]
        // then we hash each sublist as bytes, giving us [hash, hash, ...] as desired
        // seed only applies to row as a whole, hashes within a row are unseeded
        // this code is kind of scuffed because we need to account for every combination of seed existence + self validity
        if self.null_count() > 0 || seed.is_some_and(|arr| arr.null_count() > 0) {
            if let Some(seed_arr) = seed {
                let combined_validity =
                    arrow_bitmap_and_helper(self.validity(), seed_arr.validity())
                        .expect("there should be some invalid element");
                // need to extract the sublists from the flattened child
                let windows_iter = self
                    .offsets()
                    .windows(2)
                    .zip(seed_arr.as_arrow().values_iter())
                    .zip(combined_validity);
                with_match_hashable_daft_types!(self.flat_child.data_type(), |$T| {
                    let downcasted = self.flat_child.downcast::<<$T as DaftDataType>::ArrayType>()?;
                    Ok(UInt64Array::from_iter(self.name(), windows_iter.map(|((w, s), valid)| {
                        if !valid {
                            return None;
                        }
                        let hashes = downcasted.slice(w[0] as usize, w[1] as usize).ok()?.hash(None).ok()?;
                        let bytes: Vec<u8> = hashes.as_arrow().values_iter().flat_map(|v| v.to_le_bytes()).collect();
                        Some(xxh3_64_with_seed(&bytes, *s))
                    })))
                })
            } else {
                let windows_iter = self.offsets().windows(2).zip(self.validity().unwrap());
                with_match_hashable_daft_types!(self.flat_child.data_type(), |$T| {
                    let downcasted = self.flat_child.downcast::<<$T as DaftDataType>::ArrayType>()?;
                    Ok(UInt64Array::from_iter(self.name(), windows_iter.map(|(w, valid)| {
                        if !valid {
                            return None;
                        }
                        let hashes = downcasted.slice(w[0] as usize, w[1] as usize).ok()?.hash(None).ok()?;
                        let bytes: Vec<u8> = hashes.as_arrow().values_iter().flat_map(|v| v.to_le_bytes()).collect();
                        Some(xxh3_64(&bytes))
                    })))
                })
            }
        } else {
            // I think it's more readable this way
            #[allow(clippy::collapsible_else_if)]
            if let Some(seed_arr) = seed {
                let windows_iter = self
                    .offsets()
                    .windows(2)
                    .zip(seed_arr.as_arrow().values_iter());
                with_match_hashable_daft_types!(self.flat_child.data_type(), |$T| {
                    let downcasted = self.flat_child.downcast::<<$T as DaftDataType>::ArrayType>()?;
                    Ok(UInt64Array::from_iter(self.name(), windows_iter.map(|(w, s)| {
                        let hashes = downcasted.slice(w[0] as usize, w[1] as usize).ok()?.hash(None).ok()?;
                        let bytes: Vec<u8> = hashes.as_arrow().values_iter().flat_map(|v| v.to_le_bytes()).collect();
                        Some(xxh3_64_with_seed(&bytes, *s))
                    })))
                })
            } else {
                let windows_iter = self.offsets().windows(2);
                with_match_hashable_daft_types!(self.flat_child.data_type(), |$T| {
                    let downcasted = self.flat_child.downcast::<<$T as DaftDataType>::ArrayType>()?;
                    Ok(UInt64Array::from_iter(self.name(), windows_iter.map(|w| {
                        let hashes = downcasted.slice(w[0] as usize, w[1] as usize).ok()?.hash(None).ok()?;
                        let bytes: Vec<u8> = hashes.as_arrow().values_iter().flat_map(|v| v.to_le_bytes()).collect();
                        Some(xxh3_64(&bytes))
                    })))
                })
            }
        }
    }
}

macro_rules! impl_int_murmur3_32 {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
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

impl Utf8Array {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let as_arrowed = self.as_arrow();
        let has_nulls = as_arrowed
            .validity()
            .map(|v| v.unset_bits() > 0)
            .unwrap_or(false);
        if has_nulls {
            murmur3_32_hash_from_iter_with_nulls(
                self.name(),
                as_arrowed.into_iter().map(|v| v.map(|v| v.as_bytes())),
            )
        } else {
            murmur3_32_hash_from_iter_no_nulls(
                self.name(),
                as_arrowed.values_iter().map(|v| v.as_bytes()),
            )
        }
    }
}

impl BinaryArray {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let as_arrowed = self.as_arrow();
        let has_nulls = as_arrowed
            .validity()
            .map(|v| v.unset_bits() > 0)
            .unwrap_or(false);
        if has_nulls {
            murmur3_32_hash_from_iter_with_nulls(self.name(), as_arrowed.into_iter())
        } else {
            murmur3_32_hash_from_iter_no_nulls(self.name(), as_arrowed.values_iter())
        }
    }
}

impl FixedSizeBinaryArray {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let as_arrowed = self.as_arrow();
        let has_nulls = as_arrowed
            .validity()
            .map(|v| v.unset_bits() > 0)
            .unwrap_or(false);
        if has_nulls {
            murmur3_32_hash_from_iter_with_nulls(self.name(), as_arrowed.into_iter())
        } else {
            murmur3_32_hash_from_iter_no_nulls(self.name(), as_arrowed.values_iter())
        }
    }
}

impl DateArray {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        self.physical.murmur3_32()
    }
}

impl TimeArray {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let us = self.cast(&crate::DataType::Time(
            crate::datatypes::TimeUnit::Microseconds,
        ))?;
        us.time()?.physical.murmur3_32()
    }
}

impl TimestampArray {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let us = self.cast(&crate::DataType::Timestamp(
            crate::datatypes::TimeUnit::Microseconds,
            None,
        ))?;
        us.timestamp()?.physical.murmur3_32()
    }
}

impl Decimal128Array {
    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        let arr = self.physical.as_arrow();
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
        let array = Box::new(arrow2::array::Int32Array::from_iter(hashes));
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
    let array = Box::new(arrow2::array::Int32Array::from_iter(hashes));
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
