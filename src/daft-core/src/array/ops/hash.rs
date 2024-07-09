use crate::{
    array::{DataArray, FixedSizeListArray, ListArray},
    datatypes::{
        logical::{DateArray, Decimal128Array, TimeArray, TimestampArray},
        BinaryArray, BooleanArray, DaftNumericType, FixedSizeBinaryArray, Int16Array, Int32Array,
        Int64Array, Int8Array, NullArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
        Utf8Array,
    },
    kernels,
    utils::arrow::arrow_bitmap_and_helper,
    Series,
};

use arrow2::types::Index;
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

fn hash_list(
    name: &str,
    offsets: &[i64],
    flat_child: &Series,
    validity: Option<&arrow2::bitmap::Bitmap>,
    seed: Option<&UInt64Array>,
) -> DaftResult<UInt64Array> {
    // first we hash the flat child
    // turning [[stuff], [stuff, stuff], ...] into [[hash], [hash, hash], ...]
    // then we hash each sublist as bytes, giving us [hash, hash, ...] as desired
    // if seed is provided, the sublists are hashed with the seed broadcasted

    if let Some(seed_arr) = seed {
        let combined_validity = arrow_bitmap_and_helper(validity, seed.unwrap().validity());
        UInt64Array::from_iter(
            name,
            u64::range(0, offsets.len() - 1).unwrap().map(|i| {
                let start = offsets[i as usize] as usize;
                let end = offsets[i as usize + 1] as usize;
                // apply the current seed across this row
                let cur_seed_opt = seed_arr.get(i as usize);
                let flat_seed = UInt64Array::from_iter(
                    "seed",
                    std::iter::repeat(cur_seed_opt).take(end - start),
                );
                let hashed_child = flat_child
                    .slice(start, end)
                    .ok()?
                    .hash(Some(&flat_seed))
                    .ok()?;
                let child_bytes: Vec<u8> = hashed_child
                    .as_arrow()
                    .values_iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect();
                if let Some(cur_seed) = cur_seed_opt {
                    Some(xxh3_64_with_seed(&child_bytes, cur_seed))
                } else {
                    Some(xxh3_64(&child_bytes))
                }
            }),
        )
        .with_validity(combined_validity)
    } else {
        // since we don't have a seed we can hash entire flat child at once
        let hashed_child = flat_child.hash(None)?;
        // hashing collects the array anyways so this collect doesn't matter
        let child_bytes: Vec<u8> = hashed_child
            .as_arrow()
            .values_iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        const OFFSET: usize = (u64::BITS as usize) / 8; // how many bytes per u64
        let combined_validity = validity.cloned();
        UInt64Array::from_iter(
            name,
            u64::range(0, offsets.len() - 1).unwrap().map(|i| {
                let start = (offsets[i as usize] as usize) * OFFSET;
                let end = (offsets[i as usize + 1] as usize) * OFFSET;
                Some(xxh3_64(&child_bytes[start..end]))
            }),
        )
        .with_validity(combined_validity)
    }
}

impl ListArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        hash_list(
            self.name(),
            self.offsets(),
            &self.flat_child,
            self.validity(),
            seed,
        )
    }
}

impl FixedSizeListArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let size = self.fixed_element_len();
        let len = self.flat_child.len() as i64;
        // see comment on hash_list for why we are collecting
        let offsets: Vec<i64> = (0..=len).step_by(size).collect();
        hash_list(
            self.name(),
            &offsets,
            &self.flat_child,
            self.validity(),
            seed,
        )
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
