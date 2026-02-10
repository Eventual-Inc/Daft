use std::{
    hash::{BuildHasher, Hasher},
    sync::Arc,
};

use arrow::buffer::{Buffer, ScalarBuffer};
use common_error::DaftResult;
use daft_hash::{HashFunctionKind, MurBuildHasher, Sha1Hasher};
use daft_schema::{dtype::DataType, field::Field};
use xxhash_rust::{const_xxh3, xxh3::xxh3_64_with_seed, xxh32::xxh32, xxh64::xxh64};

use super::as_arrow::AsArrow;
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
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
        let has_nulls = self.nulls().map(|v| v.null_count() > 0).unwrap_or(false);
        let as_arrowed = self.as_arrow()?;
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
        let has_nulls = self.nulls().map(|v| v.null_count() > 0).unwrap_or(false);
        let as_arrowed = self.as_arrow()?;
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
        let has_nulls = self.nulls().map(|v| v.null_count() > 0).unwrap_or(false);
        let as_arrowed = self.as_arrow()?;
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
                let has_nulls = self.nulls().map(|v| v.null_count() > 0).unwrap_or(false);
                if has_nulls {
                    let as_arrowed = self.as_arrow()?;
                    murmur3_32_hash_from_iter_with_nulls(
                        self.name(),
                        as_arrowed
                            .iter()
                            .map(|v| v.map(|v| (v as i64).to_le_bytes())),
                    )
                } else {
                    murmur3_32_hash_from_iter_no_nulls(
                        self.name(),
                        self.values().iter().map(|v| (*v as i64).to_le_bytes()),
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
        Ok(Int32Array::new(Field::new(self.name(), DataType::Int32).into(), array).unwrap())
    }
}

impl ListArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        self.hash_with(seed, HashFunctionKind::XxHash3_64)
    }

    pub fn hash_with(
        &self,
        seed: Option<&UInt64Array>,
        hash_function: HashFunctionKind,
    ) -> DaftResult<UInt64Array> {
        let offsets = self.offsets();
        let offsets_slice = offsets.as_slice();
        hash_list(
            self.name(),
            offsets_slice,
            &self.flat_child,
            self.nulls(),
            seed,
            hash_function,
        )
    }
}

impl FixedSizeListArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        self.hash_with(seed, HashFunctionKind::XxHash3_64)
    }

    pub fn hash_with(
        &self,
        seed: Option<&UInt64Array>,
        hash_function: HashFunctionKind,
    ) -> DaftResult<UInt64Array> {
        if let DataType::FixedSizeList(_, size) = self.data_type() {
            let size = *size;
            let len = self.flat_child.len() as i64;
            let offsets: Vec<i64> = (0..=len).step_by(size).collect();
            hash_list(
                self.name(),
                &offsets,
                &self.flat_child,
                self.nulls(),
                seed,
                hash_function,
            )
        } else {
            panic!("FixedSizeListArray data_type should be FixedSizeList");
        }
    }
}

impl StructArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        self.hash_with(seed, HashFunctionKind::XxHash3_64)
    }

    pub fn hash_with(
        &self,
        seed: Option<&UInt64Array>,
        hash_function: HashFunctionKind,
    ) -> DaftResult<UInt64Array> {
        if self.children.is_empty() {
            return Err(common_error::DaftError::ValueError(
                "Cannot hash struct with no children".into(),
            ));
        }
        let mut res = self
            .children
            .first()
            .unwrap()
            .hash_with(seed, hash_function)?;
        for child in self.children.iter().skip(1) {
            res = child.hash_with(Some(&res), hash_function)?;
        }
        res.rename(self.name()).with_nulls(self.nulls().cloned())
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
    Ok(Int32Array::from_iter(
        Field::new(name, DataType::Int32),
        hashes,
    ))
}

fn murmur3_32_hash_from_iter_no_nulls<B: AsRef<[u8]>>(
    name: &str,
    byte_iter: impl Iterator<Item = B>,
) -> DaftResult<Int32Array> {
    let hashes = byte_iter.map(|b| {
        let unsigned = mur3::murmurhash3_x86_32(b.as_ref(), 0);
        i32::from_ne_bytes(unsigned.to_ne_bytes())
    });
    Ok(Int32Array::from_values(name, hashes))
}

fn hash_list(
    name: &str,
    offsets: &[i64],
    flat_child: &crate::series::Series,
    nulls: Option<&daft_arrow::buffer::NullBuffer>,
    seed: Option<&UInt64Array>,
    hash_function: HashFunctionKind,
) -> DaftResult<UInt64Array> {
    // First we hash the flat child
    // turning [[stuff], [stuff, stuff], ...] into [[hash], [hash, hash], ...]
    // then we hash each sublist as bytes, giving us [hash, hash, ...] as desired
    // if seed is provided, the sublists are hashed with the seed broadcasted

    if let Some(seed_arr) = seed {
        let combined_validity = daft_arrow::buffer::NullBuffer::union(nulls, seed.unwrap().nulls());
        let mut hashes = Vec::with_capacity(offsets.len());

        for i in 0..(offsets.len() - 1) {
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            let cur_seed_opt = seed_arr.get(i);
            let flat_seed = UInt64Array::from_iter(
                Arc::new(Field::new("seed", DataType::UInt64)),
                std::iter::repeat_n(cur_seed_opt, end - start),
            );
            let hashed_child = flat_child
                .slice(start, end)?
                .hash_with(Some(&flat_seed), hash_function)?;
            let hashed_arrow = hashed_child.as_arrow()?;
            let hashed_values = hashed_arrow.values();
            let child_bytes = hashed_values.inner().as_slice();

            let hash_val = match hash_function {
                HashFunctionKind::XxHash32 => {
                    let seed = cur_seed_opt.unwrap_or(0) as u32;
                    xxh32(child_bytes, seed) as u64
                }
                HashFunctionKind::XxHash64 => {
                    let seed = cur_seed_opt.unwrap_or(0);
                    xxh64(child_bytes, seed)
                }
                HashFunctionKind::XxHash3_64 => {
                    if let Some(cur_seed) = cur_seed_opt {
                        xxh3_64_with_seed(child_bytes, cur_seed)
                    } else {
                        const_xxh3::xxh3_64(child_bytes)
                    }
                }
                HashFunctionKind::MurmurHash3 => {
                    // Use 42 as default seed,
                    // refer to: https://github.com/Eventual-Inc/Daft/blob/7be4b1ff9ed3fdc3a45947beefab7e7291cd3be7/src/daft-hash/src/lib.rs#L18
                    let hasher = MurBuildHasher::new(cur_seed_opt.unwrap_or(42) as u32);
                    let mut hasher = hasher.build_hasher();
                    hasher.write(child_bytes);
                    hasher.finish()
                }
                HashFunctionKind::Sha1 => {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(child_bytes);
                    hasher.finish()
                }
            };
            hashes.push(hash_val);
        }

        let sb = ScalarBuffer::from(Buffer::from(hashes));
        let arrow_arr = arrow::array::UInt64Array::new(sb, None);
        let field = Arc::new(Field::new(name, DataType::UInt64));
        let mut result = UInt64Array::from_arrow(field, Arc::new(arrow_arr))?;
        result = result.with_nulls(combined_validity)?;
        Ok(result)
    } else {
        let hashed_child = flat_child.hash_with(None, hash_function)?;
        let hashed_arrow = hashed_child.as_arrow()?;
        let hashed_values = hashed_arrow.values();
        let child_bytes = hashed_values.inner().as_slice();

        const OFFSET: usize = (u64::BITS as usize) / 8;
        let combined_validity = nulls.cloned();
        let mut hashes = Vec::with_capacity(offsets.len() - 1);

        for i in 0..(offsets.len() - 1) {
            let start = (offsets[i] as usize) * OFFSET;
            let end = (offsets[i + 1] as usize) * OFFSET;

            let hash_val = match hash_function {
                HashFunctionKind::XxHash32 => xxh32(&child_bytes[start..end], 0) as u64,
                HashFunctionKind::XxHash64 => xxh64(&child_bytes[start..end], 0),
                HashFunctionKind::XxHash3_64 => const_xxh3::xxh3_64(&child_bytes[start..end]),
                HashFunctionKind::MurmurHash3 => {
                    // Use 42 as default seed,
                    // refer to: https://github.com/Eventual-Inc/Daft/blob/7be4b1ff9ed3fdc3a45947beefab7e7291cd3be7/src/daft-hash/src/lib.rs#L18
                    let hasher = MurBuildHasher::new(42);
                    let mut hasher = hasher.build_hasher();
                    hasher.write(&child_bytes[start..end]);
                    hasher.finish()
                }
                HashFunctionKind::Sha1 => {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(&child_bytes[start..end]);
                    hasher.finish()
                }
            };
            hashes.push(hash_val);
        }

        let sb = ScalarBuffer::from(Buffer::from(hashes));
        let arrow_arr = arrow::array::UInt64Array::new(sb, None);
        let field = Arc::new(Field::new(name, DataType::UInt64));
        let mut result = UInt64Array::from_arrow(field, Arc::new(arrow_arr))?;
        result = result.with_nulls(combined_validity)?;
        Ok(result)
    }
}
