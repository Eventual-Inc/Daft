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

#[cfg(test)]
mod tests {
    use std::hash::{BuildHasher, Hasher};

    use daft_hash::{HashFunctionKind, MurBuildHasher, Sha1Hasher};
    use xxhash_rust::{const_xxh3, xxh3::xxh3_64_with_seed};

    use crate::{
        array::{FixedSizeListArray, ListArray, StructArray, ops::full::FullNull},
        datatypes::{
            BinaryArray, BooleanArray, DataType, Decimal128Array, Field, FixedSizeBinaryArray,
            Int32Array, Int64Array, NullArray, TimeUnit, UInt64Array, Utf8Array,
            logical::{DateArray, TimestampArray},
        },
        series::IntoSeries,
    };

    // ─── helpers to compute expected hashes directly ───

    fn xxh3_64(bytes: &[u8]) -> u64 {
        const_xxh3::xxh3_64(bytes)
    }

    fn xxh3_64_seeded(bytes: &[u8], seed: u64) -> u64 {
        xxh3_64_with_seed(bytes, seed)
    }

    fn murmur3_64(bytes: &[u8]) -> u64 {
        let hasher = MurBuildHasher::new(42);
        let mut h = hasher.build_hasher();
        h.write(bytes);
        h.finish()
    }

    fn sha1(bytes: &[u8]) -> u64 {
        let mut h = Sha1Hasher::default();
        h.write(bytes);
        h.finish()
    }

    fn murmur3_32(bytes: &[u8]) -> i32 {
        let unsigned = mur3::murmurhash3_x86_32(bytes, 0);
        i32::from_ne_bytes(unsigned.to_ne_bytes())
    }

    // ─── Primitive hash_with: verify against raw xxh3 ───

    #[test]
    fn test_primitive_hash_xxh3_values() {
        let values: Vec<i64> = vec![1, 2, 3];
        let arr = Int64Array::from_slice("x", &values);
        let result = arr.hash(None).unwrap();

        for (i, v) in values.iter().enumerate() {
            let expected = xxh3_64(&v.to_le_bytes());
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    #[test]
    fn test_primitive_hash_xxh3_seeded() {
        let values: Vec<i64> = vec![10, 20];
        let seeds: Vec<u64> = vec![42, 99];
        let arr = Int64Array::from_slice("x", &values);
        let seed_arr = UInt64Array::from_slice("seed", &seeds);
        let result = arr.hash(Some(&seed_arr)).unwrap();

        for i in 0..values.len() {
            let expected = xxh3_64_seeded(&values[i].to_le_bytes(), seeds[i]);
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    #[test]
    fn test_primitive_hash_murmur3_values() {
        let values: Vec<i64> = vec![100, 200];
        let arr = Int64Array::from_slice("x", &values);
        let result = arr.hash_with(None, HashFunctionKind::MurmurHash3).unwrap();

        for (i, v) in values.iter().enumerate() {
            let expected = murmur3_64(&v.to_le_bytes());
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    #[test]
    fn test_primitive_hash_sha1_values() {
        let values: Vec<i64> = vec![7, 42];
        let arr = Int64Array::from_slice("x", &values);
        let result = arr.hash_with(None, HashFunctionKind::Sha1).unwrap();

        for (i, v) in values.iter().enumerate() {
            let expected = sha1(&v.to_le_bytes());
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    #[test]
    fn test_primitive_hash_with_nulls() {
        let arr = Int64Array::from_iter(
            Field::new("x", DataType::Int64),
            vec![Some(1), None, Some(3)],
        );
        let result = arr.hash(None).unwrap();
        assert_eq!(result.get(0).unwrap(), xxh3_64(&1i64.to_le_bytes()));
        assert_eq!(result.get(2).unwrap(), xxh3_64(&3i64.to_le_bytes()));
    }

    // ─── Utf8 hash_with ───

    #[test]
    fn test_utf8_hash_xxh3_values() {
        let strs = vec!["hello", "world"];
        let arr = Utf8Array::from_slice("s", &strs);
        let result = arr.hash(None).unwrap();

        for (i, s) in strs.iter().enumerate() {
            let expected = xxh3_64(s.as_bytes());
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    #[test]
    fn test_utf8_hash_seeded() {
        let strs = vec!["a", "b"];
        let seeds: Vec<u64> = vec![100, 200];
        let arr = Utf8Array::from_slice("s", &strs);
        let seed_arr = UInt64Array::from_slice("seed", &seeds);
        let result = arr.hash(Some(&seed_arr)).unwrap();

        for i in 0..strs.len() {
            let expected = xxh3_64_seeded(strs[i].as_bytes(), seeds[i]);
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    #[test]
    fn test_utf8_hash_with_nulls() {
        let arr = Utf8Array::from_iter("s", vec![Some("a"), None, Some("c")]);
        let result = arr.hash(None).unwrap();
        assert_eq!(result.get(0).unwrap(), xxh3_64(b"a"));
        assert_eq!(result.get(2).unwrap(), xxh3_64(b"c"));
    }

    // ─── Utf8 murmur3_32 ───

    #[test]
    fn test_utf8_murmur3_values() {
        let strs = vec!["hello", "world"];
        let arr = Utf8Array::from_slice("s", &strs);
        let result = arr.murmur3_32().unwrap();

        for (i, s) in strs.iter().enumerate() {
            assert_eq!(result.get(i).unwrap(), murmur3_32(s.as_bytes()));
        }
    }

    #[test]
    fn test_utf8_murmur3_with_nulls() {
        let arr = Utf8Array::from_iter("s", vec![Some("hello"), None, Some("world")]);
        let result = arr.murmur3_32().unwrap();
        assert_eq!(result.get(0).unwrap(), murmur3_32(b"hello"));
        assert!(result.get(1).is_none());
        assert_eq!(result.get(2).unwrap(), murmur3_32(b"world"));
    }

    // ─── Binary hash_with ───

    #[test]
    fn test_binary_hash_values() {
        let data: Vec<&[u8]> = vec![b"abc", b"def"];
        let arr = BinaryArray::from_values("b", data.clone());
        let result = arr.hash(None).unwrap();

        for (i, d) in data.iter().enumerate() {
            assert_eq!(result.get(i).unwrap(), xxh3_64(d));
        }
    }

    #[test]
    fn test_binary_hash_with_nulls() {
        let arr = BinaryArray::from_iter(
            "b",
            vec![Some(b"abc".as_ref()), None, Some(b"xyz".as_ref())],
        );
        let result = arr.hash(None).unwrap();
        assert_eq!(result.get(0).unwrap(), xxh3_64(b"abc"));
        assert_eq!(result.get(2).unwrap(), xxh3_64(b"xyz"));
    }

    // ─── Binary murmur3_32 ───

    #[test]
    fn test_binary_murmur3_values() {
        let data: Vec<&[u8]> = vec![b"foo", b"bar"];
        let arr = BinaryArray::from_values("b", data.clone());
        let result = arr.murmur3_32().unwrap();

        for (i, d) in data.iter().enumerate() {
            assert_eq!(result.get(i).unwrap(), murmur3_32(d));
        }
    }

    #[test]
    fn test_binary_murmur3_with_nulls() {
        let arr = BinaryArray::from_iter(
            "b",
            vec![Some(b"foo".as_ref()), None, Some(b"bar".as_ref())],
        );
        let result = arr.murmur3_32().unwrap();
        assert_eq!(result.get(0).unwrap(), murmur3_32(b"foo"));
        assert!(result.get(1).is_none());
        assert_eq!(result.get(2).unwrap(), murmur3_32(b"bar"));
    }

    // ─── FixedSizeBinary ───

    #[test]
    fn test_fixed_size_binary_hash_values() {
        let arr = FixedSizeBinaryArray::from_iter(
            "fb",
            vec![Some([1u8, 2, 3, 4]), Some([5, 6, 7, 8])].into_iter(),
            4,
        );
        let result = arr.hash(None).unwrap();
        assert_eq!(result.get(0).unwrap(), xxh3_64(&[1, 2, 3, 4]));
        assert_eq!(result.get(1).unwrap(), xxh3_64(&[5, 6, 7, 8]));
    }

    #[test]
    fn test_fixed_size_binary_murmur3_values() {
        let arr = FixedSizeBinaryArray::from_iter(
            "fb",
            vec![Some([10u8, 20]), Some([30, 40])].into_iter(),
            2,
        );
        let result = arr.murmur3_32().unwrap();
        assert_eq!(result.get(0).unwrap(), murmur3_32(&[10, 20]));
        assert_eq!(result.get(1).unwrap(), murmur3_32(&[30, 40]));
    }

    #[test]
    fn test_fixed_size_binary_murmur3_with_nulls() {
        let arr = FixedSizeBinaryArray::from_iter(
            "fb",
            vec![Some([10u8, 20]), None, Some([30, 40])].into_iter(),
            2,
        );
        let result = arr.murmur3_32().unwrap();
        assert_eq!(result.get(0).unwrap(), murmur3_32(&[10, 20]));
        assert!(result.get(1).is_none());
        assert_eq!(result.get(2).unwrap(), murmur3_32(&[30, 40]));
    }

    // ─── Boolean hash_with ───

    #[test]
    fn test_boolean_hash_values() {
        // Booleans are hashed as b"1" / b"0" (ASCII bytes)
        let arr = BooleanArray::from_slice("b", &[true, false]);
        let result = arr.hash(None).unwrap();
        assert_eq!(result.get(0).unwrap(), xxh3_64(b"1"));
        assert_eq!(result.get(1).unwrap(), xxh3_64(b"0"));
    }

    #[test]
    fn test_boolean_hash_seeded() {
        let arr = BooleanArray::from_slice("b", &[true]);
        let seed = UInt64Array::from_slice("seed", &[77]);
        let result = arr.hash(Some(&seed)).unwrap();
        assert_eq!(result.get(0).unwrap(), xxh3_64_seeded(b"1", 77));
    }

    #[test]
    fn test_boolean_hash_with_nulls() {
        let arr = BooleanArray::from_iter("b", vec![Some(true), None, Some(false)].into_iter());
        let result = arr.hash(None).unwrap();
        assert_eq!(result.get(0).unwrap(), xxh3_64(b"1"));
        assert_eq!(result.get(2).unwrap(), xxh3_64(b"0"));
    }

    // ─── NullArray hash_with ───

    #[test]
    fn test_null_hash_all_same() {
        let arr = NullArray::full_null("n", &DataType::Null, 3);
        let result = arr.hash(None).unwrap();
        let v0 = result.get(0).unwrap();
        assert_eq!(result.get(1).unwrap(), v0);
        assert_eq!(result.get(2).unwrap(), v0);
    }

    #[test]
    fn test_null_hash_with_seed() {
        let arr = NullArray::full_null("n", &DataType::Null, 2);
        let seed = UInt64Array::from_slice("seed", &[10, 20]);
        let result = arr.hash(Some(&seed)).unwrap();
        // Different seeds should produce different hashes
        assert_ne!(result.get(0).unwrap(), result.get(1).unwrap());
    }

    #[test]
    fn test_null_hash_all_functions() {
        let arr = NullArray::full_null("n", &DataType::Null, 2);
        for hf in [
            HashFunctionKind::XxHash32,
            HashFunctionKind::XxHash64,
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = arr.hash_with(None, hf).unwrap();
            // All nulls in the same function should hash to the same value
            assert_eq!(result.get(0).unwrap(), result.get(1).unwrap());
        }
    }

    // ─── impl_primitive_murmur3_32: verify against mur3 crate ───

    #[test]
    fn test_int32_murmur3_values() {
        let values: Vec<i32> = vec![1, 2, 3];
        let arr = Int32Array::from_slice("x", &values);
        let result = arr.murmur3_32().unwrap();

        for (i, v) in values.iter().enumerate() {
            let expected = murmur3_32(&(*v as i64).to_le_bytes());
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    #[test]
    fn test_int64_murmur3_values() {
        let values: Vec<i64> = vec![100, 200, 300];
        let arr = Int64Array::from_slice("x", &values);
        let result = arr.murmur3_32().unwrap();

        for (i, v) in values.iter().enumerate() {
            let expected = murmur3_32(&v.to_le_bytes());
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    #[test]
    fn test_int64_murmur3_with_nulls() {
        let arr = Int64Array::from_iter(
            Field::new("x", DataType::Int64),
            vec![Some(1), None, Some(3)],
        );
        let result = arr.murmur3_32().unwrap();
        assert_eq!(result.get(0).unwrap(), murmur3_32(&1i64.to_le_bytes()));
        assert!(result.get(1).is_none());
        assert_eq!(result.get(2).unwrap(), murmur3_32(&3i64.to_le_bytes()));
    }

    #[test]
    fn test_uint64_murmur3_values() {
        let values: Vec<u64> = vec![10, 20, 30];
        let arr = UInt64Array::from_slice("x", &values);
        let result = arr.murmur3_32().unwrap();

        for (i, v) in values.iter().enumerate() {
            let expected = murmur3_32(&(*v as i64).to_le_bytes());
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    // ─── DateArray murmur3_32 ───

    #[test]
    fn test_date_murmur3_values() {
        let values: Vec<i32> = vec![18000, 18001];
        let physical = Int32Array::from_slice("date", &values);
        let arr = DateArray::new(Field::new("date", DataType::Date), physical);
        let result = arr.murmur3_32().unwrap();

        for (i, v) in values.iter().enumerate() {
            let expected = murmur3_32(&(*v as i64).to_le_bytes());
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    // ─── TimestampArray murmur3_32 ───

    #[test]
    fn test_timestamp_murmur3_values() {
        let us_values: Vec<i64> = vec![1_000_000, 2_000_000];
        let physical = Int64Array::from_slice("ts", &us_values);
        let arr = TimestampArray::new(
            Field::new("ts", DataType::Timestamp(TimeUnit::Microseconds, None)),
            physical,
        );
        let result = arr.murmur3_32().unwrap();

        for (i, v) in us_values.iter().enumerate() {
            let expected = murmur3_32(&v.to_le_bytes());
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    // ─── Decimal128Array murmur3_32 ───

    #[test]
    fn test_decimal_murmur3_values() {
        let values: Vec<i128> = vec![100, 200];
        let arr = Decimal128Array::from_field_and_values(
            Field::new("d", DataType::Decimal128(10, 2)),
            values.clone(),
        );
        let result = arr.murmur3_32().unwrap();

        for (i, d) in values.iter().enumerate() {
            let twos_compliment = u128::from_ne_bytes(d.to_ne_bytes());
            let bits_needed = u128::BITS - twos_compliment.leading_zeros();
            let bytes_needed = bits_needed.div_ceil(8) as usize;
            let be_bytes = twos_compliment.to_be_bytes();
            let unsigned =
                mur3::murmurhash3_x86_32(&be_bytes[(be_bytes.len() - bytes_needed)..], 0);
            let expected = i32::from_ne_bytes(unsigned.to_ne_bytes());
            assert_eq!(result.get(i).unwrap(), expected);
        }
    }

    // ─── StructArray hash_with ───

    #[test]
    fn test_struct_hash_single_child() {
        // Single child: struct hash == child hash
        let child = Int64Array::from_slice("a", &[1, 2]).into_series();
        let field = Field::new(
            "s",
            DataType::Struct(vec![Field::new("a", DataType::Int64)]),
        );
        let arr = StructArray::new(field, vec![child], None);
        let result = arr.hash(None).unwrap();

        assert_eq!(result.get(0).unwrap(), xxh3_64(&1i64.to_le_bytes()));
        assert_eq!(result.get(1).unwrap(), xxh3_64(&2i64.to_le_bytes()));
    }

    #[test]
    fn test_struct_hash_two_children() {
        // Two children: hash child0, then hash child1 seeded with child0's hashes
        let c0 = Int64Array::from_slice("a", &[1]).into_series();
        let c1 = Int64Array::from_slice("b", &[2]).into_series();
        let field = Field::new(
            "s",
            DataType::Struct(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Int64),
            ]),
        );
        let arr = StructArray::new(field, vec![c0, c1], None);
        let result = arr.hash(None).unwrap();

        let h0 = xxh3_64(&1i64.to_le_bytes());
        let expected = xxh3_64_seeded(&2i64.to_le_bytes(), h0);
        assert_eq!(result.get(0).unwrap(), expected);
    }

    #[test]
    fn test_struct_hash_empty_children_errors() {
        let field = Field::new("s", DataType::Struct(vec![]));
        let arr = StructArray::new(field, vec![], None);
        assert!(arr.hash(None).is_err());
    }

    #[test]
    fn test_struct_hash_all_functions() {
        let child = Int64Array::from_slice("a", &[1, 2]).into_series();
        let field = Field::new(
            "s",
            DataType::Struct(vec![Field::new("a", DataType::Int64)]),
        );
        let arr = StructArray::new(field, vec![child], None);
        for hf in [
            HashFunctionKind::XxHash32,
            HashFunctionKind::XxHash64,
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = arr.hash_with(None, hf).unwrap();
            assert_eq!(result.len(), 2);
        }
    }

    // ─── ListArray hash_with ───

    #[test]
    fn test_list_hash_values() {
        // [[1, 2], [3]]
        // Unseeded: hash flat children, then xxh3 the concatenated child hash bytes per row
        let arr = ListArray::from_vec::<i64>("l", vec![Some(vec![1, 2]), Some(vec![3])]);
        let result = arr.hash(None).unwrap();

        let h1 = xxh3_64(&1i64.to_le_bytes());
        let h2 = xxh3_64(&2i64.to_le_bytes());
        let h3 = xxh3_64(&3i64.to_le_bytes());

        let mut row0_bytes = Vec::new();
        row0_bytes.extend_from_slice(&h1.to_le_bytes());
        row0_bytes.extend_from_slice(&h2.to_le_bytes());
        assert_eq!(result.get(0).unwrap(), xxh3_64(&row0_bytes));
        assert_eq!(result.get(1).unwrap(), xxh3_64(&h3.to_le_bytes()));
    }

    #[test]
    fn test_list_hash_with_seed() {
        let arr = ListArray::from_vec::<i64>("l", vec![Some(vec![1, 2]), Some(vec![3, 4])]);
        let seed = UInt64Array::from_slice("seed", &[42, 43]);
        let result = arr.hash(Some(&seed)).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_list_hash_with_nulls() {
        let arr = ListArray::from_vec::<i64>("l", vec![Some(vec![1, 2]), None, Some(vec![3])]);
        let result = arr.hash(None).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_list_hash_all_functions() {
        let arr = ListArray::from_vec::<i64>("l", vec![Some(vec![10, 20]), Some(vec![30])]);
        for hf in [
            HashFunctionKind::XxHash32,
            HashFunctionKind::XxHash64,
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = arr.hash_with(None, hf).unwrap();
            assert_eq!(result.len(), 2);
        }
    }

    // ─── FixedSizeListArray hash_with ───

    #[test]
    fn test_fixed_size_list_hash_values() {
        // [[1,2], [3,4]]
        let flat = Int64Array::from_slice("item", &[1, 2, 3, 4]).into_series();
        let field = Field::new("fl", DataType::FixedSizeList(Box::new(DataType::Int64), 2));
        let arr = FixedSizeListArray::new(field, flat, None);
        let result = arr.hash(None).unwrap();

        // Same logic as list
        let h1 = xxh3_64(&1i64.to_le_bytes());
        let h2 = xxh3_64(&2i64.to_le_bytes());
        let mut row0_bytes = Vec::new();
        row0_bytes.extend_from_slice(&h1.to_le_bytes());
        row0_bytes.extend_from_slice(&h2.to_le_bytes());
        assert_eq!(result.get(0).unwrap(), xxh3_64(&row0_bytes));
    }

    // ─── Determinism and seed tests ───

    #[test]
    fn test_hash_deterministic() {
        let arr = Int64Array::from_slice("x", &[42, 99, 0]);
        let r1 = arr.hash(None).unwrap();
        let r2 = arr.hash(None).unwrap();
        for i in 0..3 {
            assert_eq!(r1.get(i), r2.get(i));
        }
    }

    #[test]
    fn test_different_values_different_hashes() {
        let arr = Int64Array::from_slice("x", &[1, 2]);
        let result = arr.hash(None).unwrap();
        assert_ne!(result.get(0), result.get(1));
    }

    #[test]
    fn test_seed_changes_hash() {
        let arr = Int64Array::from_slice("x", &[42]);
        let no_seed = arr.hash(None).unwrap();
        let seed = UInt64Array::from_slice("seed", &[999]);
        let with_seed = arr.hash(Some(&seed)).unwrap();
        assert_ne!(no_seed.get(0), with_seed.get(0));
    }
}
