use std::hash::{BuildHasher, Hasher};

use arrow2::{
    array::{
        Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, NullArray, PrimitiveArray,
        Utf8Array,
    },
    datatypes::{DataType, PhysicalType},
    error::{Error, Result},
    types::{NativeType, Offset},
};
use daft_hash::{HashFunctionKind, MurBuildHasher, Sha1Hasher};
use xxhash_rust::{
    const_xxh3,
    xxh3::{xxh3_64, xxh3_64_with_seed},
};

fn hash_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    match hash_function {
        HashFunctionKind::XxHash => {
            const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.values_iter())
                    .map(|(v, s)| match v {
                        Some(v) => xxh3_64_with_seed(v.to_le_bytes().as_ref(), *s),
                        None => NULL_HASH,
                    })
                    .collect::<Vec<_>>()
            } else {
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => xxh3_64(v.to_le_bytes().as_ref()),
                        None => NULL_HASH,
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::MurmurHash3 => {
            let hasher = MurBuildHasher::new(seed.and_then(|s| s.get(0)).unwrap_or(42) as u32);
            let hashes = array
                .iter()
                .map(|v| {
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(v) => {
                            hasher.write(v.to_le_bytes().as_ref());
                            hasher.finish()
                        }
                        None => {
                            hasher.write(b"");
                            hasher.finish()
                        }
                    }
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::Sha1 => {
            let hashes = array
                .iter()
                .map(|v| {
                    let mut hasher = Sha1Hasher::default();
                    match v {
                        Some(v) => {
                            hasher.write(v.to_le_bytes().as_ref());
                            hasher.finish()
                        }
                        None => {
                            hasher.write(b"");
                            hasher.finish()
                        }
                    }
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
    }
}

fn hash_boolean(
    array: &BooleanArray,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    match hash_function {
        HashFunctionKind::XxHash => {
            const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
            const FALSE_HASH: u64 = const_xxh3::xxh3_64(b"0");
            const TRUE_HASH: u64 = const_xxh3::xxh3_64(b"1");

            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.values_iter())
                    .map(|(v, s)| match v {
                        Some(true) => xxh3_64_with_seed(b"1", *s),
                        Some(false) => xxh3_64_with_seed(b"0", *s),
                        None => NULL_HASH,
                    })
                    .collect::<Vec<_>>()
            } else {
                array
                    .iter()
                    .map(|v| match v {
                        Some(true) => TRUE_HASH,
                        Some(false) => FALSE_HASH,
                        None => NULL_HASH,
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::MurmurHash3 => {
            let hasher = MurBuildHasher::new(seed.and_then(|s| s.get(0)).unwrap_or(42) as u32);
            let hashes = array
                .iter()
                .map(|v| {
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(true) => {
                            hasher.write(b"1");
                            hasher.finish()
                        }
                        Some(false) => {
                            hasher.write(b"0");
                            hasher.finish()
                        }
                        None => {
                            hasher.write(b"");
                            hasher.finish()
                        }
                    }
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::Sha1 => {
            let hashes = array
                .iter()
                .map(|v| {
                    let mut hasher = Sha1Hasher::default();
                    match v {
                        Some(true) => {
                            hasher.write(b"1");
                            hasher.finish()
                        }
                        Some(false) => {
                            hasher.write(b"0");
                            hasher.finish()
                        }
                        None => {
                            hasher.write(b"");
                            hasher.finish()
                        }
                    }
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
    }
}

fn hash_null(
    array: &NullArray,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    match hash_function {
        HashFunctionKind::XxHash => {
            const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
            let hashes = if let Some(seed) = seed {
                seed.values_iter()
                    .map(|s| xxh3_64_with_seed(b"", *s))
                    .collect::<Vec<_>>()
            } else {
                (0..array.len()).map(|_| NULL_HASH).collect::<Vec<_>>()
            };
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::MurmurHash3 => {
            let hasher = MurBuildHasher::new(seed.and_then(|s| s.get(0)).unwrap_or(42) as u32);
            let hashes = (0..array.len())
                .map(|_| {
                    let mut hasher = hasher.build_hasher();
                    hasher.write(b"");
                    hasher.finish()
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::Sha1 => {
            let hashes = (0..array.len())
                .map(|_| {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(b"");
                    hasher.finish()
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
    }
}

fn hash_binary<O: Offset>(
    array: &BinaryArray<O>,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    match hash_function {
        HashFunctionKind::XxHash => {
            let hashes = if let Some(seed) = seed {
                array
                    .values_iter()
                    .zip(seed.values_iter())
                    .map(|(v, s)| xxh3_64_with_seed(v, *s))
                    .collect::<Vec<_>>()
            } else {
                array.values_iter().map(xxh3_64).collect::<Vec<_>>()
            };
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::MurmurHash3 => {
            let hasher = MurBuildHasher::new(seed.and_then(|s| s.get(0)).unwrap_or(42) as u32);
            let hashes = array
                .values_iter()
                .map(|v| {
                    let mut hasher = hasher.build_hasher();
                    hasher.write(v);
                    hasher.finish()
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::Sha1 => {
            let hashes = array
                .values_iter()
                .map(|v| {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(v);
                    hasher.finish()
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
    }
}

fn hash_fixed_size_binary(
    array: &FixedSizeBinaryArray,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    match hash_function {
        HashFunctionKind::XxHash => {
            let hashes = if let Some(seed) = seed {
                array
                    .values_iter()
                    .zip(seed.values_iter())
                    .map(|(v, s)| xxh3_64_with_seed(v, *s))
                    .collect::<Vec<_>>()
            } else {
                array.values_iter().map(xxh3_64).collect::<Vec<_>>()
            };
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::MurmurHash3 => {
            let hasher = MurBuildHasher::new(seed.and_then(|s| s.get(0)).unwrap_or(42) as u32);
            let hashes = array
                .values_iter()
                .map(|v| {
                    let mut hasher = hasher.build_hasher();
                    hasher.write(v);
                    hasher.finish()
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::Sha1 => {
            let hashes = array
                .values_iter()
                .map(|v| {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(v);
                    hasher.finish()
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
    }
}

fn hash_utf8<O: Offset>(
    array: &Utf8Array<O>,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    match hash_function {
        HashFunctionKind::XxHash => {
            let hashes = if let Some(seed) = seed {
                array
                    .values_iter()
                    .zip(seed.values_iter())
                    .map(|(v, s)| xxh3_64_with_seed(v.as_bytes(), *s))
                    .collect::<Vec<_>>()
            } else {
                array
                    .values_iter()
                    .map(|v| xxh3_64(v.as_bytes()))
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::MurmurHash3 => {
            let hasher = MurBuildHasher::new(seed.and_then(|s| s.get(0)).unwrap_or(42) as u32);
            let hashes = array
                .values_iter()
                .map(|v| {
                    let mut hasher = hasher.build_hasher();
                    hasher.write(v.as_bytes());
                    hasher.finish()
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
        HashFunctionKind::Sha1 => {
            let hashes = array
                .values_iter()
                .map(|v| {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(v.as_bytes());
                    hasher.finish()
                })
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
    }
}

macro_rules! with_match_hashing_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use arrow2::datatypes::PrimitiveType::*;
    use arrow2::types::{days_ms, months_days_ns};
    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        Int128 => __with_ty__! { i128 },
        DaysMs => __with_ty__! { days_ms },
        MonthDayNano => __with_ty__! { months_days_ns },
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
        Float32 => __with_ty__! { f32 },
        Float64 => __with_ty__! { f64 },
        _ => return Err(Error::NotYetImplemented(format!(
            "Hash not implemented for type {:?}",
            $key_type
        )))
    }
})}

pub fn hash(
    array: &dyn Array,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> Result<PrimitiveArray<u64>> {
    if let Some(s) = seed {
        if s.len() != array.len() {
            return Err(Error::InvalidArgumentError(format!(
                "seed length does not match array length: {} vs {}",
                s.len(),
                array.len()
            )));
        }

        if *s.data_type() != DataType::UInt64 {
            return Err(Error::InvalidArgumentError(format!(
                "seed data type expected to be uint64, got {:?}",
                *s.data_type()
            )));
        }
    }

    Ok(match array.data_type().to_physical_type() {
        PhysicalType::Null => {
            hash_null(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        PhysicalType::Boolean => {
            hash_boolean(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        PhysicalType::Primitive(primitive) => with_match_hashing_primitive_type!(primitive, |$T| {
            hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }),
        PhysicalType::Binary => {
            hash_binary::<i32>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        PhysicalType::LargeBinary => {
            hash_binary::<i64>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        PhysicalType::FixedSizeBinary => {
            hash_fixed_size_binary(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        PhysicalType::Utf8 => {
            hash_utf8::<i32>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        PhysicalType::LargeUtf8 => {
            hash_utf8::<i64>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        t => {
            return Err(Error::NotYetImplemented(format!(
                "Hash not implemented for type {t:?}"
            )))
        }
    })
}
