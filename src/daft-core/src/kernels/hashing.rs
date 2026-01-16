use std::hash::{BuildHasher, Hasher};

use arrow::{
    array::{
        Array, BooleanArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray, NullArray,
        PrimitiveArray,
    },
    datatypes::{
        ArrowPrimitiveType, DataType as ArrowDataType, Decimal128Type, Float32Type, Float64Type,
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use common_error::{DaftError, DaftResult};
use daft_hash::{HashFunctionKind, MurBuildHasher, Sha1Hasher};
use xxhash_rust::{
    const_xxh3, const_xxh32, const_xxh64, xxh3::xxh3_64_with_seed, xxh32::xxh32, xxh64::xxh64,
};

/// Helper trait to convert primitive types to bytes for hashing
trait ToLeBytes {
    fn to_le_bytes_vec(&self) -> Vec<u8>;
}

macro_rules! impl_to_le_bytes {
    ($($t:ty),*) => {
        $(
            impl ToLeBytes for $t {
                fn to_le_bytes_vec(&self) -> Vec<u8> {
                    self.to_le_bytes().to_vec()
                }
            }
        )*
    };
}

// Implement the ToLeBytes trait for the given types
impl_to_le_bytes!(i8, i16, i32, i64, i128, u8, u16, u32, u64, f32, f64);

macro_rules! with_match_hashing_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use ArrowDataType::*;
    match $key_type {
        Int8 => __with_ty__! { Int8Type },
        Int16 => __with_ty__! { Int16Type },
        Int32 => __with_ty__! { Int32Type },
        Int64 => __with_ty__! { Int64Type },
        UInt8 => __with_ty__! { UInt8Type },
        UInt16 => __with_ty__! { UInt16Type },
        UInt32 => __with_ty__! { UInt32Type },
        UInt64 => __with_ty__! { UInt64Type },
        Float32 => __with_ty__! { Float32Type },
        Float64 => __with_ty__! { Float64Type },
        _ => return Err(DaftError::ValueError(format!(
            "Hash not implemented for type {:?}",
            $key_type
        )))
    }
})}

/// Hash an array with a given seed and hash function
///
/// # Arguments
///
/// * `array` - The array to hash
/// * `seed` - The seed to use for hashing
/// * `hash_function` - The hash function to use
///
/// # Returns
///
/// A new array with the same length as the input array, containing the hashes of the input array.
///
/// # Errors
///
/// Returns an error if the seed length does not match the array length, or if the seed data type is not uint64.
pub fn hash(
    array: &dyn Array,
    seed: Option<&PrimitiveArray<UInt64Type>>,
    hash_function: HashFunctionKind,
) -> DaftResult<PrimitiveArray<UInt64Type>> {
    if let Some(s) = seed {
        if s.len() != array.len() {
            return Err(DaftError::ValueError(format!(
                "seed length does not match array length: {} vs {}",
                s.len(),
                array.len()
            )));
        }

        if s.data_type() != &ArrowDataType::UInt64 {
            return Err(DaftError::ValueError(format!(
                "seed data type expected to be uint64, got {:?}",
                s.data_type()
            )));
        }
    }

    // Check for Time and Timestamp types first (logical type check)
    match array.data_type() {
        ArrowDataType::Time32(_) => {
            let time_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int32Type>>()
                .ok_or_else(|| {
                    DaftError::ValueError(
                        "Expected Time32 array to be PrimitiveArray<Int32Type>".to_string(),
                    )
                })?;
            return Ok(hash_primitive::<Int32Type>(time_array, seed, hash_function));
        }
        ArrowDataType::Time64(_) => {
            let time_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()
                .ok_or_else(|| {
                    DaftError::ValueError(
                        "Expected Time64 array to be PrimitiveArray<Int64Type>".to_string(),
                    )
                })?;
            return Ok(hash_primitive::<Int64Type>(time_array, seed, hash_function));
        }
        ArrowDataType::Timestamp(_, timezone) => {
            // Timestamps are stored as i64 values (microseconds since epoch)
            let timestamp_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()
                .ok_or_else(|| {
                    DaftError::ValueError(
                        "Expected Timestamp array to be PrimitiveArray<Int64Type>".to_string(),
                    )
                })?;

            // For timestamps with timezone, we need to include the timezone in the hash
            // to ensure that the same instant in different timezones produces different hashes
            if let Some(tz) = timezone {
                return Ok(hash_timestamp_with_timezone(
                    timestamp_array,
                    tz.as_ref(),
                    seed,
                    hash_function,
                ));
            } else {
                // For timestamps without timezone, just hash the timestamp value
                return Ok(hash_primitive::<Int64Type>(
                    timestamp_array,
                    seed,
                    hash_function,
                ));
            }
        }
        _ => {}
    }

    Ok(match array.data_type() {
        ArrowDataType::Null => {
            hash_null(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        ArrowDataType::Boolean => {
            hash_boolean(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        ArrowDataType::Int8 => {
            with_match_hashing_primitive_type!(array.data_type(), |$T| {
                hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
            })
        }
        ArrowDataType::Int16 => {
            with_match_hashing_primitive_type!(array.data_type(), |$T| {
                hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
            })
        }
        ArrowDataType::Int32 => {
            with_match_hashing_primitive_type!(array.data_type(), |$T| {
                hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
            })
        }
        ArrowDataType::Int64 => {
            with_match_hashing_primitive_type!(array.data_type(), |$T| {
                hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
            })
        }
        ArrowDataType::UInt8 => {
            with_match_hashing_primitive_type!(array.data_type(), |$T| {
                hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
            })
        }
        ArrowDataType::UInt16 => {
            with_match_hashing_primitive_type!(array.data_type(), |$T| {
                hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
            })
        }
        ArrowDataType::UInt32 => {
            with_match_hashing_primitive_type!(array.data_type(), |$T| {
                hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
            })
        }
        ArrowDataType::UInt64 => {
            with_match_hashing_primitive_type!(array.data_type(), |$T| {
                hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
            })
        }
        ArrowDataType::Float32 => {
            with_match_hashing_primitive_type!(array.data_type(), |$T| {
                hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
            })
        }
        ArrowDataType::Float64 => {
            with_match_hashing_primitive_type!(array.data_type(), |$T| {
                hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
            })
        }
        ArrowDataType::Decimal128(_precision, scale) => {
            // Use special decimal hashing that considers precision and scale
            let decimal_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Decimal128Type>>()
                .ok_or_else(|| {
                    DaftError::ValueError(
                        "Expected decimal array to be PrimitiveArray<Decimal128Type>".to_string(),
                    )
                })?;
            hash_decimal(decimal_array, seed, hash_function, *scale as usize)
        }
        ArrowDataType::Binary => {
            // Note: Arrow-rs Binary is i32 offset, but Daft uses LargeBinary (i64 offset)
            // For compatibility, assume we want LargeBinary
            return Err(DaftError::ValueError(
                "Binary (i32 offset) not supported, use LargeBinary".to_string(),
            ));
        }
        ArrowDataType::LargeBinary => {
            hash_large_binary(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        ArrowDataType::FixedSizeBinary(_) => {
            hash_fixed_size_binary(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        ArrowDataType::Utf8 => {
            // Note: Arrow-rs Utf8 is i32 offset, but Daft uses LargeUtf8 (i64 offset)
            // For compatibility, assume we want LargeUtf8
            return Err(DaftError::ValueError(
                "Utf8 (i32 offset) not supported, use LargeUtf8".to_string(),
            ));
        }
        ArrowDataType::LargeUtf8 => {
            hash_large_string(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        t => {
            return Err(DaftError::ValueError(format!(
                "Hash not implemented for type {t:?}"
            )));
        }
    })
}

fn hash_primitive<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    seed: Option<&PrimitiveArray<UInt64Type>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<UInt64Type>
where
    T::Native: ToLeBytes,
{
    fn xxhash<const NULL_HASH: u64, T: ArrowPrimitiveType, F: Fn(&[u8], u64) -> u64>(
        array: &PrimitiveArray<T>,
        seed: Option<&PrimitiveArray<UInt64Type>>,
        f: F,
    ) -> PrimitiveArray<UInt64Type>
    where
        T::Native: ToLeBytes,
    {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.iter())
                .map(|(v, s)| match (v, s) {
                    (Some(v), Some(s)) => f(&v.to_le_bytes_vec(), s),
                    _ => NULL_HASH,
                })
                .collect::<Vec<_>>()
        } else {
            array
                .iter()
                .map(|v| match v {
                    Some(v) => f(&v.to_le_bytes_vec(), 0),
                    None => NULL_HASH,
                })
                .collect::<Vec<_>>()
        };
        PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
    }

    match hash_function {
        HashFunctionKind::MurmurHash3 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let seed_val = s.unwrap_or(42);
                        let hasher = MurBuildHasher::new(seed_val as u32);
                        let mut hasher = hasher.build_hasher();
                        match v {
                            Some(v) => {
                                hasher.write(&v.to_le_bytes_vec());
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                let hasher = MurBuildHasher::new(42);
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = hasher.build_hasher();
                        match v {
                            Some(v) => {
                                hasher.write(&v.to_le_bytes_vec());
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::Sha1 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let mut hasher = Sha1Hasher::default();
                        // Note: Sha1Hasher doesn't support seeding in a standard way
                        // We write the seed first if present, then the value
                        if let Some(seed_val) = s {
                            hasher.write(&seed_val.to_le_bytes());
                        }
                        match v {
                            Some(v) => {
                                hasher.write(&v.to_le_bytes_vec());
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = Sha1Hasher::default();
                        match v {
                            Some(v) => {
                                hasher.write(&v.to_le_bytes_vec());
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::XxHash32 => {
            const NULL_HASH: u64 = const_xxh32::xxh32(b"", 0) as u64;
            xxhash::<NULL_HASH, _, _>(array, seed, |v, s| xxh32(v, s as u32) as u64)
        }
        HashFunctionKind::XxHash64 => {
            const NULL_HASH: u64 = const_xxh64::xxh64(b"", 0);
            xxhash::<NULL_HASH, _, _>(array, seed, xxh64)
        }
        HashFunctionKind::XxHash3_64 => {
            const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
            xxhash::<NULL_HASH, _, _>(array, seed, xxh3_64_with_seed)
        }
    }
}

fn hash_boolean(
    array: &BooleanArray,
    seed: Option<&PrimitiveArray<UInt64Type>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<UInt64Type> {
    fn xxhash<
        const NULL_HASH: u64,
        const TRUE_HASH: u64,
        const FALSE_HASH: u64,
        F: Fn(&[u8], u64) -> u64,
    >(
        array: &BooleanArray,
        seed: Option<&PrimitiveArray<UInt64Type>>,
        f: F,
    ) -> PrimitiveArray<UInt64Type> {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.iter())
                .map(|(v, s)| match (v, s) {
                    (Some(true), Some(s)) => f(b"1", s),
                    (Some(false), Some(s)) => f(b"0", s),
                    _ => NULL_HASH,
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
        PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
    }

    match hash_function {
        HashFunctionKind::MurmurHash3 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let seed_val = s.unwrap_or(42);
                        let hasher = MurBuildHasher::new(seed_val as u32);
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
                    .collect::<Vec<_>>()
            } else {
                let hasher = MurBuildHasher::new(42);
                array
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
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::Sha1 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let mut hasher = Sha1Hasher::default();
                        if let Some(seed_val) = s {
                            hasher.write(&seed_val.to_le_bytes());
                        }
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
                    .collect::<Vec<_>>()
            } else {
                array
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
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::XxHash32 => {
            const NULL_HASH: u64 = const_xxh32::xxh32(b"", 0) as u64;
            const FALSE_HASH: u64 = const_xxh32::xxh32(b"0", 0) as u64;
            const TRUE_HASH: u64 = const_xxh32::xxh32(b"1", 0) as u64;
            xxhash::<NULL_HASH, TRUE_HASH, FALSE_HASH, _>(array, seed, |v, s| {
                xxh32(v, s as u32) as u64
            })
        }
        HashFunctionKind::XxHash64 => {
            const NULL_HASH: u64 = const_xxh64::xxh64(b"", 0);
            const FALSE_HASH: u64 = const_xxh64::xxh64(b"0", 0);
            const TRUE_HASH: u64 = const_xxh64::xxh64(b"1", 0);
            xxhash::<NULL_HASH, TRUE_HASH, FALSE_HASH, _>(array, seed, xxh64)
        }
        HashFunctionKind::XxHash3_64 => {
            const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
            const FALSE_HASH: u64 = const_xxh3::xxh3_64(b"0");
            const TRUE_HASH: u64 = const_xxh3::xxh3_64(b"1");
            xxhash::<NULL_HASH, TRUE_HASH, FALSE_HASH, _>(array, seed, xxh3_64_with_seed)
        }
    }
}

fn hash_null(
    array: &NullArray,
    seed: Option<&PrimitiveArray<UInt64Type>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<UInt64Type> {
    fn xxhash<const NULL_HASH: u64, F: Fn(&[u8], u64) -> u64>(
        len: usize,
        seed: Option<&PrimitiveArray<UInt64Type>>,
        f: F,
    ) -> PrimitiveArray<UInt64Type> {
        let hashes = if let Some(seed) = seed {
            seed.iter()
                .map(|s| f(b"", s.unwrap_or(0)))
                .collect::<Vec<_>>()
        } else {
            (0..len).map(|_| NULL_HASH).collect::<Vec<_>>()
        };
        PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
    }

    match hash_function {
        HashFunctionKind::MurmurHash3 => {
            let hashes = if let Some(seed) = seed {
                seed.iter()
                    .map(|s| {
                        let seed_val = s.unwrap_or(42);
                        let hasher = MurBuildHasher::new(seed_val as u32);
                        let mut hasher = hasher.build_hasher();
                        hasher.write(b"");
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            } else {
                let hasher = MurBuildHasher::new(42);
                (0..array.len())
                    .map(|_| {
                        let mut hasher = hasher.build_hasher();
                        hasher.write(b"");
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::Sha1 => {
            let hashes = if let Some(seed) = seed {
                seed.iter()
                    .map(|s| {
                        let mut hasher = Sha1Hasher::default();
                        if let Some(seed_val) = s {
                            hasher.write(&seed_val.to_le_bytes());
                        }
                        hasher.write(b"");
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            } else {
                (0..array.len())
                    .map(|_| {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(b"");
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::XxHash32 => {
            const NULL_HASH: u64 = const_xxh32::xxh32(b"", 0) as u64;
            xxhash::<NULL_HASH, _>(array.len(), seed, |v, s| xxh32(v, s as u32) as u64)
        }
        HashFunctionKind::XxHash64 => {
            const NULL_HASH: u64 = const_xxh64::xxh64(b"", 0);
            xxhash::<NULL_HASH, _>(array.len(), seed, xxh64)
        }
        HashFunctionKind::XxHash3_64 => {
            const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
            xxhash::<NULL_HASH, _>(array.len(), seed, xxh3_64_with_seed)
        }
    }
}

fn hash_large_binary(
    array: &LargeBinaryArray,
    seed: Option<&PrimitiveArray<UInt64Type>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<UInt64Type> {
    fn xxhash<F: Fn(&[u8], u64) -> u64>(
        array: &LargeBinaryArray,
        seed: Option<&PrimitiveArray<UInt64Type>>,
        f: F,
    ) -> PrimitiveArray<UInt64Type> {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.iter())
                .map(|(v, s)| f(v.unwrap_or(b""), s.unwrap_or(0)))
                .collect::<Vec<_>>()
        } else {
            array
                .iter()
                .map(|v| f(v.unwrap_or(b""), 0))
                .collect::<Vec<_>>()
        };
        PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
    }

    match hash_function {
        HashFunctionKind::XxHash32 => xxhash(array, seed, |v, s| xxh32(v, s as u32) as u64),
        HashFunctionKind::XxHash64 => xxhash(array, seed, xxh64),
        HashFunctionKind::XxHash3_64 => xxhash(array, seed, xxh3_64_with_seed),
        HashFunctionKind::MurmurHash3 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let seed_val = s.unwrap_or(42);
                        let hasher = MurBuildHasher::new(seed_val as u32);
                        let mut hasher = hasher.build_hasher();
                        hasher.write(v.unwrap_or(b""));
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            } else {
                let hasher = MurBuildHasher::new(42);
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = hasher.build_hasher();
                        hasher.write(v.unwrap_or(b""));
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::Sha1 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let mut hasher = Sha1Hasher::default();
                        if let Some(seed_val) = s {
                            hasher.write(&seed_val.to_le_bytes());
                        }
                        hasher.write(v.unwrap_or(b""));
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            } else {
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(v.unwrap_or(b""));
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
    }
}

fn hash_fixed_size_binary(
    array: &FixedSizeBinaryArray,
    seed: Option<&PrimitiveArray<UInt64Type>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<UInt64Type> {
    fn xxhash<F: Fn(&[u8], u64) -> u64>(
        array: &FixedSizeBinaryArray,
        seed: Option<&PrimitiveArray<UInt64Type>>,
        f: F,
    ) -> PrimitiveArray<UInt64Type> {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.iter())
                .map(|(v, s)| f(v.unwrap_or(&[]), s.unwrap_or(0)))
                .collect::<Vec<_>>()
        } else {
            array
                .iter()
                .map(|v| f(v.unwrap_or(&[]), 0))
                .collect::<Vec<_>>()
        };
        PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
    }

    match hash_function {
        HashFunctionKind::XxHash32 => xxhash(array, seed, |v, s| xxh32(v, s as u32) as u64),
        HashFunctionKind::XxHash64 => xxhash(array, seed, xxh64),
        HashFunctionKind::XxHash3_64 => xxhash(array, seed, xxh3_64_with_seed),
        HashFunctionKind::MurmurHash3 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let seed_val = s.unwrap_or(42);
                        let hasher = MurBuildHasher::new(seed_val as u32);
                        let mut hasher = hasher.build_hasher();
                        hasher.write(v.unwrap_or(&[]));
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            } else {
                let hasher = MurBuildHasher::new(42);
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = hasher.build_hasher();
                        hasher.write(v.unwrap_or(&[]));
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::Sha1 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let mut hasher = Sha1Hasher::default();
                        if let Some(seed_val) = s {
                            hasher.write(&seed_val.to_le_bytes());
                        }
                        hasher.write(v.unwrap_or(&[]));
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            } else {
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(v.unwrap_or(&[]));
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
    }
}

fn hash_large_string(
    array: &LargeStringArray,
    seed: Option<&PrimitiveArray<UInt64Type>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<UInt64Type> {
    fn xxhash<F: Fn(&[u8], u64) -> u64>(
        array: &LargeStringArray,
        seed: Option<&PrimitiveArray<UInt64Type>>,
        f: F,
    ) -> PrimitiveArray<UInt64Type> {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.iter())
                .map(|(v, s)| f(v.unwrap_or("").as_bytes(), s.unwrap_or(0)))
                .collect::<Vec<_>>()
        } else {
            array
                .iter()
                .map(|v| f(v.unwrap_or("").as_bytes(), 0))
                .collect::<Vec<_>>()
        };
        PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
    }

    match hash_function {
        HashFunctionKind::XxHash32 => xxhash(array, seed, |v, s| xxh32(v, s as u32) as u64),
        HashFunctionKind::XxHash64 => xxhash(array, seed, xxh64),
        HashFunctionKind::XxHash3_64 => xxhash(array, seed, xxh3_64_with_seed),
        HashFunctionKind::MurmurHash3 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let seed_val = s.unwrap_or(42);
                        let hasher = MurBuildHasher::new(seed_val as u32);
                        let mut hasher = hasher.build_hasher();
                        hasher.write(v.unwrap_or("").as_bytes());
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            } else {
                let hasher = MurBuildHasher::new(42);
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = hasher.build_hasher();
                        hasher.write(v.unwrap_or("").as_bytes());
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::Sha1 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let mut hasher = Sha1Hasher::default();
                        if let Some(seed_val) = s {
                            hasher.write(&seed_val.to_le_bytes());
                        }
                        hasher.write(v.unwrap_or("").as_bytes());
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            } else {
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(v.unwrap_or("").as_bytes());
                        hasher.finish()
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
    }
}

fn hash_timestamp_with_timezone(
    array: &PrimitiveArray<Int64Type>,
    timezone: &str,
    seed: Option<&PrimitiveArray<UInt64Type>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<UInt64Type> {
    fn xxhash<const NULL_HASH: u64, F: Fn(&[u8], u64) -> u64>(
        array: &PrimitiveArray<Int64Type>,
        timezone: &str,
        seed: Option<&PrimitiveArray<UInt64Type>>,
        f: F,
    ) -> PrimitiveArray<UInt64Type> {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.iter())
                .map(|(v, s)| match (v, s) {
                    (Some(v), Some(s)) => {
                        // Combine timestamp and timezone for hashing
                        let mut combined = Vec::new();
                        combined.extend_from_slice(&v.to_le_bytes());
                        combined.extend_from_slice(timezone.as_bytes());
                        f(&combined, s)
                    }
                    _ => NULL_HASH,
                })
                .collect::<Vec<_>>()
        } else {
            array
                .iter()
                .map(|v| match v {
                    Some(v) => {
                        // Combine timestamp and timezone for hashing
                        let mut combined = Vec::new();
                        combined.extend_from_slice(&v.to_le_bytes());
                        combined.extend_from_slice(timezone.as_bytes());
                        f(&combined, 0)
                    }
                    None => NULL_HASH,
                })
                .collect::<Vec<_>>()
        };
        PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
    }

    // For timestamps with timezone, we combine the timestamp value with the timezone string
    // to ensure that the same instant in different timezones produces different hashes
    match hash_function {
        HashFunctionKind::XxHash32 => {
            const NULL_HASH: u64 = const_xxh32::xxh32(b"", 0) as u64;
            xxhash::<NULL_HASH, _>(array, timezone, seed, |v, s| xxh32(v, s as u32) as u64)
        }
        HashFunctionKind::XxHash64 => {
            const NULL_HASH: u64 = const_xxh64::xxh64(b"", 0);
            xxhash::<NULL_HASH, _>(array, timezone, seed, xxh64)
        }
        HashFunctionKind::XxHash3_64 => {
            const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
            xxhash::<NULL_HASH, _>(array, timezone, seed, xxh3_64_with_seed)
        }
        HashFunctionKind::MurmurHash3 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let seed_val = s.unwrap_or(42);
                        let hasher = MurBuildHasher::new(seed_val as u32);
                        let mut hasher = hasher.build_hasher();
                        match v {
                            Some(v) => {
                                // Combine timestamp and timezone for hashing
                                hasher.write(&v.to_le_bytes());
                                hasher.write(timezone.as_bytes());
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                let hasher = MurBuildHasher::new(42);
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = hasher.build_hasher();
                        match v {
                            Some(v) => {
                                // Combine timestamp and timezone for hashing
                                hasher.write(&v.to_le_bytes());
                                hasher.write(timezone.as_bytes());
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::Sha1 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let mut hasher = Sha1Hasher::default();
                        if let Some(seed_val) = s {
                            hasher.write(&seed_val.to_le_bytes());
                        }
                        match v {
                            Some(v) => {
                                // Combine timestamp and timezone for hashing
                                hasher.write(&v.to_le_bytes());
                                hasher.write(timezone.as_bytes());
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = Sha1Hasher::default();
                        match v {
                            Some(v) => {
                                // Combine timestamp and timezone for hashing
                                hasher.write(&v.to_le_bytes());
                                hasher.write(timezone.as_bytes());
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
    }
}

fn hash_decimal(
    array: &PrimitiveArray<Decimal128Type>,
    seed: Option<&PrimitiveArray<UInt64Type>>,
    hash_function: HashFunctionKind,
    scale: usize,
) -> PrimitiveArray<UInt64Type> {
    // For decimal hashing, we preserve the exact representation including scale
    // Different scales should produce different hashes (123, 123.0, 123.00 are different)
    // We convert to string representation that preserves the scale information
    fn format_decimal(value: i128, scale: usize) -> Vec<u8> {
        if value == 0 {
            // For zero, return "0.000..." with the appropriate number of decimal places
            let mut result = String::from("0");
            if scale > 0 {
                result.push('.');
                result.push_str(&"0".repeat(scale));
            }
            return result.into_bytes();
        }

        // Handle negative values
        let (is_negative, abs_value) = if value < 0 {
            (true, (-value) as u128)
        } else {
            (false, value as u128)
        };

        // Convert to string with proper decimal placement
        let value_str = abs_value.to_string();
        let mut result = String::new();

        if is_negative {
            result.push('-');
        }

        if scale == 0 {
            // No decimal places
            result.push_str(&value_str);
        } else if value_str.len() <= scale {
            // Value is smaller than scale, so it's 0.00...value
            result.push('0');
            result.push('.');
            // Add leading zeros
            result.push_str(&"0".repeat(scale - value_str.len()));
            result.push_str(&value_str);
        } else {
            // Value has both integer and fractional parts
            let integer_part = &value_str[..value_str.len() - scale];
            let fractional_part = &value_str[value_str.len() - scale..];
            result.push_str(integer_part);
            result.push('.');
            result.push_str(fractional_part);
        }

        result.into_bytes()
    }

    fn xxhash<const NULL_HASH: u64, F: Fn(&[u8], u64) -> u64>(
        array: &PrimitiveArray<Decimal128Type>,
        seed: Option<&PrimitiveArray<UInt64Type>>,
        f: F,
        scale: usize,
    ) -> PrimitiveArray<UInt64Type> {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.iter())
                .map(|(v, s)| match (v, s) {
                    (Some(v), Some(s)) => {
                        let formatted = format_decimal(v, scale);
                        f(&formatted, s)
                    }
                    _ => NULL_HASH,
                })
                .collect::<Vec<_>>()
        } else {
            array
                .iter()
                .map(|v| match v {
                    Some(v) => {
                        let formatted = format_decimal(v, scale);
                        f(&formatted, 0)
                    }
                    None => NULL_HASH,
                })
                .collect::<Vec<_>>()
        };
        PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
    }

    match hash_function {
        HashFunctionKind::XxHash32 => {
            const NULL_HASH: u64 = const_xxh32::xxh32(b"", 0) as u64;
            xxhash::<NULL_HASH, _>(array, seed, |v, s| xxh32(v, s as u32) as u64, scale)
        }
        HashFunctionKind::XxHash64 => {
            const NULL_HASH: u64 = const_xxh64::xxh64(b"", 0);
            xxhash::<NULL_HASH, _>(array, seed, xxh64, scale)
        }
        HashFunctionKind::XxHash3_64 => {
            const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
            xxhash::<NULL_HASH, _>(array, seed, xxh3_64_with_seed, scale)
        }
        HashFunctionKind::MurmurHash3 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let seed_val = s.unwrap_or(42);
                        let hasher = MurBuildHasher::new(seed_val as u32);
                        let mut hasher = hasher.build_hasher();
                        match v {
                            Some(v) => {
                                let formatted = format_decimal(v, scale);
                                hasher.write(&formatted);
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                let hasher = MurBuildHasher::new(42);
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = hasher.build_hasher();
                        match v {
                            Some(v) => {
                                let formatted = format_decimal(v, scale);
                                hasher.write(&formatted);
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
        HashFunctionKind::Sha1 => {
            let hashes = if let Some(seed) = seed {
                array
                    .iter()
                    .zip(seed.iter())
                    .map(|(v, s)| {
                        let mut hasher = Sha1Hasher::default();
                        if let Some(seed_val) = s {
                            hasher.write(&seed_val.to_le_bytes());
                        }
                        match v {
                            Some(v) => {
                                let formatted = format_decimal(v, scale);
                                hasher.write(&formatted);
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                array
                    .iter()
                    .map(|v| {
                        let mut hasher = Sha1Hasher::default();
                        match v {
                            Some(v) => {
                                let formatted = format_decimal(v, scale);
                                hasher.write(&formatted);
                                hasher.finish()
                            }
                            None => {
                                hasher.write(b"");
                                hasher.finish()
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            };
            PrimitiveArray::<UInt64Type>::from_iter_values(hashes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_primitive_i32_no_nulls() {
        let array = PrimitiveArray::<Int32Type>::from(vec![1, 2, 3, 4, 5]);
        let result = hash_primitive(&array, None, HashFunctionKind::XxHash3_64);
        assert_eq!(result.len(), 5);
        assert_eq!(result.null_count(), 0);
        // Verify deterministic: same input produces same output
        let result2 = hash_primitive(&array, None, HashFunctionKind::XxHash3_64);
        assert_eq!(result.values(), result2.values());
    }

    #[test]
    fn test_hash_primitive_i32_with_nulls() {
        let array = PrimitiveArray::<Int32Type>::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let result = hash_primitive(&array, None, HashFunctionKind::XxHash3_64);
        assert_eq!(result.len(), 5);
        // Nulls should produce consistent hash
        assert_eq!(result.value(1), result.value(3));
    }

    #[test]
    fn test_hash_primitive_with_seed() {
        let array = PrimitiveArray::<Int32Type>::from(vec![1, 2, 3]);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![42, 42, 42]);
        let result = hash_primitive(&array, Some(&seed), HashFunctionKind::XxHash3_64);
        assert_eq!(result.len(), 3);
        // Hash with seed should differ from hash without seed
        let result_no_seed = hash_primitive(&array, None, HashFunctionKind::XxHash3_64);
        assert_ne!(result.values(), result_no_seed.values());
    }

    #[test]
    fn test_hash_boolean() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let result = hash_boolean(&array, None, HashFunctionKind::XxHash3_64);
        assert_eq!(result.len(), 3);
        // True and false should have different hashes
        assert_ne!(result.value(0), result.value(1));
    }

    #[test]
    fn test_hash_null() {
        let array = NullArray::new(5);
        let result = hash_null(&array, None, HashFunctionKind::XxHash3_64);
        assert_eq!(result.len(), 5);
        // All nulls should hash to the same value
        let first = result.value(0);
        for i in 1..5 {
            assert_eq!(result.value(i), first);
        }
    }

    #[test]
    fn test_hash_utf8() {
        let array = LargeStringArray::from(vec![Some("hello"), Some("world"), None]);
        let result = hash_large_string(&array, None, HashFunctionKind::XxHash3_64);
        assert_eq!(result.len(), 3);
        // Different strings should have different hashes
        assert_ne!(result.value(0), result.value(1));
    }

    #[test]
    fn test_hash_binary() {
        let array = LargeBinaryArray::from_opt_vec(vec![Some(b"hello"), Some(b"world"), None]);
        let result = hash_large_binary(&array, None, HashFunctionKind::XxHash3_64);
        assert_eq!(result.len(), 3);
        // Different binary values should have different hashes
        assert_ne!(result.value(0), result.value(1));
    }

    #[test]
    fn test_hash_fixed_size_binary() {
        let values: Vec<&[u8]> = vec![b"foo", b"bar", b"baz"];
        let array = FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap();
        let result = hash_fixed_size_binary(&array, None, HashFunctionKind::XxHash3_64);
        assert_eq!(result.len(), 3);
        // Different values should have different hashes
        assert_ne!(result.value(0), result.value(1));
    }

    #[test]
    fn test_hash_decimal() {
        let array = PrimitiveArray::<Decimal128Type>::from(vec![
            Some(12300_i128), // 123.00 with scale 2
            Some(12340_i128), // 123.40 with scale 2
            None,
        ]);
        let result = hash_decimal(&array, None, HashFunctionKind::XxHash3_64, 2);
        assert_eq!(result.len(), 3);
        // Different decimals should have different hashes
        assert_ne!(result.value(0), result.value(1));
    }

    #[test]
    fn test_hash_timestamp_with_timezone() {
        let array = PrimitiveArray::<Int64Type>::from(vec![Some(1000000), Some(2000000), None]);
        let result =
            hash_timestamp_with_timezone(&array, "UTC", None, HashFunctionKind::XxHash3_64);
        assert_eq!(result.len(), 3);
        // Different timestamps should have different hashes
        assert_ne!(result.value(0), result.value(1));
    }

    #[test]
    fn test_hash_main_entry_point() {
        let array = PrimitiveArray::<Int32Type>::from(vec![1, 2, 3]);
        let result = hash(&array, None, HashFunctionKind::XxHash3_64).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_hash_multiple_functions() {
        let array = PrimitiveArray::<Int32Type>::from(vec![1, 2, 3]);
        let result_xxh3 = hash_primitive(&array, None, HashFunctionKind::XxHash3_64);
        let result_xxh64 = hash_primitive(&array, None, HashFunctionKind::XxHash64);
        let result_murmur = hash_primitive(&array, None, HashFunctionKind::MurmurHash3);
        // Different hash functions should produce different results
        assert_ne!(result_xxh3.values(), result_xxh64.values());
        assert_ne!(result_xxh3.values(), result_murmur.values());
    }
}
