use std::hash::{BuildHasher, Hasher};

use daft_arrow::{
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
    const_xxh3, const_xxh32, const_xxh64, xxh3::xxh3_64_with_seed, xxh32::xxh32, xxh64::xxh64,
};

fn hash_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    fn xxhash<const NULL_HASH: u64, T: NativeType, F: Fn(&[u8], u64) -> u64>(
        array: &PrimitiveArray<T>,
        seed: Option<&PrimitiveArray<u64>>,
        f: F,
    ) -> PrimitiveArray<u64> {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.values_iter())
                .map(|(v, s)| match v {
                    Some(v) => f(v.to_le_bytes().as_ref(), *s),
                    None => NULL_HASH,
                })
                .collect::<Vec<_>>()
        } else {
            array
                .iter()
                .map(|v| match v {
                    Some(v) => f(v.to_le_bytes().as_ref(), 0),
                    None => NULL_HASH,
                })
                .collect::<Vec<_>>()
        };
        PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
    }

    match hash_function {
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
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    fn xxhash<
        const NULL_HASH: u64,
        const TRUE_HASH: u64,
        const FALSE_HASH: u64,
        F: Fn(&[u8], u64) -> u64,
    >(
        array: &BooleanArray,
        seed: Option<&PrimitiveArray<u64>>,
        f: F,
    ) -> PrimitiveArray<u64> {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.values_iter())
                .map(|(v, s)| match v {
                    Some(true) => f(b"1", *s),
                    Some(false) => f(b"0", *s),
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

    match hash_function {
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
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    fn xxhash<const NULL_HASH: u64, F: Fn(&[u8], u64) -> u64>(
        len: usize,
        seed: Option<&PrimitiveArray<u64>>,
        f: F,
    ) -> PrimitiveArray<u64> {
        let hashes = if let Some(seed) = seed {
            seed.values_iter().map(|s| f(b"", *s)).collect::<Vec<_>>()
        } else {
            (0..len).map(|_| NULL_HASH).collect::<Vec<_>>()
        };
        PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
    }

    match hash_function {
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

fn hash_binary<O: Offset>(
    array: &BinaryArray<O>,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    fn xxhash<O: Offset, F: Fn(&[u8], u64) -> u64>(
        array: &BinaryArray<O>,
        seed: Option<&PrimitiveArray<u64>>,
        f: F,
    ) -> PrimitiveArray<u64> {
        let hashes = if let Some(seed) = seed {
            array
                .values_iter()
                .zip(seed.values_iter())
                .map(|(v, s)| f(v, *s))
                .collect::<Vec<_>>()
        } else {
            array.values_iter().map(|v| f(v, 0)).collect::<Vec<_>>()
        };
        PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
    }

    match hash_function {
        HashFunctionKind::XxHash32 => xxhash(array, seed, |v, s| xxh32(v, s as u32) as u64),
        HashFunctionKind::XxHash64 => xxhash(array, seed, xxh64),
        HashFunctionKind::XxHash3_64 => xxhash(array, seed, xxh3_64_with_seed),
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
    fn xxhash<F: Fn(&[u8], u64) -> u64>(
        array: &FixedSizeBinaryArray,
        seed: Option<&PrimitiveArray<u64>>,
        f: F,
    ) -> PrimitiveArray<u64> {
        let hashes = if let Some(seed) = seed {
            array
                .values_iter()
                .zip(seed.values_iter())
                .map(|(v, s)| f(v, *s))
                .collect::<Vec<_>>()
        } else {
            array.values_iter().map(|v| f(v, 0)).collect::<Vec<_>>()
        };
        PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
    }

    match hash_function {
        HashFunctionKind::XxHash32 => xxhash(array, seed, |v, s| xxh32(v, s as u32) as u64),
        HashFunctionKind::XxHash64 => xxhash(array, seed, xxh64),
        HashFunctionKind::XxHash3_64 => xxhash(array, seed, xxh3_64_with_seed),
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
    fn xxhash<O: Offset, F: Fn(&[u8], u64) -> u64>(
        array: &Utf8Array<O>,
        seed: Option<&PrimitiveArray<u64>>,
        f: F,
    ) -> PrimitiveArray<u64> {
        let hashes = if let Some(seed) = seed {
            array
                .values_iter()
                .zip(seed.values_iter())
                .map(|(v, s)| f(v.as_bytes(), *s))
                .collect::<Vec<_>>()
        } else {
            array
                .values_iter()
                .map(|v| f(v.as_bytes(), 0))
                .collect::<Vec<_>>()
        };
        PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
    }

    match hash_function {
        HashFunctionKind::XxHash32 => xxhash(array, seed, |v, s| xxh32(v, s as u32) as u64),
        HashFunctionKind::XxHash64 => xxhash(array, seed, xxh64),
        HashFunctionKind::XxHash3_64 => xxhash(array, seed, xxh3_64_with_seed),
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

fn hash_timestamp_with_timezone(
    array: &PrimitiveArray<i64>,
    timezone: &str,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<u64> {
    fn xxhash<const NULL_HASH: u64, F: Fn(&[u8], u64) -> u64>(
        array: &PrimitiveArray<i64>,
        timezone: &str,
        seed: Option<&PrimitiveArray<u64>>,
        f: F,
    ) -> PrimitiveArray<u64> {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.values_iter())
                .map(|(v, s)| match v {
                    Some(v) => {
                        // Combine timestamp and timezone for hashing
                        let mut combined = Vec::new();
                        combined.extend_from_slice(&v.to_le_bytes());
                        combined.extend_from_slice(timezone.as_bytes());
                        f(&combined, *s)
                    }
                    None => NULL_HASH,
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
        PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
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
            let hasher = MurBuildHasher::new(seed.and_then(|s| s.get(0)).unwrap_or(42) as u32);
            let hashes = array
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
                .collect::<Vec<_>>();
            PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
        }
    }
}

fn hash_decimal(
    array: &PrimitiveArray<i128>,
    seed: Option<&PrimitiveArray<u64>>,
    hash_function: HashFunctionKind,
    scale: usize,
) -> PrimitiveArray<u64> {
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
        array: &PrimitiveArray<i128>,
        seed: Option<&PrimitiveArray<u64>>,
        f: F,
        scale: usize,
    ) -> PrimitiveArray<u64> {
        let hashes = if let Some(seed) = seed {
            array
                .iter()
                .zip(seed.values_iter())
                .map(|(v, s)| match v {
                    Some(v) => {
                        let formatted = format_decimal(*v, scale);
                        f(&formatted, *s)
                    }
                    None => NULL_HASH,
                })
                .collect::<Vec<_>>()
        } else {
            array
                .iter()
                .map(|v| match v {
                    Some(v) => {
                        let formatted = format_decimal(*v, scale);
                        f(&formatted, 0)
                    }
                    None => NULL_HASH,
                })
                .collect::<Vec<_>>()
        };
        PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
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
            let hasher = MurBuildHasher::new(seed.and_then(|s| s.get(0)).unwrap_or(42) as u32);
            let hashes = array
                .iter()
                .map(|v| {
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(v) => {
                            let formatted = format_decimal(*v, scale);
                            hasher.write(&formatted);
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
                            let formatted = format_decimal(*v, scale);
                            hasher.write(&formatted);
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

macro_rules! with_match_hashing_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use daft_arrow::datatypes::PrimitiveType::*;
    use daft_arrow::types::{days_ms, months_days_ns};
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

    // Check for Time and Timestamp types first (logical type check)
    match array.data_type() {
        DataType::Time32(_) => {
            let time_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .ok_or_else(|| {
                    Error::InvalidArgumentError(
                        "Expected Time32 array to be PrimitiveArray<i32>".to_string(),
                    )
                })?;
            return Ok(hash_primitive::<i32>(time_array, seed, hash_function));
        }
        DataType::Time64(_) => {
            let time_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .ok_or_else(|| {
                    Error::InvalidArgumentError(
                        "Expected Time64 array to be PrimitiveArray<i64>".to_string(),
                    )
                })?;
            return Ok(hash_primitive::<i64>(time_array, seed, hash_function));
        }
        DataType::Timestamp(_, timezone) => {
            // Timestamps are stored as i64 values (microseconds since epoch)
            let timestamp_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .ok_or_else(|| {
                    Error::InvalidArgumentError(
                        "Expected Timestamp array to be PrimitiveArray<i64>".to_string(),
                    )
                })?;

            // For timestamps with timezone, we need to include the timezone in the hash
            // to ensure that the same instant in different timezones produces different hashes
            if let Some(tz) = timezone {
                return Ok(hash_timestamp_with_timezone(
                    timestamp_array,
                    tz,
                    seed,
                    hash_function,
                ));
            } else {
                // For timestamps without timezone, just hash the timestamp value
                return Ok(hash_primitive::<i64>(timestamp_array, seed, hash_function));
            }
        }
        _ => {}
    }

    Ok(match array.data_type().to_physical_type() {
        PhysicalType::Null => {
            hash_null(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        PhysicalType::Boolean => {
            hash_boolean(array.as_any().downcast_ref().unwrap(), seed, hash_function)
        }
        PhysicalType::Primitive(primitive) => {
            // Check if this is a decimal type
            if let DataType::Decimal(_precision, scale) = array.data_type() {
                // Use special decimal hashing that considers precision and scale
                let decimal_array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i128>>()
                    .ok_or_else(|| {
                        Error::InvalidArgumentError(
                            "Expected decimal array to be PrimitiveArray<i128>".to_string(),
                        )
                    })?;
                hash_decimal(decimal_array, seed, hash_function, *scale)
            } else {
                // Use regular primitive hashing
                with_match_hashing_primitive_type!(primitive, |$T| {
                    hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed, hash_function)
                })
            }
        }
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
            )));
        }
    })
}
