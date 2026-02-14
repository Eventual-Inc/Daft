use std::hash::{BuildHasher, Hasher};

use arrow::{
    array::{
        Array, BooleanArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray, NullArray,
        PrimitiveArray, UInt64Array,
    },
    buffer::{MutableBuffer, ScalarBuffer},
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

/// Helper trait to convert primitive types to bytes for hashing without heap allocation
trait ToLeBytes {
    type Bytes: AsRef<[u8]>;
    fn to_le_bytes_arr(&self) -> Self::Bytes;
}

macro_rules! impl_to_le_bytes {
    ($t:ty, $n:literal) => {
        impl ToLeBytes for $t {
            type Bytes = [u8; $n];
            #[inline]
            fn to_le_bytes_arr(&self) -> [u8; $n] {
                self.to_le_bytes()
            }
        }
    };
}

impl_to_le_bytes!(i8, 1);
impl_to_le_bytes!(i16, 2);
impl_to_le_bytes!(i32, 4);
impl_to_le_bytes!(i64, 8);
impl_to_le_bytes!(i128, 16);
impl_to_le_bytes!(u8, 1);
impl_to_le_bytes!(u16, 2);
impl_to_le_bytes!(u32, 4);
impl_to_le_bytes!(u64, 8);
impl_to_le_bytes!(f32, 4);
impl_to_le_bytes!(f64, 8);

/// Convert a MutableBuffer of u64 values into a PrimitiveArray<UInt64Type> with no null bitmap.
/// This is the fastest path: no null tracking, cache-line aligned, zero-copy into the final array.
#[inline(always)]
fn finish_buffer(buffer: MutableBuffer) -> PrimitiveArray<UInt64Type> {
    let sb = unsafe { ScalarBuffer::new_unchecked(buffer.into()) };
    UInt64Array::new(sb, None)
}

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
        let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());

        if array.null_count() == 0 {
            if let Some(seed) = seed {
                for (v, s) in array.values().iter().zip(seed.values()) {
                    buffer.push(f(v.to_le_bytes_arr().as_ref(), *s));
                }
            } else {
                for v in array.values() {
                    buffer.push(f(v.to_le_bytes_arr().as_ref(), 0));
                }
            }
        } else if let Some(seed) = seed {
            for (v, s) in array.iter().zip(seed.values()) {
                let hash = match v {
                    Some(v) => f(v.to_le_bytes_arr().as_ref(), *s),
                    None => NULL_HASH,
                };
                buffer.push(hash);
            }
        } else {
            for v in array {
                let hash = match v {
                    Some(v) => f(v.to_le_bytes_arr().as_ref(), 0),
                    None => NULL_HASH,
                };
                buffer.push(hash);
            }
        }
        finish_buffer(buffer)
    }

    match hash_function {
        HashFunctionKind::MurmurHash3 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if array.null_count() == 0 {
                if let Some(seed) = seed {
                    for (v, s) in array.values().iter().zip(seed.values().iter()) {
                        let hasher = MurBuildHasher::new(*s as u32);
                        let mut hasher = hasher.build_hasher();
                        hasher.write(v.to_le_bytes_arr().as_ref());
                        buffer.push(hasher.finish());
                    }
                } else {
                    let hasher = MurBuildHasher::new(42);
                    for v in array.values() {
                        let mut hasher = hasher.build_hasher();
                        hasher.write(v.to_le_bytes_arr().as_ref());
                        buffer.push(hasher.finish());
                    }
                }
            } else if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values().iter()) {
                    let hasher = MurBuildHasher::new(*s as u32);
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(v) => {
                            hasher.write(v.to_le_bytes_arr().as_ref());
                            buffer.push(hasher.finish());
                        }
                        None => {
                            hasher.write(b"");
                            buffer.push(hasher.finish());
                        }
                    }
                }
            } else {
                let hasher = MurBuildHasher::new(42);
                for v in array {
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(v) => {
                            hasher.write(v.to_le_bytes_arr().as_ref());
                            buffer.push(hasher.finish());
                        }
                        None => {
                            hasher.write(b"");
                            buffer.push(hasher.finish());
                        }
                    }
                }
            }
            finish_buffer(buffer)
        }
        HashFunctionKind::Sha1 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if array.null_count() == 0 {
                if let Some(seed) = seed {
                    for (v, s) in array.values().iter().zip(seed.values().iter()) {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(&s.to_le_bytes());
                        hasher.write(v.to_le_bytes_arr().as_ref());
                        buffer.push(hasher.finish());
                    }
                } else {
                    for v in array.values() {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(v.to_le_bytes_arr().as_ref());
                        buffer.push(hasher.finish());
                    }
                }
            } else if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values().iter()) {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(&s.to_le_bytes());
                    match v {
                        Some(v) => {
                            hasher.write(v.to_le_bytes_arr().as_ref());
                            buffer.push(hasher.finish());
                        }
                        None => {
                            hasher.write(b"");
                            buffer.push(hasher.finish());
                        }
                    }
                }
            } else {
                for v in array {
                    let mut hasher = Sha1Hasher::default();
                    match v {
                        Some(v) => {
                            hasher.write(v.to_le_bytes_arr().as_ref());
                            buffer.push(hasher.finish());
                        }
                        None => {
                            hasher.write(b"");
                            buffer.push(hasher.finish());
                        }
                    }
                }
            }
            finish_buffer(buffer)
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
        let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());

        if array.null_count() == 0 {
            if let Some(seed) = seed {
                for (v, s) in array.values().iter().zip(seed.values()) {
                    buffer.push(if v { f(b"1", *s) } else { f(b"0", *s) });
                }
            } else {
                for v in array.values() {
                    buffer.push(if v { TRUE_HASH } else { FALSE_HASH });
                }
            }
        } else if let Some(seed) = seed {
            for (v, s) in array.iter().zip(seed.values()) {
                let hash = match v {
                    Some(true) => f(b"1", *s),
                    Some(false) => f(b"0", *s),
                    _ => NULL_HASH,
                };
                buffer.push(hash);
            }
        } else {
            for value in array {
                let hash = match value {
                    Some(true) => TRUE_HASH,
                    Some(false) => FALSE_HASH,
                    None => NULL_HASH,
                };
                buffer.push(hash);
            }
        }
        finish_buffer(buffer)
    }

    match hash_function {
        HashFunctionKind::MurmurHash3 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if array.null_count() == 0 {
                if let Some(seed) = seed {
                    for (v, seed_val) in array.values().iter().zip(seed.values()) {
                        let hasher = MurBuildHasher::new(*seed_val as u32);
                        let mut hasher = hasher.build_hasher();
                        hasher.write(if v { b"1" } else { b"0" });
                        buffer.push(hasher.finish());
                    }
                } else {
                    let hasher = MurBuildHasher::new(42);
                    for v in array.values() {
                        let mut hasher = hasher.build_hasher();
                        hasher.write(if v { b"1" } else { b"0" });
                        buffer.push(hasher.finish());
                    }
                }
            } else if let Some(seed) = seed {
                for (v, seed_val) in array.iter().zip(seed.values()) {
                    let hasher = MurBuildHasher::new(*seed_val as u32);
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(true) => hasher.write(b"1"),
                        Some(false) => hasher.write(b"0"),
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            } else {
                let hasher = MurBuildHasher::new(42);
                for v in array {
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(true) => hasher.write(b"1"),
                        Some(false) => hasher.write(b"0"),
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
        }
        HashFunctionKind::Sha1 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if array.null_count() == 0 {
                if let Some(seed) = seed {
                    for (v, s) in array.values().iter().zip(seed.values()) {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(&s.to_le_bytes());
                        hasher.write(if v { b"1" } else { b"0" });
                        buffer.push(hasher.finish());
                    }
                } else {
                    for v in array.values() {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(if v { b"1" } else { b"0" });
                        buffer.push(hasher.finish());
                    }
                }
            } else if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(&s.to_le_bytes());
                    match v {
                        Some(true) => hasher.write(b"1"),
                        Some(false) => hasher.write(b"0"),
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            } else {
                for v in array {
                    let mut hasher = Sha1Hasher::default();
                    match v {
                        Some(true) => hasher.write(b"1"),
                        Some(false) => hasher.write(b"0"),
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
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
        let mut buffer = MutableBuffer::new(len * std::mem::size_of::<u64>());

        if let Some(seed) = seed {
            for s in seed.values() {
                buffer.push(f(b"", *s));
            }
        } else {
            for _ in 0..len {
                buffer.push(NULL_HASH);
            }
        }
        finish_buffer(buffer)
    }

    match hash_function {
        HashFunctionKind::MurmurHash3 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if let Some(seed) = seed {
                for s in seed.values() {
                    let hasher = MurBuildHasher::new(*s as u32);
                    let mut hasher = hasher.build_hasher();
                    hasher.write(b"");
                    buffer.push(hasher.finish());
                }
            } else {
                let hasher = MurBuildHasher::new(42);
                for _ in 0..array.len() {
                    let mut hasher = hasher.build_hasher();
                    hasher.write(b"");
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
        }
        HashFunctionKind::Sha1 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if let Some(seed) = seed {
                for s in seed.values() {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(&s.to_le_bytes());
                    hasher.write(b"");
                    buffer.push(hasher.finish());
                }
            } else {
                for _ in 0..array.len() {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(b"");
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
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
        let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());

        if let Some(seed) = seed {
            for (v, s) in array.iter().zip(seed.values()) {
                buffer.push(f(v.unwrap_or(b""), *s));
            }
        } else {
            for v in array {
                buffer.push(f(v.unwrap_or(b""), 0));
            }
        }
        finish_buffer(buffer)
    }

    match hash_function {
        HashFunctionKind::XxHash32 => xxhash(array, seed, |v, s| xxh32(v, s as u32) as u64),
        HashFunctionKind::XxHash64 => xxhash(array, seed, xxh64),
        HashFunctionKind::XxHash3_64 => xxhash(array, seed, xxh3_64_with_seed),
        HashFunctionKind::MurmurHash3 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let hasher = MurBuildHasher::new(*s as u32);
                    let mut hasher = hasher.build_hasher();
                    hasher.write(v.unwrap_or(b""));
                    buffer.push(hasher.finish());
                }
            } else {
                let hasher = MurBuildHasher::new(42);
                for v in array {
                    let mut hasher = hasher.build_hasher();
                    hasher.write(v.unwrap_or(b""));
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
        }
        HashFunctionKind::Sha1 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());

            if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(&s.to_le_bytes());
                    hasher.write(v.unwrap_or(b""));
                    buffer.push(hasher.finish());
                }
            } else {
                for v in array {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(v.unwrap_or(b""));
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
        }
    }
}

fn hash_fixed_size_binary(
    array: &FixedSizeBinaryArray,
    seed: Option<&PrimitiveArray<UInt64Type>>,
    hash_function: HashFunctionKind,
) -> PrimitiveArray<UInt64Type> {
    // Create a zero-filled buffer for null values
    let size = if let ArrowDataType::FixedSizeBinary(s) = array.data_type() {
        *s as usize
    } else {
        unreachable!("FixedSizeBinaryArray must have DataType::FixedSizeBinary(..)")
    };
    let zero_buffer = vec![0u8; size];

    fn xxhash<F: Fn(&[u8], u64) -> u64>(
        array: &FixedSizeBinaryArray,
        seed: Option<&PrimitiveArray<UInt64Type>>,
        zero_buffer: &[u8],
        f: F,
    ) -> PrimitiveArray<UInt64Type> {
        let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
        if let Some(seed) = seed {
            for (v, s) in array.iter().zip(seed.values()) {
                buffer.push(f(v.unwrap_or(zero_buffer), *s));
            }
        } else {
            for v in array {
                buffer.push(f(v.unwrap_or(zero_buffer), 0));
            }
        }
        finish_buffer(buffer)
    }

    match hash_function {
        HashFunctionKind::XxHash32 => {
            xxhash(array, seed, &zero_buffer, |v, s| xxh32(v, s as u32) as u64)
        }
        HashFunctionKind::XxHash64 => xxhash(array, seed, &zero_buffer, xxh64),
        HashFunctionKind::XxHash3_64 => xxhash(array, seed, &zero_buffer, xxh3_64_with_seed),
        HashFunctionKind::MurmurHash3 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let hasher = MurBuildHasher::new(*s as u32);
                    let mut hasher = hasher.build_hasher();
                    hasher.write(v.unwrap_or(&zero_buffer));
                    buffer.push(hasher.finish());
                }
            } else {
                let hasher = MurBuildHasher::new(42);
                for v in array {
                    let mut hasher = hasher.build_hasher();
                    hasher.write(v.unwrap_or(&zero_buffer));
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
        }
        HashFunctionKind::Sha1 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(&s.to_le_bytes());
                    hasher.write(v.unwrap_or(&zero_buffer));
                    buffer.push(hasher.finish());
                }
            } else {
                for v in array {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(v.unwrap_or(&zero_buffer));
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
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
        let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());

        if let Some(seed) = seed {
            for (v, s) in array.iter().zip(seed.values()) {
                buffer.push(f(v.unwrap_or("").as_bytes(), *s));
            }
        } else {
            for v in array {
                buffer.push(f(v.unwrap_or("").as_bytes(), 0));
            }
        }
        finish_buffer(buffer)
    }

    match hash_function {
        HashFunctionKind::XxHash32 => xxhash(array, seed, |v, s| xxh32(v, s as u32) as u64),
        HashFunctionKind::XxHash64 => xxhash(array, seed, xxh64),
        HashFunctionKind::XxHash3_64 => xxhash(array, seed, xxh3_64_with_seed),
        HashFunctionKind::MurmurHash3 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let hasher = MurBuildHasher::new(*s as u32);
                    let mut hasher = hasher.build_hasher();
                    hasher.write(v.unwrap_or("").as_bytes());
                    buffer.push(hasher.finish());
                }
            } else {
                let hasher = MurBuildHasher::new(42);
                for v in array {
                    let mut hasher = hasher.build_hasher();
                    hasher.write(v.unwrap_or("").as_bytes());
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
        }
        HashFunctionKind::Sha1 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(&s.to_le_bytes());
                    hasher.write(v.unwrap_or("").as_bytes());
                    buffer.push(hasher.finish());
                }
            } else {
                for v in array {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(v.unwrap_or("").as_bytes());
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
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
        let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
        let tz_bytes = timezone.as_bytes();
        let mut combined = Vec::with_capacity(8 + tz_bytes.len());

        if array.null_count() == 0 {
            if let Some(seed) = seed {
                for (v, s) in array.values().iter().zip(seed.values()) {
                    combined.clear();
                    combined.extend_from_slice(&v.to_le_bytes());
                    combined.extend_from_slice(tz_bytes);
                    buffer.push(f(&combined, *s));
                }
            } else {
                for v in array.values() {
                    combined.clear();
                    combined.extend_from_slice(&v.to_le_bytes());
                    combined.extend_from_slice(tz_bytes);
                    buffer.push(f(&combined, 0));
                }
            }
        } else if let Some(seed) = seed {
            for (v, s) in array.iter().zip(seed.values()) {
                let hash = match v {
                    Some(v) => {
                        combined.clear();
                        combined.extend_from_slice(&v.to_le_bytes());
                        combined.extend_from_slice(tz_bytes);
                        f(&combined, *s)
                    }
                    None => NULL_HASH,
                };
                buffer.push(hash);
            }
        } else {
            for v in array {
                let hash = match v {
                    Some(v) => {
                        combined.clear();
                        combined.extend_from_slice(&v.to_le_bytes());
                        combined.extend_from_slice(tz_bytes);
                        f(&combined, 0)
                    }
                    None => NULL_HASH,
                };
                buffer.push(hash);
            }
        }
        finish_buffer(buffer)
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
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            let tz = timezone.as_bytes();
            if array.null_count() == 0 {
                if let Some(seed) = seed {
                    for (v, s) in array.values().iter().zip(seed.values()) {
                        let hasher = MurBuildHasher::new(*s as u32);
                        let mut hasher = hasher.build_hasher();
                        hasher.write(&v.to_le_bytes());
                        hasher.write(tz);
                        buffer.push(hasher.finish());
                    }
                } else {
                    let hasher = MurBuildHasher::new(42);
                    for v in array.values() {
                        let mut hasher = hasher.build_hasher();
                        hasher.write(&v.to_le_bytes());
                        hasher.write(tz);
                        buffer.push(hasher.finish());
                    }
                }
            } else if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let hasher = MurBuildHasher::new(*s as u32);
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(v) => {
                            hasher.write(&v.to_le_bytes());
                            hasher.write(tz);
                        }
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            } else {
                let hasher = MurBuildHasher::new(42);
                for v in array {
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(v) => {
                            hasher.write(&v.to_le_bytes());
                            hasher.write(tz);
                        }
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
        }
        HashFunctionKind::Sha1 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            let tz = timezone.as_bytes();
            if array.null_count() == 0 {
                if let Some(seed) = seed {
                    for (v, s) in array.values().iter().zip(seed.values()) {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(&s.to_le_bytes());
                        hasher.write(&v.to_le_bytes());
                        hasher.write(tz);
                        buffer.push(hasher.finish());
                    }
                } else {
                    for v in array.values() {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(&v.to_le_bytes());
                        hasher.write(tz);
                        buffer.push(hasher.finish());
                    }
                }
            } else if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(&s.to_le_bytes());
                    match v {
                        Some(v) => {
                            hasher.write(&v.to_le_bytes());
                            hasher.write(tz);
                        }
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            } else {
                for v in array {
                    let mut hasher = Sha1Hasher::default();
                    match v {
                        Some(v) => {
                            hasher.write(&v.to_le_bytes());
                            hasher.write(tz);
                        }
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
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
        let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());

        if array.null_count() == 0 {
            if let Some(seed) = seed {
                for (v, s) in array.values().iter().zip(seed.values()) {
                    let formatted = format_decimal(*v, scale);
                    buffer.push(f(&formatted, *s));
                }
            } else {
                for v in array.values() {
                    let formatted = format_decimal(*v, scale);
                    buffer.push(f(&formatted, 0));
                }
            }
        } else if let Some(seed) = seed {
            for (v, s) in array.iter().zip(seed.values()) {
                let hash = match v {
                    Some(v) => {
                        let formatted = format_decimal(v, scale);
                        f(&formatted, *s)
                    }
                    None => NULL_HASH,
                };
                buffer.push(hash);
            }
        } else {
            for v in array {
                let hash = match v {
                    Some(v) => {
                        let formatted = format_decimal(v, scale);
                        f(&formatted, 0)
                    }
                    None => NULL_HASH,
                };
                buffer.push(hash);
            }
        }
        finish_buffer(buffer)
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
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if array.null_count() == 0 {
                if let Some(seed) = seed {
                    for (v, s) in array.values().iter().zip(seed.values()) {
                        let hasher = MurBuildHasher::new(*s as u32);
                        let mut hasher = hasher.build_hasher();
                        let formatted = format_decimal(*v, scale);
                        hasher.write(&formatted);
                        buffer.push(hasher.finish());
                    }
                } else {
                    let hasher = MurBuildHasher::new(42);
                    for v in array.values() {
                        let mut hasher = hasher.build_hasher();
                        let formatted = format_decimal(*v, scale);
                        hasher.write(&formatted);
                        buffer.push(hasher.finish());
                    }
                }
            } else if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let hasher = MurBuildHasher::new(*s as u32);
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(v) => {
                            let formatted = format_decimal(v, scale);
                            hasher.write(&formatted);
                        }
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            } else {
                let hasher = MurBuildHasher::new(42);
                for v in array {
                    let mut hasher = hasher.build_hasher();
                    match v {
                        Some(v) => {
                            let formatted = format_decimal(v, scale);
                            hasher.write(&formatted);
                        }
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
        }
        HashFunctionKind::Sha1 => {
            let mut buffer = MutableBuffer::new(array.len() * std::mem::size_of::<u64>());
            if array.null_count() == 0 {
                if let Some(seed) = seed {
                    for (v, s) in array.values().iter().zip(seed.values()) {
                        let mut hasher = Sha1Hasher::default();
                        hasher.write(&s.to_le_bytes());
                        let formatted = format_decimal(*v, scale);
                        hasher.write(&formatted);
                        buffer.push(hasher.finish());
                    }
                } else {
                    for v in array.values() {
                        let mut hasher = Sha1Hasher::default();
                        let formatted = format_decimal(*v, scale);
                        hasher.write(&formatted);
                        buffer.push(hasher.finish());
                    }
                }
            } else if let Some(seed) = seed {
                for (v, s) in array.iter().zip(seed.values()) {
                    let mut hasher = Sha1Hasher::default();
                    hasher.write(&s.to_le_bytes());
                    match v {
                        Some(v) => {
                            let formatted = format_decimal(v, scale);
                            hasher.write(&formatted);
                        }
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            } else {
                for v in array {
                    let mut hasher = Sha1Hasher::default();
                    match v {
                        Some(v) => {
                            let formatted = format_decimal(v, scale);
                            hasher.write(&formatted);
                        }
                        None => hasher.write(b""),
                    }
                    buffer.push(hasher.finish());
                }
            }
            finish_buffer(buffer)
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

    // --- hash_primitive: all hash functions × no-nulls × seeded/unseeded ---

    #[test]
    fn test_hash_primitive_no_nulls_all_functions() {
        let array = PrimitiveArray::<Int64Type>::from(vec![10, 20, 30]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::XxHash64,
            HashFunctionKind::XxHash32,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_primitive(&array, None, hf);
            assert_eq!(result.len(), 3);
            assert_eq!(result.null_count(), 0);
            // Different values produce different hashes
            assert_ne!(result.value(0), result.value(1));
            // Deterministic
            let result2 = hash_primitive(&array, None, hf);
            assert_eq!(result.values(), result2.values());
        }
    }

    #[test]
    fn test_hash_primitive_no_nulls_seeded_all_functions() {
        let array = PrimitiveArray::<Int32Type>::from(vec![1, 2, 3]);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![100, 200, 300]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::XxHash64,
            HashFunctionKind::XxHash32,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_primitive(&array, Some(&seed), hf);
            assert_eq!(result.len(), 3);
            assert_eq!(result.null_count(), 0);
            let result_no_seed = hash_primitive(&array, None, hf);
            assert_ne!(result.values(), result_no_seed.values());
        }
    }

    // --- hash_primitive: with nulls × seeded/unseeded ---

    #[test]
    fn test_hash_primitive_with_nulls_all_functions() {
        let array = PrimitiveArray::<Int32Type>::from(vec![Some(1), None, Some(3), None, Some(5)]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::XxHash64,
            HashFunctionKind::XxHash32,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_primitive(&array, None, hf);
            assert_eq!(result.len(), 5);
            assert_eq!(result.null_count(), 0);
            // Null positions produce consistent hashes
            assert_eq!(result.value(1), result.value(3));
            // Non-null values differ from each other
            assert_ne!(result.value(0), result.value(2));
        }
    }

    #[test]
    fn test_hash_primitive_with_nulls_seeded() {
        let array = PrimitiveArray::<Int32Type>::from(vec![Some(10), None, Some(30)]);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![7, 7, 7]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_primitive(&array, Some(&seed), hf);
            assert_eq!(result.len(), 3);
            assert_eq!(result.null_count(), 0);
        }
    }

    // --- hash_primitive: multiple primitive types ---

    #[test]
    fn test_hash_primitive_various_types() {
        // u64
        let arr = PrimitiveArray::<UInt64Type>::from(vec![1u64, 2, 3]);
        let r = hash_primitive(&arr, None, HashFunctionKind::XxHash3_64);
        assert_eq!(r.len(), 3);
        assert_ne!(r.value(0), r.value(1));

        // f64
        let arr = PrimitiveArray::<Float64Type>::from(vec![1.0, 2.0, 3.0]);
        let r = hash_primitive(&arr, None, HashFunctionKind::XxHash3_64);
        assert_eq!(r.len(), 3);
        assert_ne!(r.value(0), r.value(1));

        // i8
        let arr = PrimitiveArray::<Int8Type>::from(vec![1i8, 2, 3]);
        let r = hash_primitive(&arr, None, HashFunctionKind::MurmurHash3);
        assert_eq!(r.len(), 3);
        assert_ne!(r.value(0), r.value(1));
    }

    // --- hash_boolean: all paths ---

    #[test]
    fn test_hash_boolean_no_nulls_all_functions() {
        let array = BooleanArray::from(vec![true, false, true, false]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::XxHash64,
            HashFunctionKind::XxHash32,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_boolean(&array, None, hf);
            assert_eq!(result.len(), 4);
            assert_eq!(result.null_count(), 0);
            assert_ne!(result.value(0), result.value(1));
            // Same values produce same hashes
            assert_eq!(result.value(0), result.value(2));
            assert_eq!(result.value(1), result.value(3));
        }
    }

    #[test]
    fn test_hash_boolean_with_nulls_seeded() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![99, 99, 99]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_boolean(&array, Some(&seed), hf);
            assert_eq!(result.len(), 3);
            assert_eq!(result.null_count(), 0);
        }
    }

    // --- hash_null: seeded ---

    #[test]
    fn test_hash_null_seeded() {
        let array = NullArray::new(3);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![10, 20, 30]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_null(&array, Some(&seed), hf);
            assert_eq!(result.len(), 3);
        }
    }

    // --- hash_large_string: all paths ---

    #[test]
    fn test_hash_string_no_nulls_all_functions() {
        let array = LargeStringArray::from(vec!["hello", "world", "foo"]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::XxHash64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_large_string(&array, None, hf);
            assert_eq!(result.len(), 3);
            assert_ne!(result.value(0), result.value(1));
        }
    }

    #[test]
    fn test_hash_string_with_nulls_seeded() {
        let array = LargeStringArray::from(vec![Some("a"), None, Some("b")]);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![1, 2, 3]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_large_string(&array, Some(&seed), hf);
            assert_eq!(result.len(), 3);
        }
    }

    // --- hash_large_binary: all paths ---

    #[test]
    fn test_hash_binary_no_nulls_seeded() {
        let array =
            LargeBinaryArray::from_iter_values(vec![b"aaa".as_ref(), b"bbb".as_ref(), b"ccc"]);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![5, 5, 5]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_large_binary(&array, Some(&seed), hf);
            assert_eq!(result.len(), 3);
            assert_ne!(result.value(0), result.value(1));
        }
    }

    // --- hash_timestamp_with_timezone: all paths ---

    #[test]
    fn test_hash_timestamp_tz_no_nulls_all_functions() {
        let array = PrimitiveArray::<Int64Type>::from(vec![1000, 2000, 3000]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::XxHash64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_timestamp_with_timezone(&array, "UTC", None, hf);
            assert_eq!(result.len(), 3);
            assert_ne!(result.value(0), result.value(1));
        }
    }

    #[test]
    fn test_hash_timestamp_tz_with_nulls_seeded() {
        let array = PrimitiveArray::<Int64Type>::from(vec![Some(100), None, Some(300)]);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![42, 42, 42]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_timestamp_with_timezone(&array, "US/Eastern", Some(&seed), hf);
            assert_eq!(result.len(), 3);
        }
    }

    #[test]
    fn test_hash_timestamp_different_tz_different_hash() {
        let array = PrimitiveArray::<Int64Type>::from(vec![1000000]);
        let r1 = hash_timestamp_with_timezone(&array, "UTC", None, HashFunctionKind::XxHash3_64);
        let r2 =
            hash_timestamp_with_timezone(&array, "US/Pacific", None, HashFunctionKind::XxHash3_64);
        assert_ne!(r1.value(0), r2.value(0));
    }

    // --- hash_decimal: all paths ---

    #[test]
    fn test_hash_decimal_no_nulls_all_functions() {
        let array = PrimitiveArray::<Decimal128Type>::from(vec![12300i128, 45600, 78900]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::XxHash64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_decimal(&array, None, hf, 2);
            assert_eq!(result.len(), 3);
            assert_ne!(result.value(0), result.value(1));
        }
    }

    #[test]
    fn test_hash_decimal_with_nulls_seeded() {
        let array = PrimitiveArray::<Decimal128Type>::from(vec![Some(100i128), None, Some(300)]);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![1, 2, 3]);
        for hf in [
            HashFunctionKind::XxHash3_64,
            HashFunctionKind::MurmurHash3,
            HashFunctionKind::Sha1,
        ] {
            let result = hash_decimal(&array, Some(&seed), hf, 2);
            assert_eq!(result.len(), 3);
        }
    }

    // --- hash() entry point: various types ---

    #[test]
    fn test_hash_entry_point_all_types() {
        // i32
        let arr = PrimitiveArray::<Int32Type>::from(vec![1, 2, 3]);
        let r = hash(&arr, None, HashFunctionKind::XxHash3_64).unwrap();
        assert_eq!(r.len(), 3);

        // u64
        let arr = PrimitiveArray::<UInt64Type>::from(vec![10u64, 20, 30]);
        let r = hash(&arr, None, HashFunctionKind::MurmurHash3).unwrap();
        assert_eq!(r.len(), 3);

        // f64
        let arr = PrimitiveArray::<Float64Type>::from(vec![1.5, 2.5]);
        let r = hash(&arr, None, HashFunctionKind::Sha1).unwrap();
        assert_eq!(r.len(), 2);

        // boolean
        let arr = BooleanArray::from(vec![true, false]);
        let r = hash(&arr, None, HashFunctionKind::XxHash64).unwrap();
        assert_eq!(r.len(), 2);

        // null
        let arr = NullArray::new(4);
        let r = hash(&arr, None, HashFunctionKind::XxHash32).unwrap();
        assert_eq!(r.len(), 4);

        // string
        let arr = LargeStringArray::from(vec!["x", "y"]);
        let r = hash(&arr, None, HashFunctionKind::XxHash3_64).unwrap();
        assert_eq!(r.len(), 2);

        // binary
        let arr = LargeBinaryArray::from_opt_vec(vec![Some(b"a"), Some(b"b")]);
        let r = hash(&arr, None, HashFunctionKind::XxHash3_64).unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn test_hash_entry_point_seeded() {
        let arr = PrimitiveArray::<Int32Type>::from(vec![1, 2, 3]);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![42, 42, 42]);
        let r = hash(&arr, Some(&seed), HashFunctionKind::XxHash3_64).unwrap();
        assert_eq!(r.len(), 3);
        let r2 = hash(&arr, None, HashFunctionKind::XxHash3_64).unwrap();
        assert_ne!(r.values(), r2.values());
    }

    #[test]
    fn test_hash_entry_point_seed_length_mismatch() {
        let arr = PrimitiveArray::<Int32Type>::from(vec![1, 2, 3]);
        let seed = PrimitiveArray::<UInt64Type>::from(vec![42, 42]);
        let result = hash(&arr, Some(&seed), HashFunctionKind::XxHash3_64);
        assert!(result.is_err());
    }

    // --- empty arrays ---

    #[test]
    fn test_hash_empty_arrays() {
        let arr: PrimitiveArray<Int32Type> = PrimitiveArray::from(Vec::<i32>::new());
        let r = hash_primitive(&arr, None, HashFunctionKind::XxHash3_64);
        assert_eq!(r.len(), 0);

        let arr = BooleanArray::from(Vec::<bool>::new());
        let r = hash_boolean(&arr, None, HashFunctionKind::MurmurHash3);
        assert_eq!(r.len(), 0);

        let arr = NullArray::new(0);
        let r = hash_null(&arr, None, HashFunctionKind::Sha1);
        assert_eq!(r.len(), 0);
    }
}
