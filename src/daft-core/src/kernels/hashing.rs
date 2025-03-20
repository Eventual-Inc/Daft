use arrow2::{
    array::{
        Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, NullArray, PrimitiveArray,
        Utf8Array,
    },
    datatypes::{DataType, PhysicalType},
    error::{Error, Result},
    types::{NativeType, Offset},
};
use xxhash_rust::{
    const_xxh3,
    xxh3::{xxh3_64, xxh3_64_with_seed},
};

fn hash_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
    const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
    const BATCH_SIZE: usize = 16; // Optimal batch size for SIMD operations
    
    // Preallocate output vector with capacity to avoid reallocations
    let capacity = array.len();
    let mut hashes = Vec::with_capacity(capacity);
    
    // Fast path for arrays without nulls
    if array.null_count() == 0 {
        if let Some(seed) = seed {
            // Process with seed values in batches for better SIMD utilization
            let batch_count = (capacity + BATCH_SIZE - 1) / BATCH_SIZE;
            
            for batch_idx in 0..batch_count {
                let start_idx = batch_idx * BATCH_SIZE;
                let end_idx = (start_idx + BATCH_SIZE).min(capacity);
                
                // Process this batch
                for i in start_idx..end_idx {
                    let v = unsafe { array.value_unchecked(i) };
                    let s = *seed.value(i);
                    
                    // Prefetch next values if possible
                    #[cfg(feature = "nightly")]
                    if i + 4 < end_idx {
                        use std::intrinsics::prefetch_read_data;
                        unsafe {
                            // Prefetch next value for better cache utilization
                            prefetch_read_data(
                                array.values().as_ptr().add(i + 4) as *const i8,
                                3  // Prefetch level
                            );
                            
                            // Also prefetch seed value
                            prefetch_read_data(
                                seed.values().as_ptr().add(i + 4) as *const i8,
                                3
                            );
                        }
                    }
                    
                    hashes.push(xxh3_64_with_seed(v.to_le_bytes().as_ref(), s));
                }
            }
        } else {
            // Process without seed - even better vectorization potential
            // Use explicit batch processing for better SIMD utilization
            let batch_count = (capacity + BATCH_SIZE - 1) / BATCH_SIZE;
            
            for batch_idx in 0..batch_count {
                let start_idx = batch_idx * BATCH_SIZE;
                let end_idx = (start_idx + BATCH_SIZE).min(capacity);
                
                // Pre-compute byte representations for the batch to improve memory access patterns
                let mut batch_values = Vec::with_capacity(end_idx - start_idx);
                
                for i in start_idx..end_idx {
                    let v = unsafe { array.value_unchecked(i) };
                    batch_values.push(v.to_le_bytes());
                    
                    // Prefetch next values if possible (conditional compilation)
                    #[cfg(feature = "nightly")]
                    if i + 4 < end_idx {
                        use std::intrinsics::prefetch_read_data;
                        unsafe {
                            prefetch_read_data(
                                array.values().as_ptr().add(i + 4) as *const i8,
                                3  // Prefetch level
                            );
                        }
                    }
                }
                
                // Process batch values with sequential memory access pattern
                for bytes in batch_values {
                    hashes.push(xxh3_64(bytes.as_ref()));
                }
            }
        }
    } else {
        // Path for arrays with potential nulls
        let validity = array.validity();
        
        if let Some(seed) = seed {
            // With seed values - process in batches
            let batch_count = (capacity + BATCH_SIZE - 1) / BATCH_SIZE;
            
            for batch_idx in 0..batch_count {
                let start_idx = batch_idx * BATCH_SIZE;
                let end_idx = (start_idx + BATCH_SIZE).min(capacity);
                
                // Process this batch
                for i in start_idx..end_idx {
                    // If validity is None, all values are valid
                    // If validity is Some, check bitmap for validity at index i
                    if validity.map_or(true, |bitmap| bitmap.get_bit(i)) {
                        let v = unsafe { array.value_unchecked(i) };
                        hashes.push(xxh3_64_with_seed(v.to_le_bytes().as_ref(), *seed.value(i)));
                    } else {
                        hashes.push(NULL_HASH);
                    }
                    
                    // Prefetch next values if possible
                    #[cfg(feature = "nightly")]
                    if i + 4 < end_idx {
                        use std::intrinsics::prefetch_read_data;
                        unsafe {
                            prefetch_read_data(
                                array.values().as_ptr().add(i + 4) as *const i8,
                                3  // Prefetch level
                            );
                        }
                    }
                }
            }
        } else {
            // Without seed values - process in batches
            let batch_count = (capacity + BATCH_SIZE - 1) / BATCH_SIZE;
            
            for batch_idx in 0..batch_count {
                let start_idx = batch_idx * BATCH_SIZE;
                let end_idx = (start_idx + BATCH_SIZE).min(capacity);
                
                // Create small batch arrays to reduce branch prediction misses
                let mut valid_indices = Vec::with_capacity(BATCH_SIZE);
                let mut null_indices = Vec::with_capacity(BATCH_SIZE);
                
                // First, categorize each item in batch as valid or null
                for i in start_idx..end_idx {
                    if validity.map_or(true, |bitmap| bitmap.get_bit(i)) {
                        valid_indices.push(i);
                    } else {
                        null_indices.push(i);
                    }
                }
                
                // Process valid values with no branching - good for SIMD
                for &i in &valid_indices {
                    let v = unsafe { array.value_unchecked(i) };
                    hashes.push(xxh3_64(v.to_le_bytes().as_ref()));
                }
                
                // Process null values - simple constant assignment, also good for SIMD
                for _ in &null_indices {
                    hashes.push(NULL_HASH);
                }
            }
        }
    }
    
    PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
}

fn hash_boolean(array: &BooleanArray, seed: Option<&PrimitiveArray<u64>>) -> PrimitiveArray<u64> {
    const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
    const FALSE_HASH: u64 = const_xxh3::xxh3_64(b"0");
    const TRUE_HASH: u64 = const_xxh3::xxh3_64(b"1");

    // Preallocate with capacity to improve performance
    let capacity = array.len();
    let mut hashes = Vec::with_capacity(capacity);

    // Separate paths for with/without nulls for better vectorization
    if array.null_count() == 0 {
        if let Some(seed) = seed {
            // With seed values
            for (i, s) in seed.values_iter().enumerate().take(capacity) {
                if array.value(i) {
                    hashes.push(xxh3_64_with_seed(b"1", *s));
                } else {
                    hashes.push(xxh3_64_with_seed(b"0", *s));
                }
            }
        } else {
            // No seed - use precomputed hashes for better vectorization
            for i in 0..capacity {
                if array.value(i) {
                    hashes.push(TRUE_HASH);
                } else {
                    hashes.push(FALSE_HASH);
                }
            }
        }
    } else {
        // With potential nulls
        let validity = array.validity();
        
        if let Some(seed) = seed {
            // With seed values
            for i in 0..capacity {
                // If validity is None, all values are valid
                // If validity is Some, check bitmap for validity at index i
                if validity.map_or(true, |bitmap| bitmap.get_bit(i)) {
                    if array.value(i) {
                        hashes.push(xxh3_64_with_seed(b"1", *seed.value(i)));
                    } else {
                        hashes.push(xxh3_64_with_seed(b"0", *seed.value(i)));
                    }
                } else {
                    hashes.push(NULL_HASH);
                }
            }
        } else {
            // Without seed values
            for i in 0..capacity {
                if validity.map_or(true, |bitmap| bitmap.get_bit(i)) {
                    if array.value(i) {
                        hashes.push(TRUE_HASH);
                    } else {
                        hashes.push(FALSE_HASH);
                    }
                } else {
                    hashes.push(NULL_HASH);
                }
            }
        }
    }
    
    PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
}

fn hash_null(array: &NullArray, seed: Option<&PrimitiveArray<u64>>) -> PrimitiveArray<u64> {
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

fn hash_binary<O: Offset>(
    array: &BinaryArray<O>,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
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

fn hash_fixed_size_binary(
    array: &FixedSizeBinaryArray,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
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

fn hash_utf8<O: Offset>(
    array: &Utf8Array<O>,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
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

pub fn hash(array: &dyn Array, seed: Option<&PrimitiveArray<u64>>) -> Result<PrimitiveArray<u64>> {
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
        PhysicalType::Null => hash_null(array.as_any().downcast_ref().unwrap(), seed),
        PhysicalType::Boolean => hash_boolean(array.as_any().downcast_ref().unwrap(), seed),
        PhysicalType::Primitive(primitive) => with_match_hashing_primitive_type!(primitive, |$T| {
            hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed)
        }),
        PhysicalType::Binary => hash_binary::<i32>(array.as_any().downcast_ref().unwrap(), seed),
        PhysicalType::LargeBinary => {
            hash_binary::<i64>(array.as_any().downcast_ref().unwrap(), seed)
        }
        PhysicalType::FixedSizeBinary => {
            hash_fixed_size_binary(array.as_any().downcast_ref().unwrap(), seed)
        }
        PhysicalType::Utf8 => hash_utf8::<i32>(array.as_any().downcast_ref().unwrap(), seed),
        PhysicalType::LargeUtf8 => hash_utf8::<i64>(array.as_any().downcast_ref().unwrap(), seed),
        t => {
            return Err(Error::NotYetImplemented(format!(
                "Hash not implemented for type {t:?}"
            )))
        }
    })
}
