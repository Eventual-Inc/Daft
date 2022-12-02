use arrow2::{
    array::Array,
    array::{BinaryArray, PrimitiveArray, Utf8Array},
    datatypes::{DataType, PhysicalType},
    error::{Error, Result},
    types::{NativeType, Offset},
};

use xxhash_rust::const_xxh3::{xxh3_64, xxh3_64_with_seed};

fn hash_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
    let null_hash = xxh3_64(b"");
    let hashes;
    if seed.is_none() {
        hashes = array
            .iter()
            .map(|v| match v {
                Some(v) => xxh3_64(v.to_le_bytes().as_ref()),
                None => null_hash,
            })
            .collect::<Vec<_>>();
    } else {
        hashes = array
            .iter()
            .zip(seed.unwrap().values_iter())
            .map(|(v, s)| match v {
                Some(v) => xxh3_64_with_seed(v.to_le_bytes().as_ref(), *s),
                None => null_hash,
            })
            .collect::<Vec<_>>();
    }
    PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
}

fn hash_binary<O: Offset>(
    array: &BinaryArray<O>,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
    let hashes;
    if seed.is_none() {
        hashes = array.values_iter().map(|v| xxh3_64(v)).collect::<Vec<_>>();
    } else {
        hashes = array
            .values_iter()
            .zip(seed.unwrap().values_iter())
            .map(|(v, s)| xxh3_64_with_seed(v, *s))
            .collect::<Vec<_>>();
    }
    PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
}

fn hash_utf8<O: Offset>(
    array: &Utf8Array<O>,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
    let hashes;
    if seed.is_none() {
        hashes = array
            .values_iter()
            .map(|v| xxh3_64(v.as_bytes()))
            .collect::<Vec<_>>();
    } else {
        hashes = array
            .values_iter()
            .zip(seed.unwrap().values_iter())
            .map(|(v, s)| xxh3_64_with_seed(v.as_bytes(), *s))
            .collect::<Vec<_>>();
    }
    PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
}

macro_rules! with_match_primitive_type {(
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
    use PhysicalType::*;
    Ok(match array.data_type().to_physical_type() {
        // Boolean => hash_boolean(array.as_any().downcast_ref().unwrap()),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed)
        }),
        Binary => hash_binary::<i32>(array.as_any().downcast_ref().unwrap(), seed),
        LargeBinary => hash_binary::<i64>(array.as_any().downcast_ref().unwrap(), seed),
        Utf8 => hash_utf8::<i32>(array.as_any().downcast_ref().unwrap(), seed),
        LargeUtf8 => hash_utf8::<i64>(array.as_any().downcast_ref().unwrap(), seed),
        t => {
            return Err(Error::NotYetImplemented(format!(
                "Hash not implemented for type {:?}",
                t
            )))
        }
    })
}
