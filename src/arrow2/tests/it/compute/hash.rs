use arrow2::array::new_null_array;
use arrow2::compute::hash::*;
use arrow2::datatypes::DataType::*;
use arrow2::datatypes::TimeUnit;

#[test]
fn consistency() {
    let datatypes = vec![
        Null,
        Boolean,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Int8,
        Int16,
        Int32,
        Int64,
        Float32,
        Float64,
        Timestamp(TimeUnit::Second, None),
        Timestamp(TimeUnit::Millisecond, None),
        Timestamp(TimeUnit::Microsecond, None),
        Timestamp(TimeUnit::Nanosecond, None),
        Time64(TimeUnit::Microsecond),
        Time64(TimeUnit::Nanosecond),
        Date32,
        Time32(TimeUnit::Second),
        Time32(TimeUnit::Millisecond),
        Date64,
        Utf8,
        LargeUtf8,
        Binary,
        LargeBinary,
        Duration(TimeUnit::Second),
        Duration(TimeUnit::Millisecond),
        Duration(TimeUnit::Microsecond),
        Duration(TimeUnit::Nanosecond),
    ];

    datatypes.into_iter().for_each(|d1| {
        let array = new_null_array(d1.clone(), 10);
        if can_hash(&d1) {
            assert!(hash(array.as_ref()).is_ok());
        } else {
            assert!(hash(array.as_ref()).is_err());
        }
    });
}
