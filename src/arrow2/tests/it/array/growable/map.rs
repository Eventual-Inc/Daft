use arrow2::{
    array::{
        growable::{Growable, GrowableMap},
        Array, MapArray, PrimitiveArray, StructArray, Utf8Array,
    },
    bitmap::Bitmap,
    datatypes::{DataType, Field},
    offset::OffsetsBuffer,
};

fn some_values() -> (DataType, Vec<Box<dyn Array>>) {
    let strings: Box<dyn Array> = Box::new(Utf8Array::<i32>::from([
        Some("a"),
        Some("aa"),
        Some("bc"),
        Some("mark"),
        Some("doe"),
        Some("xyz"),
    ]));
    let ints: Box<dyn Array> = Box::new(PrimitiveArray::<i32>::from(&[
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ]));
    let fields = vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("val", DataType::Int32, true),
    ];
    (DataType::Struct(fields), vec![strings, ints])
}

#[test]
fn basic() {
    let (fields, values) = some_values();

    let kv_array = StructArray::new(fields.clone(), values, None).boxed();
    let kv_field = Field::new("kv", fields, false);
    let data_type = DataType::Map(Box::new(kv_field), false);
    let offsets = OffsetsBuffer::try_from(vec![0, 1, 2, 4, 6]).unwrap();

    let array = MapArray::new(data_type.clone(), offsets, kv_array.clone(), None);

    let mut a = GrowableMap::new(vec![&array], false, 0);

    a.extend(0, 1, 2);
    assert_eq!(a.len(), 2);
    let result: MapArray = a.into();

    let kv_array = kv_array.sliced(1, 4);
    let offsets = OffsetsBuffer::try_from(vec![0, 1, 3]).unwrap();
    let expected = MapArray::new(data_type, offsets, kv_array, None);

    assert_eq!(result, expected)
}

#[test]
fn offset() {
    let (fields, values) = some_values();

    let kv_array = StructArray::new(fields.clone(), values, None).boxed();
    let kv_field = Field::new("kv", fields, false);
    let data_type = DataType::Map(Box::new(kv_field), false);
    let offsets = OffsetsBuffer::try_from(vec![0, 1, 2, 4, 6]).unwrap();

    let array = MapArray::new(data_type.clone(), offsets, kv_array.clone(), None).sliced(1, 3);

    let mut a = GrowableMap::new(vec![&array], false, 0);

    a.extend(0, 1, 2);
    assert_eq!(a.len(), 2);
    let result: MapArray = a.into();

    let kv_array = kv_array.sliced(2, 4);
    let offsets = OffsetsBuffer::try_from(vec![0, 2, 4]).unwrap();
    let expected = MapArray::new(data_type, offsets, kv_array, None);

    assert_eq!(result, expected)
}

#[test]
fn nulls() {
    let (fields, values) = some_values();

    let kv_array = StructArray::new(fields.clone(), values, None).boxed();
    let kv_field = Field::new("kv", fields, false);
    let data_type = DataType::Map(Box::new(kv_field), false);
    let offsets = OffsetsBuffer::try_from(vec![0, 1, 2, 4, 6]).unwrap();

    let array = MapArray::new(
        data_type.clone(),
        offsets,
        kv_array.clone(),
        Some(Bitmap::from_u8_slice([0b00000010], 4)),
    );

    let mut a = GrowableMap::new(vec![&array], false, 0);

    a.extend(0, 1, 2);
    assert_eq!(a.len(), 2);
    let result: MapArray = a.into();

    let kv_array = kv_array.sliced(1, 4);
    let offsets = OffsetsBuffer::try_from(vec![0, 1, 3]).unwrap();
    let expected = MapArray::new(
        data_type,
        offsets,
        kv_array,
        Some(Bitmap::from_u8_slice([0b00000010], 4).sliced(1, 2)),
    );

    assert_eq!(result, expected)
}
