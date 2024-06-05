use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::avro::avro_schema::file::{Block, CompressedBlock, Compression};
use arrow2::io::avro::avro_schema::write::{compress, write_block, write_metadata};
use arrow2::io::avro::write;
use arrow2::types::months_days_ns;
use avro_schema::schema::{Field as AvroField, Record, Schema as AvroSchema};

use super::read::read_avro;

pub(super) fn schema() -> Schema {
    Schema::from(vec![
        Field::new("int64", DataType::Int64, false),
        Field::new("int64 nullable", DataType::Int64, true),
        Field::new("utf8", DataType::Utf8, false),
        Field::new("utf8 nullable", DataType::Utf8, true),
        Field::new("int32", DataType::Int32, false),
        Field::new("int32 nullable", DataType::Int32, true),
        Field::new("date", DataType::Date32, false),
        Field::new("date nullable", DataType::Date32, true),
        Field::new("binary", DataType::Binary, false),
        Field::new("binary nullable", DataType::Binary, true),
        Field::new("fs binary", DataType::FixedSizeBinary(3), false),
        Field::new("fs binary nullable", DataType::FixedSizeBinary(3), true),
        Field::new("float32", DataType::Float32, false),
        Field::new("float32 nullable", DataType::Float32, true),
        Field::new("float64", DataType::Float64, false),
        Field::new("float64 nullable", DataType::Float64, true),
        Field::new("boolean", DataType::Boolean, false),
        Field::new("boolean nullable", DataType::Boolean, true),
        Field::new(
            "interval",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
        Field::new(
            "interval nullable",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        ),
        Field::new(
            "list",
            DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
            false,
        ),
        Field::new(
            "list nullable",
            DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
    ])
}

pub(super) fn data() -> Chunk<Box<dyn Array>> {
    let list_dt = DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
    let list_dt1 = DataType::List(Box::new(Field::new("item", DataType::Int32, true)));

    let columns = vec![
        Box::new(Int64Array::from_slice([27, 47])) as Box<dyn Array>,
        Box::new(Int64Array::from([Some(27), None])),
        Box::new(Utf8Array::<i32>::from_slice(["foo", "bar"])),
        Box::new(Utf8Array::<i32>::from([Some("foo"), None])),
        Box::new(Int32Array::from_slice([1, 1])),
        Box::new(Int32Array::from([Some(1), None])),
        Box::new(Int32Array::from_slice([1, 2]).to(DataType::Date32)),
        Box::new(Int32Array::from([Some(1), None]).to(DataType::Date32)),
        Box::new(BinaryArray::<i32>::from_slice([b"foo", b"bar"])),
        Box::new(BinaryArray::<i32>::from([Some(b"foo"), None])),
        Box::new(FixedSizeBinaryArray::from_slice([[1, 2, 3], [1, 2, 3]])),
        Box::new(FixedSizeBinaryArray::from([Some([1, 2, 3]), None])),
        Box::new(PrimitiveArray::<f32>::from_slice([1.0, 2.0])),
        Box::new(PrimitiveArray::<f32>::from([Some(1.0), None])),
        Box::new(PrimitiveArray::<f64>::from_slice([1.0, 2.0])),
        Box::new(PrimitiveArray::<f64>::from([Some(1.0), None])),
        Box::new(BooleanArray::from_slice([true, false])),
        Box::new(BooleanArray::from([Some(true), None])),
        Box::new(PrimitiveArray::<months_days_ns>::from_slice([
            months_days_ns::new(1, 1, 10 * 1_000_000), // 10 millis
            months_days_ns::new(2, 2, 20 * 1_000_000), // 20 millis
        ])),
        Box::new(PrimitiveArray::<months_days_ns>::from([
            Some(months_days_ns::new(1, 1, 10 * 1_000_000)), // 10 millis
            None,
        ])),
        Box::new(ListArray::<i32>::new(
            list_dt,
            vec![0, 2, 5].try_into().unwrap(),
            Box::new(PrimitiveArray::<i32>::from([
                None,
                Some(1),
                None,
                Some(3),
                Some(4),
            ])),
            None,
        )),
        Box::new(ListArray::<i32>::new(
            list_dt1,
            vec![0, 2, 2].try_into().unwrap(),
            Box::new(PrimitiveArray::<i32>::from([None, Some(1)])),
            Some([true, false].into()),
        )),
    ];

    Chunk::new(columns)
}

pub(super) fn serialize_to_block<R: AsRef<dyn Array>>(
    columns: &Chunk<R>,
    schema: &Schema,
    compression: Option<Compression>,
) -> Result<CompressedBlock> {
    let record = write::to_record(schema)?;

    let mut serializers = columns
        .arrays()
        .iter()
        .map(|x| x.as_ref())
        .zip(record.fields.iter())
        .map(|(array, field)| write::new_serializer(array, &field.schema))
        .collect::<Vec<_>>();
    let mut block = Block::new(columns.len(), vec![]);

    write::serialize(&mut serializers, &mut block);

    let mut compressed_block = CompressedBlock::default();

    compress(&mut block, &mut compressed_block, compression)?;

    Ok(compressed_block)
}

fn write_avro<R: AsRef<dyn Array>>(
    columns: &Chunk<R>,
    schema: &Schema,
    compression: Option<Compression>,
) -> Result<Vec<u8>> {
    let compressed_block = serialize_to_block(columns, schema, compression)?;

    let avro_fields = write::to_record(schema)?;
    let mut file = vec![];

    write_metadata(&mut file, avro_fields, compression)?;

    write_block(&mut file, &compressed_block)?;

    Ok(file)
}

fn roundtrip(compression: Option<Compression>) -> Result<()> {
    let expected = data();
    let expected_schema = schema();

    let data = write_avro(&expected, &expected_schema, compression)?;

    let (result, read_schema) = read_avro(&data, None)?;

    assert_eq!(expected_schema, read_schema);
    for (c1, c2) in result.columns().iter().zip(expected.columns().iter()) {
        assert_eq!(c1.as_ref(), c2.as_ref());
    }
    Ok(())
}

#[test]
fn no_compression() -> Result<()> {
    roundtrip(None)
}

#[cfg(feature = "io_avro_compression")]
#[test]
fn snappy() -> Result<()> {
    roundtrip(Some(Compression::Snappy))
}

#[cfg(feature = "io_avro_compression")]
#[test]
fn deflate() -> Result<()> {
    roundtrip(Some(Compression::Deflate))
}

fn large_format_schema() -> Schema {
    Schema::from(vec![
        Field::new("large_utf8", DataType::LargeUtf8, false),
        Field::new("large_utf8_nullable", DataType::LargeUtf8, true),
        Field::new("large_binary", DataType::LargeBinary, false),
        Field::new("large_binary_nullable", DataType::LargeBinary, true),
    ])
}

fn large_format_data() -> Chunk<Box<dyn Array>> {
    let columns = vec![
        Box::new(Utf8Array::<i64>::from_slice(["a", "b"])) as Box<dyn Array>,
        Box::new(Utf8Array::<i64>::from([Some("a"), None])),
        Box::new(BinaryArray::<i64>::from_slice([b"foo", b"bar"])),
        Box::new(BinaryArray::<i64>::from([Some(b"foo"), None])),
    ];
    Chunk::new(columns)
}

fn large_format_expected_schema() -> Schema {
    Schema::from(vec![
        Field::new("large_utf8", DataType::Utf8, false),
        Field::new("large_utf8_nullable", DataType::Utf8, true),
        Field::new("large_binary", DataType::Binary, false),
        Field::new("large_binary_nullable", DataType::Binary, true),
    ])
}

fn large_format_expected_data() -> Chunk<Box<dyn Array>> {
    let columns = vec![
        Box::new(Utf8Array::<i32>::from_slice(["a", "b"])) as Box<dyn Array>,
        Box::new(Utf8Array::<i32>::from([Some("a"), None])),
        Box::new(BinaryArray::<i32>::from_slice([b"foo", b"bar"])),
        Box::new(BinaryArray::<i32>::from([Some(b"foo"), None])),
    ];
    Chunk::new(columns)
}

#[test]
fn check_large_format() -> Result<()> {
    let write_schema = large_format_schema();
    let write_data = large_format_data();

    let data = write_avro(&write_data, &write_schema, None)?;
    let (result, read_schame) = read_avro(&data, None)?;

    let expected_schema = large_format_expected_schema();
    assert_eq!(read_schame, expected_schema);

    let expected_data = large_format_expected_data();
    for (c1, c2) in result.columns().iter().zip(expected_data.columns().iter()) {
        assert_eq!(c1.as_ref(), c2.as_ref());
    }

    Ok(())
}

fn struct_schema() -> Schema {
    Schema::from(vec![
        Field::new(
            "struct",
            DataType::Struct(vec![
                Field::new("item1", DataType::Int32, false),
                Field::new("item2", DataType::Int32, true),
            ]),
            false,
        ),
        Field::new(
            "struct nullable",
            DataType::Struct(vec![
                Field::new("item1", DataType::Int32, false),
                Field::new("item2", DataType::Int32, true),
            ]),
            true,
        ),
    ])
}

fn struct_data() -> Chunk<Box<dyn Array>> {
    let struct_dt = DataType::Struct(vec![
        Field::new("item1", DataType::Int32, false),
        Field::new("item2", DataType::Int32, true),
    ]);

    Chunk::new(vec![
        Box::new(StructArray::new(
            struct_dt.clone(),
            vec![
                Box::new(PrimitiveArray::<i32>::from_slice([1, 2])),
                Box::new(PrimitiveArray::<i32>::from([None, Some(1)])),
            ],
            None,
        )),
        Box::new(StructArray::new(
            struct_dt,
            vec![
                Box::new(PrimitiveArray::<i32>::from_slice([1, 2])),
                Box::new(PrimitiveArray::<i32>::from([None, Some(1)])),
            ],
            Some([true, false].into()),
        )),
    ])
}

fn avro_record() -> Record {
    Record {
        name: "".to_string(),
        namespace: None,
        doc: None,
        aliases: vec![],
        fields: vec![
            AvroField {
                name: "struct".to_string(),
                doc: None,
                schema: AvroSchema::Record(Record {
                    name: "r1".to_string(),
                    namespace: None,
                    doc: None,
                    aliases: vec![],
                    fields: vec![
                        AvroField {
                            name: "item1".to_string(),
                            doc: None,
                            schema: AvroSchema::Int(None),
                            default: None,
                            order: None,
                            aliases: vec![],
                        },
                        AvroField {
                            name: "item2".to_string(),
                            doc: None,
                            schema: AvroSchema::Union(vec![
                                AvroSchema::Null,
                                AvroSchema::Int(None),
                            ]),
                            default: None,
                            order: None,
                            aliases: vec![],
                        },
                    ],
                }),
                default: None,
                order: None,
                aliases: vec![],
            },
            AvroField {
                name: "struct nullable".to_string(),
                doc: None,
                schema: AvroSchema::Union(vec![
                    AvroSchema::Null,
                    AvroSchema::Record(Record {
                        name: "r2".to_string(),
                        namespace: None,
                        doc: None,
                        aliases: vec![],
                        fields: vec![
                            AvroField {
                                name: "item1".to_string(),
                                doc: None,
                                schema: AvroSchema::Int(None),
                                default: None,
                                order: None,
                                aliases: vec![],
                            },
                            AvroField {
                                name: "item2".to_string(),
                                doc: None,
                                schema: AvroSchema::Union(vec![
                                    AvroSchema::Null,
                                    AvroSchema::Int(None),
                                ]),
                                default: None,
                                order: None,
                                aliases: vec![],
                            },
                        ],
                    }),
                ]),
                default: None,
                order: None,
                aliases: vec![],
            },
        ],
    }
}

#[test]
fn avro_record_schema() -> Result<()> {
    let arrow_schema = struct_schema();
    let record = write::to_record(&arrow_schema)?;
    assert_eq!(record, avro_record());
    Ok(())
}

#[test]
fn struct_() -> Result<()> {
    let write_schema = struct_schema();
    let write_data = struct_data();

    let data = write_avro(&write_data, &write_schema, None)?;
    let (result, read_schema) = read_avro(&data, None)?;

    let expected_schema = struct_schema();
    assert_eq!(read_schema, expected_schema);

    let expected_data = struct_data();
    for (c1, c2) in result.columns().iter().zip(expected_data.columns().iter()) {
        assert_eq!(c1.as_ref(), c2.as_ref());
    }

    Ok(())
}
