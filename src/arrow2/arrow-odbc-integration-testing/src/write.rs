use stdext::function_name;

use arrow2::array::{Array, BinaryArray, BooleanArray, Int32Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field};
use arrow2::error::Result;
use arrow2::io::odbc::write::{buffer_from_description, infer_descriptions, serialize};

use super::read::read;
use super::{setup_empty_table, ENV, MSSQL};

fn test(
    expected: Chunk<Box<dyn Array>>,
    fields: Vec<Field>,
    type_: &str,
    table_name: &str,
) -> Result<()> {
    let connection = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&connection, table_name, &[type_]).unwrap();

    let query = &format!("INSERT INTO {table_name} (a) VALUES (?)");
    let mut a = connection.prepare(query).unwrap();

    let mut buffer = buffer_from_description(infer_descriptions(&fields)?, expected.len());

    // write
    buffer.set_num_rows(expected.len());
    let array = &expected.columns()[0];

    serialize(array.as_ref(), &mut buffer.column_mut(0))?;

    a.execute(&buffer).unwrap();

    // read
    let query = format!("SELECT a FROM {table_name} ORDER BY id");
    let chunks = read(&connection, &query)?.1;

    assert_eq!(chunks[0], expected);
    Ok(())
}

#[test]
fn int() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let table_name = format!("write_{}", table_name);
    let expected = Chunk::new(vec![Box::new(Int32Array::from_slice([1])) as _]);

    test(
        expected,
        vec![Field::new("a", DataType::Int32, false)],
        "INT",
        &table_name,
    )
}

#[test]
fn int_nullable() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let table_name = format!("write_{}", table_name);
    let expected = Chunk::new(vec![Box::new(Int32Array::from([Some(1), None])) as _]);

    test(
        expected,
        vec![Field::new("a", DataType::Int32, true)],
        "INT",
        &table_name,
    )
}

#[test]
fn bool() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let table_name = format!("write_{}", table_name);
    let expected = Chunk::new(vec![Box::new(BooleanArray::from_slice([true, false])) as _]);

    test(
        expected,
        vec![Field::new("a", DataType::Boolean, false)],
        "BIT",
        &table_name,
    )
}

#[test]
fn bool_nullable() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let table_name = format!("write_{}", table_name);
    let expected = Chunk::new(vec![
        Box::new(BooleanArray::from([Some(true), Some(false), None])) as _,
    ]);

    test(
        expected,
        vec![Field::new("a", DataType::Boolean, true)],
        "BIT",
        &table_name,
    )
}

#[test]
fn utf8() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let table_name = format!("write_{}", table_name);
    let expected =
        Chunk::new(vec![
            Box::new(Utf8Array::<i32>::from([Some("aa"), None, Some("aaaa")])) as _,
        ]);

    test(
        expected,
        vec![Field::new("a", DataType::Utf8, true)],
        "VARCHAR(4)",
        &table_name,
    )
}

#[test]
fn binary() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let table_name = format!("write_{}", table_name);
    let expected = Chunk::new(vec![Box::new(BinaryArray::<i32>::from([
        Some(&b"aa"[..]),
        None,
        Some(&b"aaaa"[..]),
    ])) as _]);

    test(
        expected,
        vec![Field::new("a", DataType::Binary, true)],
        "VARBINARY(4)",
        &table_name,
    )
}
