//! Demo of how to write to, and read from, an ODBC connector
//!
//! On an Ubuntu, you need to run the following (to install the driver):
//! ```bash
//! sudo apt install libsqliteodbc sqlite3 unixodbc-dev
//! sudo sed --in-place 's/libsqlite3odbc.so/\/usr\/lib\/x86_64-linux-gnu\/odbc\/libsqlite3odbc.so/' /etc/odbcinst.ini
//! ```
use arrow2::array::{Array, Int32Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field};
use arrow2::error::Result;
use arrow2::io::odbc::api;
use arrow2::io::odbc::api::Cursor;
use arrow2::io::odbc::read;
use arrow2::io::odbc::write;

fn main() -> Result<()> {
    let connector = "Driver={SQLite3};Database=sqlite-test.db";
    let env = api::Environment::new()?;
    let connection = env.connect_with_connection_string(connector)?;

    // let's create an empty table with a schema
    connection.execute("DROP TABLE IF EXISTS example;", ())?;
    connection.execute("CREATE TABLE example (c1 INT, c2 TEXT);", ())?;

    // and now let's write some data into it (from arrow arrays!)
    // first, we prepare the statement
    let query = "INSERT INTO example (c1, c2) VALUES (?, ?)";
    let prepared = connection.prepare(query).unwrap();

    // secondly, we initialize buffers from odbc-api
    let fields = vec![
        // (for now) the types here must match the tables' schema
        Field::new("unused", DataType::Int32, true),
        Field::new("unused", DataType::LargeUtf8, true),
    ];

    // third, we initialize the writer
    let mut writer = write::Writer::try_new(prepared, fields)?;

    // say we have (or receive from a channel) a chunk:
    let chunk = Chunk::new(vec![
        Box::new(Int32Array::from_slice([1, 2, 3])) as Box<dyn Array>,
        Box::new(Utf8Array::<i64>::from([Some("Hello"), None, Some("World")])),
    ]);

    // we write it like this
    writer.write(&chunk)?;

    // and we can later read from it
    let chunks = read(&connection, "SELECT c1 FROM example")?;

    // and the result should be the same
    assert_eq!(chunks[0].columns()[0], chunk.columns()[0]);

    Ok(())
}

/// Reads chunks from a query done against an ODBC connection
pub fn read(connection: &api::Connection<'_>, query: &str) -> Result<Vec<Chunk<Box<dyn Array>>>> {
    let mut a = connection.prepare(query)?;
    let fields = read::infer_schema(&a)?;

    let max_batch_size = 100;
    let buffer = read::buffer_from_metadata(&a, max_batch_size)?;

    let cursor = a.execute(())?.unwrap();
    let mut cursor = cursor.bind_buffer(buffer)?;

    let mut chunks = vec![];
    while let Some(batch) = cursor.fetch()? {
        let arrays = (0..batch.num_cols())
            .zip(fields.iter())
            .map(|(index, field)| {
                let column_view = batch.column(index);
                read::deserialize(column_view, field.data_type.clone())
            })
            .collect::<Vec<_>>();
        chunks.push(Chunk::new(arrays));
    }

    Ok(chunks)
}
