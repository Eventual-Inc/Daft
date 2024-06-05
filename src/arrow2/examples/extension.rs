use std::io::{Cursor, Seek, Write};

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::ipc::read;
use arrow2::io::ipc::write;

fn main() -> Result<()> {
    // declare an extension.
    let extension_type =
        DataType::Extension("date16".to_string(), Box::new(DataType::UInt16), None);

    // initialize an array with it.
    let array = UInt16Array::from_slice([1, 2]).to(extension_type.clone());

    // from here on, it works as usual
    let buffer = Cursor::new(vec![]);

    // write to IPC
    let result_buffer = write_ipc(buffer, array)?;

    // read it back
    let batch = read_ipc(&result_buffer.into_inner())?;

    // and verify that the datatype is preserved.
    let array = &batch.columns()[0];
    assert_eq!(array.data_type(), &extension_type);

    // see https://arrow.apache.org/docs/format/Columnar.html#extension-types
    // for consuming by other consumers.
    Ok(())
}

fn write_ipc<W: Write + Seek>(writer: W, array: impl Array + 'static) -> Result<W> {
    let schema = vec![Field::new("a", array.data_type().clone(), false)].into();

    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::new(writer, schema, None, options);

    let batch = Chunk::try_new(vec![Box::new(array) as Box<dyn Array>])?;

    writer.start()?;
    writer.write(&batch, None)?;
    writer.finish()?;

    Ok(writer.into_inner())
}

fn read_ipc(buf: &[u8]) -> Result<Chunk<Box<dyn Array>>> {
    let mut cursor = Cursor::new(buf);
    let metadata = read::read_file_metadata(&mut cursor)?;
    let mut reader = read::FileReader::new(cursor, metadata, None, None);
    reader.next().unwrap()
}
