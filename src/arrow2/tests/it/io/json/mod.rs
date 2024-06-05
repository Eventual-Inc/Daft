mod read;
mod write;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::json::write as json_write;

fn write_batch(array: Box<dyn Array>) -> Result<Vec<u8>> {
    let mut serializer = json_write::Serializer::new(vec![Ok(array)].into_iter(), vec![]);

    let mut buf = vec![];
    json_write::write(&mut buf, &mut serializer)?;
    Ok(buf)
}

fn write_record_batch<A: AsRef<dyn Array>>(schema: Schema, chunk: Chunk<A>) -> Result<Vec<u8>> {
    let mut serializer = json_write::RecordSerializer::new(schema, &chunk, vec![]);

    let mut buf = vec![];
    json_write::write(&mut buf, &mut serializer)?;
    Ok(buf)
}
