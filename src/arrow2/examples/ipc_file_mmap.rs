//! Example showing how to memory map an Arrow IPC file into a [`Chunk`].
use std::sync::Arc;

use arrow2::array::{Array, BooleanArray};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Field, Schema};
use arrow2::error::Error;

// Arrow2 requires something that implements `AsRef<[u8]>`, which
// `Mmap` supports. Here we mock it
struct Mmap(Vec<u8>);

impl AsRef<[u8]> for Mmap {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

// Auxiliary function to write an arrow file
// This function is guaranteed to produce a valid arrow file
fn write(
    chunks: &[Chunk<Box<dyn Array>>],
    schema: &Schema,
    ipc_fields: Option<Vec<arrow2::io::ipc::IpcField>>,
    compression: Option<arrow2::io::ipc::write::Compression>,
) -> Result<Vec<u8>, Error> {
    let result = vec![];
    let options = arrow2::io::ipc::write::WriteOptions { compression };
    let mut writer = arrow2::io::ipc::write::FileWriter::try_new(
        result,
        schema.clone(),
        ipc_fields.clone(),
        options,
    )?;
    for chunk in chunks {
        writer.write(chunk, ipc_fields.as_ref().map(|x| x.as_ref()))?;
    }
    writer.finish()?;
    Ok(writer.into_inner())
}

fn check_round_trip(array: Box<dyn Array>) -> Result<(), Error> {
    let schema = Schema::from(vec![Field::new("a", array.data_type().clone(), true)]);
    let columns = Chunk::try_new(vec![array.clone()])?;

    // given a mmap
    let data = Arc::new(write(&[columns], &schema, None, None)?);

    // we first read the files' metadata
    let metadata =
        arrow2::io::ipc::read::read_file_metadata(&mut std::io::Cursor::new(data.as_ref()))?;

    // next we mmap the dictionaries
    // Safety: `write` above guarantees that this is a valid Arrow IPC file
    let dictionaries =
        unsafe { arrow2::mmap::mmap_dictionaries_unchecked(&metadata, data.clone())? };

    // and finally mmap a chunk (0 in this case).
    // Safety: `write` above guarantees that this is a valid Arrow IPC file
    let new_array = unsafe { arrow2::mmap::mmap_unchecked(&metadata, &dictionaries, data, 0)? };
    assert_eq!(new_array.into_arrays()[0], array);
    Ok(())
}

fn main() -> Result<(), Error> {
    let array = BooleanArray::from([None, None, Some(true)]).boxed();
    check_round_trip(array)
}
