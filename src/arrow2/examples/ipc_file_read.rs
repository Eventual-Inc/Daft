use std::fs::File;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::ipc::read;
use arrow2::io::print;

/// Simplest way: read all record batches from the file. This can be used e.g. for random access.
#[allow(clippy::type_complexity)]
fn read_chunks(path: &str) -> Result<(Schema, Vec<Chunk<Box<dyn Array>>>)> {
    let mut file = File::open(path)?;

    // read the files' metadata. At this point, we can distribute the read whatever we like.
    let metadata = read::read_file_metadata(&mut file)?;

    let schema = metadata.schema.clone();

    // Simplest way: use the reader, an iterator over batches.
    let reader = read::FileReader::new(file, metadata, None, None);

    let chunks = reader.collect::<Result<Vec<_>>>()?;
    Ok((schema, chunks))
}

/// Random access way: read a single record batch from the file. This can be used e.g. for random access.
fn read_batch(path: &str) -> Result<(Schema, Chunk<Box<dyn Array>>)> {
    let mut file = File::open(path)?;

    // read the files' metadata. At this point, we can distribute the read whatever we like.
    let metadata = read::read_file_metadata(&mut file)?;

    let schema = metadata.schema.clone();

    // advanced way: read the dictionary
    let dictionaries = read::read_file_dictionaries(&mut file, &metadata, &mut Default::default())?;

    // and the chunk
    let chunk_index = 0;

    let chunk = read::read_batch(
        &mut file,
        &dictionaries,
        &metadata,
        None,
        None,
        chunk_index,
        &mut Default::default(),
        &mut Default::default(),
    )?;

    Ok((schema, chunk))
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let (schema, chunks) = read_chunks(file_path)?;
    let names = schema.fields.iter().map(|f| &f.name).collect::<Vec<_>>();
    println!("{}", print::write(&chunks, &names));

    let (schema, chunk) = read_batch(file_path)?;
    let names = schema.fields.iter().map(|f| &f.name).collect::<Vec<_>>();
    println!("{}", print::write(&[chunk], &names));
    Ok(())
}
