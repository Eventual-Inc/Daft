use std::fs::File;
use std::io::{BufReader, Seek};

use arrow2::array::Array;
use arrow2::error::Result;
use arrow2::io::ndjson::read;
use arrow2::io::ndjson::read::FallibleStreamingIterator;

fn read_path(path: &str) -> Result<Vec<Box<dyn Array>>> {
    let batch_size = 1024; // number of rows per array
    let mut reader = BufReader::new(File::open(path)?);

    let data_type = read::infer(&mut reader, None)?;
    reader.rewind()?;

    let mut reader = read::FileReader::new(reader, vec!["".to_string(); batch_size], None);

    let mut arrays = vec![];
    // `next` is IO-bounded
    while let Some(rows) = reader.next()? {
        // `deserialize` is CPU-bounded
        let array = read::deserialize(rows, data_type.clone())?;
        arrays.push(array);
    }

    Ok(arrays)
}

fn main() -> Result<()> {
    // Example of reading a NDJSON file from a path
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let arrays = read_path(file_path)?;
    println!("{arrays:#?}");
    Ok(())
}
