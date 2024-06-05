use std::fs::File;

use arrow2::array::{Array, Int32Array};
use arrow2::error::Result;
use arrow2::io::ndjson::write;

fn write_path(path: &str, array: Box<dyn Array>) -> Result<()> {
    let writer = File::create(path)?;

    let serializer = write::Serializer::new(vec![Ok(array)].into_iter(), vec![]);

    let mut writer = write::FileWriter::new(writer, serializer);
    writer.by_ref().collect::<Result<()>>()
}

fn main() -> Result<()> {
    // Example of reading a NDJSON file from a path
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let array = Box::new(Int32Array::from(&[
        Some(0),
        None,
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ]));

    write_path(file_path, array)?;
    Ok(())
}
