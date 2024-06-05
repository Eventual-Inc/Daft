use std::fs::File;

use arrow2::{
    array::{Array, Int32Array},
    error::Error,
    io::json::write,
};

fn write_array(path: &str, array: Box<dyn Array>) -> Result<(), Error> {
    let mut writer = File::create(path)?;

    let arrays = vec![Ok(array)].into_iter();

    // Advancing this iterator serializes the next array to its internal buffer (i.e. CPU-bounded)
    let blocks = write::Serializer::new(arrays, vec![]);

    // the operation of writing is IO-bounded.
    write::write(&mut writer, blocks)?;

    Ok(())
}

fn main() -> Result<(), Error> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let array = Int32Array::from(&[Some(0), None, Some(2), Some(3), Some(4), Some(5), Some(6)]);

    write_array(file_path, Box::new(array))
}
