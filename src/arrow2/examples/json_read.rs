/// Example of reading a JSON file.
use std::fs;

use arrow2::array::Array;
use arrow2::error::Result;
use arrow2::io::json::read;

fn read_path(path: &str) -> Result<Box<dyn Array>> {
    // read the file into memory (IO-bounded)
    let data = fs::read(path)?;

    // create a non-owning struct of the data (CPU-bounded)
    let json = read::json_deserializer::parse(&data)?;

    // use it to infer an Arrow schema (CPU-bounded)
    let data_type = read::infer(&json)?;

    // and deserialize it (CPU-bounded)
    read::deserialize(&json, data_type)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let batch = read_path(file_path)?;
    println!("{batch:#?}");
    Ok(())
}
