use arrow2::array::*;
use arrow2::error::Error;
use arrow2::io::orc::{format, read};

fn deserialize_column(path: &str, column_name: &str) -> Result<Box<dyn Array>, Error> {
    // open the file
    let mut reader = std::fs::File::open(path).unwrap();

    // read its metadata (IO-bounded)
    let metadata = format::read::read_metadata(&mut reader)?;

    // infer its (Arrow) [`Schema`]
    let schema = read::infer_schema(&metadata.footer)?;

    // find the position of the column in the schema
    let (pos, field) = schema
        .fields
        .iter()
        .enumerate()
        .find(|f| f.1.name == column_name)
        .unwrap();

    // pick a stripe (basically a set of rows)
    let stripe = 0;

    // read the stripe's footer (IO-bounded)
    let footer = format::read::read_stripe_footer(&mut reader, &metadata, stripe, &mut vec![])?;

    // read the column's data from the stripe (IO-bounded)
    let data_type = field.data_type.clone();
    let column = format::read::read_stripe_column(
        &mut reader,
        &metadata,
        0,
        footer,
        // 1 because ORC schemas always start with a struct, which we ignore
        1 + pos as u32,
        vec![],
    )?;

    // finally, deserialize to Arrow (CPU-bounded)
    read::deserialize(data_type, &column)
}

fn main() -> Result<(), Error> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];
    let column = &args[2];

    let array = deserialize_column(file_path, column)?;
    println!("{array:?}");
    Ok(())
}
