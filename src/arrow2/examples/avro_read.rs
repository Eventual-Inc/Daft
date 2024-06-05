use std::fs::File;
use std::io::BufReader;

use arrow2::error::Result;
use arrow2::io::avro::avro_schema;
use arrow2::io::avro::read;

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let path = &args[1];

    let file = &mut BufReader::new(File::open(path)?);

    let metadata = avro_schema::read::read_metadata(file)?;

    let schema = read::infer_schema(&metadata.record)?;

    println!("{metadata:#?}");

    let reader = read::Reader::new(file, metadata, schema.fields, None);

    for maybe_chunk in reader {
        let columns = maybe_chunk?;
        assert!(!columns.is_empty());
    }
    Ok(())
}
