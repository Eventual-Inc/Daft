use std::fs::File;

use arrow2::io::json_integration::ArrowJson;
use clap::Parser;

use arrow2::io::ipc::read;
use arrow2::io::ipc::write;
use arrow2::{
    error::{Error, Result},
    io::json_integration::write as json_write,
};
use arrow_integration_testing::read_json_file;

#[derive(Debug, Clone, clap::ArgEnum)]
#[clap(rename_all = "SCREAMING_SNAKE_CASE")]
enum Mode {
    ArrowToJson,
    JsonToArrow,
    Validate,
}

#[derive(Debug, Parser)]
struct Args {
    #[clap(short, long, help = "integration flag")]
    integration: bool,
    #[clap(short, long, help = "Path to arrow file")]
    arrow: String,
    #[clap(short, long, help = "Path to json file")]
    json: String,
    #[clap(arg_enum, short, long)]
    mode: Mode,
    #[clap(short, long, help = "enable/disable verbose mode")]
    verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let arrow_file = args.arrow;
    let json_file = args.json;
    let mode = args.mode;
    let verbose = args.verbose;

    match mode {
        Mode::JsonToArrow => json_to_arrow(&json_file, &arrow_file, verbose),
        Mode::ArrowToJson => arrow_to_json(&arrow_file, &json_file, verbose),
        Mode::Validate => validate(&arrow_file, &json_file, verbose),
    }
}

fn json_to_arrow(json_name: &str, arrow_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Converting {} to {}", json_name, arrow_name);
    }

    let json_file = read_json_file(json_name)?;

    let arrow_file = File::create(arrow_name)?;
    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::try_new(
        arrow_file,
        json_file.schema.clone(),
        Some(json_file.fields),
        options,
    )?;

    for b in json_file.chunks {
        writer.write(&b, None)?;
    }

    writer.finish()?;

    Ok(())
}

fn arrow_to_json(arrow_name: &str, json_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Converting {} to {}", arrow_name, json_name);
    }

    let mut arrow_file = File::open(arrow_name)?;
    let metadata = read::read_file_metadata(&mut arrow_file)?;
    let reader = read::FileReader::new(arrow_file, metadata.clone(), None, None);

    let names = metadata
        .schema
        .fields
        .iter()
        .map(|f| &f.name)
        .collect::<Vec<_>>();

    let schema = json_write::serialize_schema(&metadata.schema, &metadata.ipc_schema.fields);

    let batches = reader
        .map(|batch| Ok(json_write::serialize_chunk(&batch?, &names)))
        .collect::<Result<Vec<_>>>()?;

    let arrow_json = ArrowJson {
        schema,
        batches,
        dictionaries: None,
    };

    let json_file = File::create(json_name)?;
    serde_json::to_writer(&json_file, &arrow_json).unwrap();

    Ok(())
}

fn validate(arrow_name: &str, json_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Validating {} and {}", arrow_name, json_name);
    }

    // open JSON file
    let json_file = read_json_file(json_name)?;

    // open Arrow file
    let mut arrow_file = File::open(arrow_name)?;
    let metadata = read::read_file_metadata(&mut arrow_file)?;
    let reader = read::FileReader::new(arrow_file, metadata, None, None);
    let arrow_schema = reader.schema();

    // compare schemas
    if &json_file.schema != arrow_schema {
        return Err(Error::InvalidArgumentError(format!(
            "Schemas do not match. JSON: {:?}. Arrow: {:?}",
            json_file.schema, arrow_schema
        )));
    }

    let json_batches = json_file.chunks;

    if verbose {
        eprintln!(
            "Schemas match. JSON file has {} batches.",
            json_batches.len()
        );
    }

    let batches = reader.collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(json_batches, batches);
    Ok(())
}
