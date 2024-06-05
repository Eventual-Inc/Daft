use std::fs::File;
use std::io::Read;

use arrow2::array::Array;
use arrow2::io::ipc::IpcField;
use arrow2::{
    chunk::Chunk,
    datatypes::{DataType, Schema},
    error::Result,
    io::{
        json_integration::read,
        json_integration::ArrowJson,
        parquet::write::{
            transverse, CompressionOptions as ParquetCompression, Encoding, FileWriter,
            RowGroupIterator, Version as ParquetVersion, WriteOptions,
        },
    },
    AHashMap,
};
use clap::Parser;
use flate2::read::GzDecoder;

/// Read gzipped JSON file
pub fn read_gzip_json(
    version: &str,
    file_name: &str,
) -> Result<(Schema, Vec<IpcField>, Vec<Chunk<Box<dyn Array>>>)> {
    let path = format!(
        "../testing/arrow-testing/data/arrow-ipc-stream/integration/{}/{}.json.gz",
        version, file_name
    );
    let file = File::open(path).unwrap();
    let mut gz = GzDecoder::new(&file);
    let mut s = String::new();
    gz.read_to_string(&mut s).unwrap();
    // convert to Arrow JSON
    let arrow_json: ArrowJson = serde_json::from_str(&s)?;

    let schema = serde_json::to_value(arrow_json.schema).unwrap();

    let (schema, ipc_fields) = read::deserialize_schema(&schema)?;

    // read dictionaries
    let mut dictionaries = AHashMap::new();
    if let Some(dicts) = arrow_json.dictionaries {
        for json_dict in dicts {
            // TODO: convert to a concrete Arrow type
            dictionaries.insert(json_dict.id, json_dict);
        }
    }

    let batches = arrow_json
        .batches
        .iter()
        .map(|batch| read::deserialize_chunk(&schema, &ipc_fields, batch, &dictionaries))
        .collect::<Result<Vec<_>>>()?;

    Ok((schema, ipc_fields, batches))
}

#[derive(clap::ArgEnum, Debug, Clone)]
enum Version {
    #[clap(name = "1")]
    V1,
    #[clap(name = "2")]
    V2,
}

impl Into<ParquetVersion> for Version {
    fn into(self) -> ParquetVersion {
        match self {
            Version::V1 => ParquetVersion::V1,
            Version::V2 => ParquetVersion::V2,
        }
    }
}

#[derive(clap::ArgEnum, Debug, Clone)]
enum Compression {
    Zstd,
    Snappy,
    Uncompressed,
}

impl Into<ParquetCompression> for Compression {
    fn into(self) -> ParquetCompression {
        match self {
            Compression::Zstd => ParquetCompression::Zstd(None),
            Compression::Snappy => ParquetCompression::Snappy,
            Compression::Uncompressed => ParquetCompression::Uncompressed,
        }
    }
}

#[derive(clap::ArgEnum, PartialEq, Debug, Clone)]
enum EncodingScheme {
    Plain,
    Delta,
}

#[derive(Debug, Parser)]
struct Args {
    #[clap(short, long, help = "Path to JSON file")]
    json: String,
    #[clap(short('o'), long("output"), help = "Path to write parquet file")]
    write_path: String,
    #[clap(short, long, arg_enum, help = "Parquet version", default_value_t = Version::V2)]
    version: Version,
    #[clap(short, long, help = "commas separated projection")]
    projection: Option<String>,
    #[clap(short, long, arg_enum, help = "encoding scheme for utf8", default_value_t = EncodingScheme::Plain)]
    encoding_utf8: EncodingScheme,
    #[clap(short('i'), long, arg_enum, help = "encoding scheme for int", default_value_t = EncodingScheme::Plain)]
    encoding_int: EncodingScheme,
    #[clap(short, long, arg_enum)]
    compression: Compression,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let projection = args.projection.map(|x| {
        x.split(',')
            .map(|x| x.parse::<usize>().unwrap())
            .collect::<Vec<_>>()
    });

    let (schema, _, batches) = read_gzip_json("1.0.0-littleendian", &args.json)?;

    let schema = if let Some(projection) = &projection {
        let fields = schema
            .fields
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                if projection.contains(&i) {
                    Some(f.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Schema::from(fields)
    } else {
        schema
    };

    let batches = if let Some(projection) = &projection {
        batches
            .iter()
            .map(|batch| {
                let columns = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, f)| {
                        if projection.contains(&i) {
                            Some(f.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                Chunk::try_new(columns).unwrap()
            })
            .collect::<Vec<_>>()
    } else {
        batches
    };

    let options = WriteOptions {
        write_statistics: true,
        compression: args.compression.into(),
        version: args.version.into(),
        data_pagesize_limit: None,
    };

    let encodings = schema
        .fields
        .iter()
        .map(|f| {
            transverse(&f.data_type, |dt| match dt {
                DataType::Dictionary(..) => Encoding::RleDictionary,
                DataType::Int32 => {
                    if args.encoding_int == EncodingScheme::Delta {
                        Encoding::DeltaBinaryPacked
                    } else {
                        Encoding::Plain
                    }
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    if args.encoding_utf8 == EncodingScheme::Delta {
                        Encoding::DeltaLengthByteArray
                    } else {
                        Encoding::Plain
                    }
                }
                _ => Encoding::Plain,
            })
        })
        .collect();

    let row_groups =
        RowGroupIterator::try_new(batches.into_iter().map(Ok), &schema, options, encodings)?;

    let writer = File::create(args.write_path)?;

    let mut writer = FileWriter::try_new(writer, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    let _ = writer.end(None)?;

    Ok(())
}
