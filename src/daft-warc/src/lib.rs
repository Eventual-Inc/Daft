use std::sync::Arc;

use arrow2::{self, array::{BinaryArray, MutablePrimitiveArray, MutableUtf8Array}};
use common_error::DaftResult;
use daft_compression::CompressionCodec;
use daft_core::{prelude::{DataType, Field, Schema, SchemaRef, TimeUnit}, series::Series};
use daft_dsl::ExprRef;
use daft_io::{parse_url, GetResult, IOClient, IOStatsRef, SourceType};
use daft_recordbatch::RecordBatch;
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::{io::{AsyncBufRead, BufReader}, fs::File};
use tokio_util::io::StreamReader;
use warc::{BufferedBody, Record, WarcHeader, WarcReader, Error as WarcError};
use arrow2::array::{Int64Array, Utf8Array};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use arrow2::buffer::Buffer;
use arrow2::array::MutableBinaryArray;

pub struct WarcConvertOptions {
    pub limit: Option<usize>,
    pub include_columns: Option<Vec<String>>,
    pub schema: Option<SchemaRef>,
    pub predicate: Option<ExprRef>,
}

struct WarcChunkIterator<I> {
    warc_reader: I,
    current_size: usize,
    records: Vec<Record<BufferedBody>>,
}

impl<I> Iterator for WarcChunkIterator<I>
where
    I: Iterator<Item = Result<Record<BufferedBody>, WarcError>>,
{
    type Item = Vec<Record<BufferedBody>>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(record) = self.warc_reader.next() {
            match record {
                Ok(record) => {
                    self.current_size += record.content_length() as usize;
                    self.records.push(record);
                    if self.current_size >= 1 * 1024 * 1024 {
                        let batch_records = std::mem::take(&mut self.records);
                        self.current_size = 0;
                        return Some(batch_records);
                    }
                }
                Err(e) => {
                    eprintln!("Error reading WARC record: {}", e);
                }
            }
        }
        if !self.records.is_empty() {
            let batch_records = std::mem::take(&mut self.records);
            self.current_size = 0;
            Some(batch_records)
        } else {
            None
        }
    }
}

impl<I> WarcChunkIterator<I> {
    fn new(warc_reader: I) -> Self {
        Self { warc_reader, current_size: 0, records: Vec::new() }
    }
}

use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;

#[derive(Debug, Clone)]
pub enum WarcType {
    Warcinfo,
    Response,
    Resource,
    Request,
    Metadata,
    Revisit,
    Conversion,
    Continuation,
    FutureType(String),
}

impl WarcType {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "warcinfo" => Some(WarcType::Warcinfo),
            "response" => Some(WarcType::Response),
            "resource" => Some(WarcType::Resource),
            "request" => Some(WarcType::Request),
            "metadata" => Some(WarcType::Metadata),
            "revisit" => Some(WarcType::Revisit),
            "conversion" => Some(WarcType::Conversion),
            "continuation" => Some(WarcType::Continuation),
            _ => Some(WarcType::FutureType(s.to_string())),
        }
    }

    fn to_string(&self) -> String {
        match self {
            WarcType::Warcinfo => "warcinfo".to_string(),
            WarcType::Response => "response".to_string(),
            WarcType::Resource => "resource".to_string(),
            WarcType::Request => "request".to_string(),
            WarcType::Metadata => "metadata".to_string(),
            WarcType::Revisit => "revisit".to_string(),
            WarcType::Conversion => "conversion".to_string(),
            WarcType::Continuation => "continuation".to_string(),
            WarcType::FutureType(s) => s.clone(),
        }
    }
}

struct WarcHeaderState {
    content_length: Option<usize>,
    record_id: Option<Uuid>,
    warc_date: Option<DateTime<Utc>>,
    warc_type: Option<WarcType>,
}

use arrow2::array::MutableArray;

async fn stream_warc_single(uri: &str, io_client: Arc<IOClient>, io_stats: Option<IOStatsRef>, convert_options: WarcConvertOptions) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let (reader, buffer_size, chunk_size): (Box<dyn AsyncBufRead + Unpin + Send>, usize, usize) =
    match io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await?
    {
        GetResult::File(file) => {
            println!("Opening local file: {}", file.path.display());
            let buffer_size = 256 * 1024;
            let file_reader = File::open(file.path).await?;
            println!("File opened successfully");
            (
                Box::new(BufReader::with_capacity(buffer_size, file_reader)),
                buffer_size,
                64,
            )
        }
        GetResult::Stream(stream, ..) => {
            println!("Opening stream");
            (
                Box::new(StreamReader::new(stream)),
                8 * 1024 * 1024,
                64,
            )
        }
    };

    let compression = CompressionCodec::from_uri(uri);

    let mut reader: Box<dyn AsyncBufRead + Unpin + Send> = if let Some(compression) = compression {
        let decoder = compression.to_decoder(reader);
        Box::new(tokio::io::BufReader::with_capacity(buffer_size, decoder))
    } else {
        reader
    };

    let mut line_buf = Vec::with_capacity(4096);
    let mut header_state = WarcHeaderState {
        content_length: None,
        record_id: None,
        warc_date: None,
        warc_type: None,
    };
    
    let mut record_id_array = MutableUtf8Array::<i64>::new();
    let mut warc_type_array = MutableUtf8Array::<i64>::new();
    let mut warc_date_array = MutablePrimitiveArray::<i64>::new();
    let mut warc_content_length_array = MutablePrimitiveArray::<i64>::new();
    let mut content_array = MutableBinaryArray::<i64>::new();

    loop {
        line_buf.clear();
        match reader.read_until(b'\n', &mut line_buf).await {
            Ok(0) => {
                break;
            }
            Ok(2) if line_buf[0] == b'\r' && line_buf[1] == b'\n' && header_state.content_length.is_some() => {
                let len = header_state.content_length.expect("Content length is required");
                if len == 0 {
                    content_array.push_null();
                } else {
                    let mut content_vec = vec![0; len];
                    reader.read_exact(&mut content_vec).await?;
                    content_array.push(Some(content_vec));
                }
                
                // Reset header state.
                // TODO(desmond): we should save this state to the record batch.
                if let Some(record_id) = header_state.record_id {
                    record_id_array.push(Some(record_id.to_string()));
                } else {
                    record_id_array.push(None::<String>);
                }
                if let Some(warc_type) = header_state.warc_type {
                    warc_type_array.push(Some(warc_type.to_string()));
                } else {
                    warc_type_array.push(None::<String>);
                }
                if let Some(warc_date) = header_state.warc_date {
                    warc_date_array.push(warc_date.timestamp_nanos_opt());
                } else {
                    warc_date_array.push(None);
                }
                if let Some(content_length) = header_state.content_length {
                    warc_content_length_array.push(Some(content_length as i64));
                } else {
                    warc_content_length_array.push(None::<i64>);
                }
                header_state.content_length = None;
                header_state.record_id = None;
                header_state.warc_date = None;
                header_state.warc_type = None;
            }
            Ok(_) => {
                // Warc headers are ASCII, so we can use from_utf8_lossy.
                let line = String::from_utf8_lossy(&line_buf);
                if line.starts_with("Content-Length:") {
                    if let Some(len) = line["Content-Length:".len()..]
                        .trim()
                        .parse::<usize>()
                        .ok() 
                    {
                        header_state.content_length = Some(len);
                    }
                } else if line.starts_with("WARC-Record-ID:") {
                    // Parse the WARC Record ID (urn:uuid)
                    let id = line["WARC-Record-ID:".len()..].trim();
                    if id.starts_with('<') && id.ends_with('>') {
                        let uuid_str = &id[10..id.len()-1];  // Skip <urn:uuid: and >
                        if let Ok(uuid) = Uuid::parse_str(uuid_str) {
                            header_state.record_id = Some(uuid);
                        }
                    }
                } else if line.starts_with("WARC-Type:") {
                    // Parse the WARC Type.
                    let type_str = line["WARC-Type:".len()..].trim();
                    header_state.warc_type = WarcType::from_str(type_str);
                    // TODO(desmond): based on the spec, if the warc type is not one of the known types, we should ignore the record.
                } else if line.starts_with("WARC-Date:") {
                    // Parse the WARC Date which is always in UTC.
                    let date_str = line["WARC-Date:".len()..].trim();
                    if let Ok(date) = DateTime::parse_from_rfc3339(date_str) {
                        header_state.warc_date = Some(date.with_timezone(&Utc));
                    }
                } 
            }
            Err(e) => {
                eprintln!("Error reading line: {}", e);
                break;
            }
        }
    }

    let num_records = content_array.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("warc_record_id", DataType::Utf8),
        Field::new("warc_type", DataType::Utf8),
        Field::new("warc_date", DataType::Timestamp(TimeUnit::Nanoseconds, Some("Etc/UTC".to_string()))),
        Field::new("warc_content_length", DataType::Int64),
        Field::new("warc_content", DataType::Binary),
    ])?);
    let record_batch = create_record_batch(
        schema, 
        vec![
            record_id_array.as_box(),
            warc_type_array.as_box(),
            warc_date_array.as_box(),
            warc_content_length_array.as_box(),
            content_array.as_box(),
        ], 
        num_records
    )?;    
    let stream = futures::stream::once(async move { Ok(record_batch) }).boxed();

    Ok(stream)
}

fn create_record_batch(
    schema: SchemaRef,
    arrays: Vec<Box<dyn arrow2::array::Array>>,
    num_records: usize,
) -> DaftResult<RecordBatch> {
    let mut series_vec = Vec::with_capacity(schema.fields.len());
    
    // Create series from the provided arrow arrays
    for ((_, field), array) in schema.fields.iter().zip(arrays.into_iter()) {
        let series = Series::from_arrow(Arc::new(field.clone()), array)?;
        series_vec.push(series);
    }

    println!("Creating record batch with {} records", num_records);

    RecordBatch::new_with_size(
        schema.clone(),
        series_vec,
        num_records,
    )
}

pub async fn stream_warc(uri: String, io_client: Arc<IOClient>, io_stats: IOStatsRef, convert_options: WarcConvertOptions) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let uri = uri.as_str();
    stream_warc_single(uri, io_client, Some(io_stats), convert_options).await
}