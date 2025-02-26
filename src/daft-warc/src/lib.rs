use std::sync::Arc;

use arrow2::{self, array::BinaryArray};
use common_error::DaftResult;
use daft_compression::CompressionCodec;
use daft_core::{prelude::{Field, Schema, SchemaRef, TimeUnit}, series::Series};
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
            _ => None,
        }
    }
}

struct WarcHeaderState {
    content_length: Option<usize>,
    record_id: Option<Uuid>,
    warc_date: Option<DateTime<Utc>>,
    warc_type: Option<WarcType>,
}

async fn stream_warc_local(uri: &str, io_client: Arc<IOClient>, io_stats: Option<IOStatsRef>, convert_options: WarcConvertOptions) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
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
    
    let mut content_buffer = arrow2::array::new_null_array(arrow2::datatypes::DataType::Binary, 4096);
    let mut buf = vec![];
    let content = reader.read_to_end(&mut buf).await?;
    println!("Content: {:?}", buf);

    loop {
        line_buf.clear();
        match reader.read_until(b'\n', &mut line_buf).await {
            Ok(0) => {
                println!("EOF");

                if let Some(len) = header_state.content_length {
                    println!("Read content chunk of size: {}", len);
                }
                if let Some(id) = &header_state.record_id {
                    println!("Record ID: {}", id);
                }
                if let Some(date) = &header_state.warc_date {
                    println!("WARC Date: {}", date);
                }
                if let Some(wtype) = &header_state.warc_type {
                    println!("WARC Type: {:?}", wtype);
                }
                break;
            }
            Ok(2) if line_buf[0] == b'\r' && line_buf[1] == b'\n' => {
                // Found end of headers, now read the content
                // let len = header_state.content_length.unwrap_or(0);
                // let slice = buffer.as_slice_mut();
                // // Read directly into the buffer
                // reader.read_exact(slice).await?;
                // // Mark the buffer as initialized since we've filled it
                // unsafe { buffer.set_len(len); }
                
                // Reset header state
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
                } else if line.starts_with("WARC-Date:") {
                    // Parse the WARC Date which is always in UTC.
                    let date_str = line["WARC-Date:".len()..].trim();
                    if let Ok(date) = DateTime::parse_from_rfc3339(date_str) {
                        header_state.warc_date = Some(date.with_timezone(&Utc));
                    }
                } else if line.starts_with("WARC-Type:") {
                    // Parse the WARC Type.
                    let type_str = line["WARC-Type:".len()..].trim();
                    header_state.warc_type = WarcType::from_str(type_str);
                    // TODO(desmond): based on the spec, if the warc type is not one of the known types, we should ignore the record.
                }
            }
            Err(e) => {
                eprintln!("Error reading line: {}", e);
                break;
            }
        }
    }

    // let read_stream = read_into_line_chunk_stream(reader, convert_options.limit, chunk_size);

    todo!()
}

fn create_record_batch(
    records: Vec<Record<BufferedBody>>,
    schema: SchemaRef,
    projection_indices: Option<&Vec<usize>>,
) -> DaftResult<RecordBatch> {
    let num_records = records.len();
    
    // Only allocate vectors for fields we need
    let mut warc_types = if projection_indices.map_or(true, |indices| indices.contains(&1)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_ids = if projection_indices.map_or(true, |indices| indices.contains(&0)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_versions = if projection_indices.map_or(true, |indices| indices.contains(&2)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_dates = if projection_indices.map_or(true, |indices| indices.contains(&3)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_target_uris = if projection_indices.map_or(true, |indices| indices.contains(&4)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_content_types = if projection_indices.map_or(true, |indices| indices.contains(&5)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_content_lengths = if projection_indices.map_or(true, |indices| indices.contains(&6)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_bodies = if projection_indices.map_or(true, |indices| indices.contains(&7)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };

    for record in records {
        if let Some(types) = warc_types.as_mut() {
            types.push(record.warc_type().to_string());
        }
        if let Some(ids) = warc_ids.as_mut() {
            ids.push(record.warc_id().to_string());
        }
        if let Some(versions) = warc_versions.as_mut() {
            versions.push(record.warc_version().to_string());
        }
        if let Some(dates) = warc_dates.as_mut() {
            dates.push(record.date().timestamp() as i64);
        }
        if let Some(target_uris) = warc_target_uris.as_mut() {
            target_uris.push(record.header(WarcHeader::TargetURI).unwrap_or_default().to_string());
        }
        if let Some(content_types) = warc_content_types.as_mut() {
            content_types.push(record.header(WarcHeader::ContentType).unwrap_or_default().to_string());
        }
        if let Some(content_lengths) = warc_content_lengths.as_mut() {
            content_lengths.push(record.content_length() as i64);
        }
        if let Some(bodies) = warc_bodies.as_mut() {
            bodies.push(record.body().to_vec());
        }
    }

    // Create series and return RecordBatch
    let mut series_vec = Vec::with_capacity(schema.fields.len());
    for (idx, (_, field)) in schema.fields.iter().enumerate() {
        let orig_idx = projection_indices.map_or(idx, |indices| indices[idx]);
        
        let series = match orig_idx {
            0 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Utf8Array::<i64>::from_slice(warc_ids.as_ref().unwrap().as_slice())))?,
            1 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Utf8Array::<i64>::from_slice(warc_types.as_ref().unwrap().as_slice())))?,
            2 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Utf8Array::<i64>::from_slice(warc_versions.as_ref().unwrap().as_slice())))?,
            3 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Int64Array::from_slice(warc_dates.as_ref().unwrap().as_slice())))?,
            4 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Utf8Array::<i64>::from_slice(warc_target_uris.as_ref().unwrap().as_slice())))?,
            5 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Utf8Array::<i64>::from_slice(warc_content_types.as_ref().unwrap().as_slice())))?,
            6 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Int64Array::from_slice(warc_content_lengths.as_ref().unwrap().as_slice())))?,
            7 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(BinaryArray::<i64>::from_slice(warc_bodies.as_ref().unwrap().as_slice())))?,
            _ => unreachable!(),
        };
        series_vec.push(series);
    }

    RecordBatch::new_with_size(
        schema.clone(),
        series_vec,
        num_records,
    )
}

pub async fn stream_warc(uri: String, io_client: Arc<IOClient>, io_stats: IOStatsRef, convert_options: WarcConvertOptions) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let uri = uri.as_str();
    let (source_type, _) = parse_url(uri)?;
    if matches!(source_type, SourceType::File) {
        stream_warc_local(uri, io_client, Some(io_stats), convert_options).await
    } else {
        todo!()
    }
}
