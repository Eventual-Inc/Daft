#![feature(let_chains)]
use std::{num::NonZeroUsize, sync::Arc};

use arrow2::array::{MutableArray, MutableBinaryArray, MutablePrimitiveArray, MutableUtf8Array};
use chrono::{DateTime, Utc};
use common_error::{DaftError, DaftResult};
use common_runtime::{get_compute_runtime, get_io_runtime};
use daft_compression::CompressionCodec;
use daft_core::{prelude::SchemaRef, series::Series};
use daft_dsl::{expr::bound_expr::BoundExpr, ExprRef};
use daft_io::{CountingReader, GetResult, IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use snafu::{futures::try_future::TryFutureExt, Snafu};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, BufReader},
};
use tokio_util::io::StreamReader;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error joining spawned task: {}", source))]
    JoinError { source: tokio::task::JoinError },
}

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        match err {
            Error::JoinError { source } => Self::External(Box::new(source)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WarcConvertOptions {
    pub limit: Option<usize>,
    pub include_columns: Option<Vec<String>>,
    pub schema: SchemaRef,
    pub predicate: Option<ExprRef>,
}

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
            "warcinfo" => Some(Self::Warcinfo),
            "response" => Some(Self::Response),
            "resource" => Some(Self::Resource),
            "request" => Some(Self::Request),
            "metadata" => Some(Self::Metadata),
            "revisit" => Some(Self::Revisit),
            "conversion" => Some(Self::Conversion),
            "continuation" => Some(Self::Continuation),
            _ => Some(Self::FutureType(s.to_string())),
        }
    }
}

impl std::fmt::Display for WarcType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Warcinfo => write!(f, "warcinfo"),
            Self::Response => write!(f, "response"),
            Self::Resource => write!(f, "resource"),
            Self::Request => write!(f, "request"),
            Self::Metadata => write!(f, "metadata"),
            Self::Revisit => write!(f, "revisit"),
            Self::Conversion => write!(f, "conversion"),
            Self::Continuation => write!(f, "continuation"),
            Self::FutureType(s) => write!(f, "{}", s),
        }
    }
}

struct WarcHeaderState {
    content_length: Option<usize>,
    record_id: Option<Uuid>,
    warc_date: Option<DateTime<Utc>>,
    warc_type: Option<WarcType>,
    warc_identified_payload_type: Option<String>,
    header_lines: Vec<(String, String)>,
}

impl WarcHeaderState {
    fn reset(&mut self) {
        self.content_length = None;
        self.record_id = None;
        self.warc_date = None;
        self.warc_type = None;
        self.warc_identified_payload_type = None;
        self.header_lines.clear();
    }
}

struct WarcRecordBatchBuilder {
    chunk_size: usize,
    schema: SchemaRef,
    record_id_array: MutableUtf8Array<i64>,
    warc_type_array: MutableUtf8Array<i64>,
    warc_date_array: MutablePrimitiveArray<i64>,
    warc_content_length_array: MutablePrimitiveArray<i64>,
    warc_identified_payload_type_array: MutableUtf8Array<i64>,
    content_array: MutableBinaryArray<i64>,
    header_array: MutableUtf8Array<i64>,
    rows_processed: usize,
    record_id_elements_so_far: usize,
    warc_type_elements_so_far: usize,
    content_bytes_so_far: usize,
    header_elements_so_far: usize,
}

impl WarcRecordBatchBuilder {
    const DEFAULT_STRING_LENGTH: usize = 20;
    const DEFAULT_CONTENT_LENGTH: usize = 27282; // 27282 is the average content length of a WARC file sampled from 100 Common Crawl WARC files.

    fn new(chunk_size: usize, schema: SchemaRef) -> Self {
        Self {
            chunk_size,
            schema,
            record_id_array: MutableUtf8Array::with_capacities(
                chunk_size,
                Self::DEFAULT_STRING_LENGTH * chunk_size,
            ),
            warc_type_array: MutableUtf8Array::with_capacities(
                chunk_size,
                Self::DEFAULT_STRING_LENGTH * chunk_size,
            ),
            warc_date_array: MutablePrimitiveArray::with_capacity(chunk_size),
            warc_content_length_array: MutablePrimitiveArray::with_capacity(chunk_size),
            warc_identified_payload_type_array: MutableUtf8Array::with_capacities(
                chunk_size,
                Self::DEFAULT_STRING_LENGTH * chunk_size,
            ),
            content_array: MutableBinaryArray::with_capacities(
                chunk_size,
                Self::DEFAULT_CONTENT_LENGTH * chunk_size,
            ),
            header_array: MutableUtf8Array::with_capacities(
                chunk_size,
                Self::DEFAULT_STRING_LENGTH * chunk_size,
            ),
            rows_processed: 0,
            record_id_elements_so_far: 0,
            warc_type_elements_so_far: 0,
            content_bytes_so_far: 0,
            header_elements_so_far: 0,
        }
    }

    fn push(
        &mut self,
        record_id: Option<&str>,
        warc_type: Option<&str>,
        warc_date: Option<i64>,
        warc_content_length: Option<i64>,
        warc_identified_payload_type: Option<&str>,
        header: Option<&str>,
    ) {
        self.record_id_array.push(record_id);
        self.warc_type_array.push(warc_type);
        self.warc_date_array.push(warc_date);
        self.warc_content_length_array.push(warc_content_length);
        self.warc_identified_payload_type_array
            .push(warc_identified_payload_type);
        self.header_array.push(header);
        // book keeping
        self.rows_processed += 1;
        self.record_id_elements_so_far += record_id.map(|s| s.len()).unwrap_or(0);
        self.warc_type_elements_so_far += warc_type.map(|s| s.len()).unwrap_or(0);
        self.content_bytes_so_far += warc_content_length.map(|l| l as usize).unwrap_or(0);
        self.header_elements_so_far += header.map(|h| h.len()).unwrap_or(0);
    }

    fn len(&self) -> usize {
        self.record_id_array.len()
    }

    fn process_arrays(&mut self) -> DaftResult<Option<RecordBatch>> {
        let num_records = self.content_array.len();
        if num_records == 0 {
            Ok(None)
        } else {
            let record_batch = create_record_batch(
                self.schema.clone(),
                vec![
                    self.record_id_array.as_box(),
                    self.warc_type_array.as_box(),
                    self.warc_date_array.as_box(),
                    self.warc_content_length_array.as_box(),
                    self.warc_identified_payload_type_array.as_box(),
                    self.content_array.as_box(),
                    self.header_array.as_box(),
                ],
                num_records,
            )?;
            let chunk_size = self.chunk_size;
            let rows_processed = self.rows_processed;
            let avg_record_id_size = self.record_id_elements_so_far / rows_processed;
            // Reset arrays.
            self.record_id_array =
                MutableUtf8Array::with_capacities(chunk_size, avg_record_id_size * chunk_size);

            let avg_warc_type_size = self.warc_type_elements_so_far / rows_processed;

            self.warc_type_array =
                MutableUtf8Array::with_capacities(chunk_size, avg_warc_type_size * chunk_size);
            self.warc_date_array = MutablePrimitiveArray::with_capacity(chunk_size);
            self.warc_content_length_array = MutablePrimitiveArray::with_capacity(chunk_size);
            self.warc_identified_payload_type_array =
                MutableUtf8Array::with_capacities(chunk_size, avg_warc_type_size * chunk_size);

            let avg_content_size = self.content_bytes_so_far / rows_processed;

            self.content_array =
                MutableBinaryArray::with_capacities(chunk_size, avg_content_size * chunk_size);

            let avg_header_size = self.header_elements_so_far / rows_processed;

            self.header_array =
                MutableUtf8Array::with_capacities(chunk_size, avg_header_size * chunk_size);
            Ok(Some(record_batch))
        }
    }
}

struct WarcRecordBatchIterator {
    reader: Box<dyn AsyncBufRead + Unpin + Send>,
    chunk_size: usize,
    bytes_read: usize,
    header_state: WarcHeaderState,
    rb_builder: WarcRecordBatchBuilder,
}

impl WarcRecordBatchIterator {
    fn new(
        reader: Box<dyn AsyncBufRead + Unpin + Send>,
        schema: SchemaRef,
        chunk_size: usize,
    ) -> Self {
        Self {
            reader,
            chunk_size,
            bytes_read: 0,
            header_state: WarcHeaderState {
                content_length: None,
                record_id: None,
                warc_date: None,
                warc_type: None,
                warc_identified_payload_type: None,
                header_lines: Vec::new(),
            },
            rb_builder: WarcRecordBatchBuilder::new(chunk_size, schema),
        }
    }

    async fn read_chunk(&mut self) -> DaftResult<Option<RecordBatch>> {
        let mut line_buf = Vec::with_capacity(4096);

        loop {
            line_buf.clear();
            match self.reader.read_until(b'\n', &mut line_buf).await {
                Ok(0) => {
                    break;
                }
                Ok(2)
                    if line_buf[0] == b'\r'
                        && line_buf[1] == b'\n'
                        && self.header_state.content_length.is_some() =>
                {
                    self.bytes_read += 2;

                    // Create properly escaped JSON object from accumulated headers
                    let header_json = if self.header_state.header_lines.is_empty() {
                        "{}".to_string()
                    } else {
                        let mut obj = serde_json::Map::new();
                        for (key, value) in &self.header_state.header_lines {
                            obj.insert(key.clone(), serde_json::Value::String(value.clone()));
                        }
                        serde_json::to_string(&obj).unwrap_or_else(|_| "{}".to_string())
                    };

                    let len = self
                        .header_state
                        .content_length
                        .expect("Content length is required");

                    self.rb_builder.push(
                        self.header_state
                            .record_id
                            .map(|id| id.to_string())
                            .as_deref(),
                        self.header_state
                            .warc_type
                            .take()
                            .map(|t| t.to_string())
                            .as_deref(),
                        self.header_state
                            .warc_date
                            .and_then(|d| d.timestamp_nanos_opt()),
                        self.header_state.content_length.map(|len| len as i64),
                        self.header_state.warc_identified_payload_type.as_deref(),
                        Some(&header_json),
                    );

                    // Handle content array separately to avoid an intermediate copy
                    if len == 0 {
                        self.rb_builder.content_array.push_null();
                    } else {
                        let slice = self.rb_builder.content_array.allocate_slice(len);
                        self.reader.read_exact(slice).await?;
                        self.bytes_read += len;
                    }

                    self.header_state.reset();

                    if self.rb_builder.len() >= self.chunk_size {
                        break;
                    }
                }
                Ok(n) => {
                    // Handle WARC header lines.
                    self.bytes_read += n;
                    let line = String::from_utf8_lossy(&line_buf);
                    let line = line.trim();

                    // Skip the WARC version line.
                    if line.starts_with("WARC/") {
                        continue;
                    }
                    if let Some(colon_pos) = line.find(':') {
                        let key = line[..colon_pos].trim().to_string();
                        let value = line[colon_pos + 1..].trim().to_string();

                        match key.as_str() {
                            "Content-Length" => {
                                if let Ok(len) = value.parse::<usize>() {
                                    self.header_state.content_length = Some(len);
                                }
                            }
                            "WARC-Record-ID" => {
                                if value.starts_with('<') && value.ends_with('>') {
                                    let uuid_str = &value[10..value.len() - 1];
                                    if let Ok(uuid) = Uuid::parse_str(uuid_str) {
                                        self.header_state.record_id = Some(uuid);
                                    }
                                }
                            }
                            "WARC-Type" => {
                                self.header_state.warc_type = WarcType::from_str(&value);
                            }
                            "WARC-Date" => {
                                if let Ok(date) = DateTime::parse_from_rfc3339(&value) {
                                    self.header_state.warc_date = Some(date.with_timezone(&Utc));
                                }
                            }
                            "WARC-Identified-Payload-Type" => {
                                self.header_state.warc_identified_payload_type = Some(value);
                            }
                            _ => {
                                // Store non-mandatory headers.
                                self.header_state.header_lines.push((key, value));
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error reading line: {}", e);
                    break;
                }
            }
        }
        self.rb_builder.process_arrays()
    }
}

fn create_record_batch(
    schema: SchemaRef,
    arrays: Vec<Box<dyn arrow2::array::Array>>,
    num_records: usize,
) -> DaftResult<RecordBatch> {
    let mut series_vec = Vec::with_capacity(schema.len());
    for (field, array) in schema.into_iter().zip(arrays.into_iter()) {
        let series = Series::from_arrow(Arc::new(field.clone()), array)?;
        series_vec.push(series);
    }
    RecordBatch::new_with_size(schema, series_vec, num_records)
}

pub fn read_warc_bulk(
    uris: &[&str],
    convert_options: WarcConvertOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    max_chunks_in_flight: Option<usize>,
    num_parallel_tasks: usize,
) -> DaftResult<Vec<RecordBatch>> {
    let runtime_handle = get_io_runtime(multithreaded_io);
    let tables = runtime_handle.block_on_current_thread(async move {
        // Launch a read task per URI, throttling the number of concurrent file reads to num_parallel tasks.
        let task_stream = futures::stream::iter(uris.iter().map(|uri| {
            let (uri, convert_options, io_client, io_stats) = (
                (*uri).to_string(),
                convert_options.clone(),
                io_client.clone(),
                io_stats.clone(),
            );
            tokio::task::spawn(async move {
                read_warc_single_into_tables(
                    uri.as_str(),
                    convert_options,
                    io_client,
                    io_stats,
                    max_chunks_in_flight,
                )
                .await
            })
            .context(JoinSnafu {})
        }));
        let mut remaining_rows = convert_options.limit.map(|limit| limit as i64);
        task_stream
            // Limit the number of file reads we have in flight at any given time.
            .buffered(num_parallel_tasks)
            // Terminate the stream if we have already reached the row limit. With the upstream buffering, we will still read up to
            // num_parallel_tasks redundant files.
            .try_take_while(|result| {
                match (result, remaining_rows) {
                    // Limit has been met, early-terminate.
                    (_, Some(rows_left)) if rows_left <= 0 => futures::future::ready(Ok(false)),
                    // Limit has not yet been met, update remaining limit slack and continue.
                    (Ok(tables), Some(rows_left)) => {
                        for table in tables {
                            remaining_rows = Some(rows_left - table.len() as i64);
                        }
                        futures::future::ready(Ok(true))
                    }
                    // (1) No limit, never early-terminate.
                    // (2) Encountered error, propagate error to try_collect to allow it to short-circuit.
                    (_, None) | (Err(_), _) => futures::future::ready(Ok(true)),
                }
            })
            .map_ok(|tables| tables.into_iter().flatten().collect::<Vec<_>>())
            .try_collect::<Vec<_>>()
            .await
    })?;

    Ok(tables.into_iter().flatten().collect::<Vec<_>>())
}

async fn read_warc_single_into_tables(
    uri: &str,
    convert_options: WarcConvertOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<Vec<RecordBatch>> {
    let record_batch_stream = stream_warc(
        uri,
        io_client,
        io_stats,
        convert_options,
        max_chunks_in_flight,
    )
    .await?;
    let collected_tables = record_batch_stream
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .collect::<Vec<_>>();
    Ok(collected_tables)
}

pub async fn stream_warc(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    convert_options: WarcConvertOptions,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let predicate = convert_options.predicate.clone();
    let limit = convert_options.limit;
    let include_columns = convert_options.include_columns.clone();
    // Default max chunks in flight is set to 2x the number of cores, which should ensure pipelining of reading chunks
    // with the parsing of chunks on the rayon threadpool.
    let max_chunks_in_flight = max_chunks_in_flight.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .unwrap_or(NonZeroUsize::new(2).unwrap())
            .checked_mul(2.try_into().unwrap())
            .unwrap()
            .into()
    });

    let (reader, buffer_size, chunk_size): (Box<dyn AsyncBufRead + Unpin + Send>, usize, usize) =
        match io_client
            .single_url_get(uri.to_string(), None, io_stats.clone())
            .await?
        {
            GetResult::File(file) => {
                let buffer_size = 256 * 1024;
                let file_reader = File::open(file.path).await?;
                let counting_reader = CountingReader::new(file_reader, io_stats);
                (
                    Box::new(BufReader::with_capacity(buffer_size, counting_reader)),
                    buffer_size,
                    64,
                )
            }
            GetResult::Stream(stream, ..) => {
                (Box::new(StreamReader::new(stream)), 8 * 1024 * 1024, 64)
            }
        };

    let compression = CompressionCodec::from_uri(uri);

    let reader: Box<dyn AsyncBufRead + Unpin + Send> = if let Some(compression) = compression {
        let decoder = compression.to_decoder(reader);
        Box::new(tokio::io::BufReader::with_capacity(buffer_size, decoder))
    } else {
        reader
    };

    let schema = convert_options.schema.clone();

    let warc_record_batch_iter = WarcRecordBatchIterator::new(reader, schema, chunk_size);

    let stream = futures::stream::unfold(warc_record_batch_iter, |mut reader| async move {
        let val = reader.read_chunk().await;
        // println!("stuck in loop: {:#?}", val);
        match val {
            Ok(Some(batch)) => Some((Ok(batch), reader)),
            Ok(None) => None,
            Err(e) => Some((Err(e), reader)),
        }
    });

    let (tx, rx) = tokio::sync::mpsc::channel(max_chunks_in_flight);
    let compute_runtime = get_compute_runtime();
    let warc_stream_task = compute_runtime.spawn(async move {
        let filtered_stream = stream.map(move |table| {
            if let Some(predicate) = &predicate {
                let table = table?;
                let predicate = BoundExpr::try_new(predicate.clone(), &table.schema)?;
                let filtered = table.filter(&[predicate])?;
                if let Some(include_columns) = &include_columns {
                    let include_column_indices = include_columns
                        .iter()
                        .map(|name| table.schema.get_index(name))
                        .collect::<DaftResult<Vec<_>>>()?;

                    Ok(filtered.get_columns(&include_column_indices))
                } else {
                    Ok(filtered)
                }
            } else {
                table
            }
        });

        let mut remaining_rows = limit.map(|limit| limit as i64);
        let mut limited_stream = filtered_stream
            .try_take_while(move |recordbatch| {
                match (recordbatch, remaining_rows) {
                    // Limit has been met, early-terminate.
                    (_, Some(rows_left)) if rows_left <= 0 => futures::future::ready(Ok(false)),
                    // Limit has not yet been met, update remaining limit slack and continue
                    (table, Some(rows_left)) => {
                        remaining_rows = Some(rows_left - table.len() as i64);
                        futures::future::ready(Ok(true))
                    }
                    // No limit, never early-terminate
                    (_, None) => futures::future::ready(Ok(true)),
                }
            })
            .boxed();
        while let Some(batch) = limited_stream.next().await {
            if let Err(e) = tx.send(batch).await {
                eprintln!("Error sending batch to channel: {}", e);
                break;
            }
        }
    });
    let receiver_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let combined_stream = common_runtime::combine_stream(receiver_stream, warc_stream_task);

    Ok(combined_stream.boxed())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::prelude::{Field, Schema, TimeUnit};
    use daft_io::{IOConfig, IOStatsContext};

    use crate::{read_warc_bulk, WarcConvertOptions};

    #[test]
    fn test_warc_read_iostats() -> DaftResult<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("WARC-Record-ID", daft_core::prelude::DataType::Utf8),
            Field::new("WARC-Type", daft_core::prelude::DataType::Utf8),
            Field::new(
                "WARC-Date",
                daft_core::prelude::DataType::Timestamp(
                    TimeUnit::Nanoseconds,
                    Some("Etc/UTC".to_string()),
                ),
            ),
            Field::new("Content-Length", daft_core::prelude::DataType::Int64),
            Field::new(
                "WARC-Identified-Payload-Type",
                daft_core::prelude::DataType::Utf8,
            ),
            Field::new("warc_content", daft_core::prelude::DataType::Binary),
            Field::new("warc_headers", daft_core::prelude::DataType::Utf8),
        ]));
        let io_config = Arc::new(IOConfig::default());
        let io_client = daft_io::get_io_client(true, io_config)?;
        let io_stats = IOStatsContext::new("test_warc_read");
        let warc_file = format!("{}/test/example.warc", env!("CARGO_MANIFEST_DIR"),);
        let warc_gz_file = format!("{}/test/example.warc.gz", env!("CARGO_MANIFEST_DIR"),);

        let convert_options = WarcConvertOptions {
            schema: schema.clone(),
            predicate: None,
            include_columns: None,
            limit: None,
        };

        let _ = read_warc_bulk(
            &[&warc_file, &warc_gz_file],
            convert_options,
            io_client,
            Some(io_stats.clone()),
            true,
            None,
            8,
        )?;

        let warc_file_size = std::fs::metadata(&warc_file)?.len() as usize;
        let warc_gz_file_size = std::fs::metadata(&warc_gz_file)?.len() as usize;
        let bytes_read = io_stats.load_bytes_read();
        assert_eq!(
            bytes_read,
            warc_file_size + warc_gz_file_size,
            "IO stats should record the bytes read correctly"
        );
        Ok(())
    }
}
