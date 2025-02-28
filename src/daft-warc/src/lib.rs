use std::sync::Arc;

use arrow2::array::{MutableArray, MutableBinaryArray, MutablePrimitiveArray, MutableUtf8Array};
use chrono::{DateTime, Utc};
use common_error::DaftResult;
use daft_compression::CompressionCodec;
use daft_core::{prelude::SchemaRef, series::Series};
use daft_dsl::ExprRef;
use daft_io::{GetResult, IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use std::future::Future;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, BufReader},
};
use tokio_util::io::StreamReader;
use uuid::Uuid;

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
    header_lines: Vec<(String, String)>,
}

impl WarcHeaderState {
    fn reset(&mut self) {
        self.content_length = None;
        self.record_id = None;
        self.warc_date = None;
        self.warc_type = None;
        self.header_lines.clear();
    }
}

struct WarcRecordBatchIterator {
    reader: Box<dyn AsyncBufRead + Unpin + Send>,
    schema: SchemaRef,
    chunk_size: usize,
    bytes_read: usize,
    unprocessed_record: bool,
    header_state: WarcHeaderState,
    record_id_array: MutableUtf8Array<i64>,
    warc_type_array: MutableUtf8Array<i64>,
    warc_date_array: MutablePrimitiveArray<i64>,
    warc_content_length_array: MutablePrimitiveArray<i64>,
    content_array: MutableBinaryArray<i64>,
    header_array: MutableUtf8Array<i64>,
}

impl WarcRecordBatchIterator {
    fn new(
        reader: Box<dyn AsyncBufRead + Unpin + Send>,
        schema: SchemaRef,
        chunk_size: usize,
    ) -> Self {
        Self {
            reader,
            schema,
            chunk_size,
            bytes_read: 0,
            unprocessed_record: false,
            header_state: WarcHeaderState {
                content_length: None,
                record_id: None,
                warc_date: None,
                warc_type: None,
                header_lines: Vec::new(),
            },
            record_id_array: MutableUtf8Array::new(),
            warc_type_array: MutableUtf8Array::new(),
            warc_date_array: MutablePrimitiveArray::new(),
            warc_content_length_array: MutablePrimitiveArray::new(),
            content_array: MutableBinaryArray::new(),
            header_array: MutableUtf8Array::new(),
        }
    }

    async fn read_chunk(&mut self) -> DaftResult<Option<RecordBatch>> {
        let mut line_buf = Vec::with_capacity(4096);

        loop {
            if self.unprocessed_record {
                return Ok(Some(self.process_arrays()?));
            }

            line_buf.clear();
            match self.reader.read_until(b'\n', &mut line_buf).await {
                Ok(0) => {
                    return if self.unprocessed_record {
                        self.unprocessed_record = false;
                        Ok(Some(self.process_arrays()?))
                    } else {
                        Ok(None)
                    };
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
                    self.header_array.push(Some(header_json));

                    let len = self
                        .header_state
                        .content_length
                        .expect("Content length is required");

                    if len == 0 {
                        self.content_array.push_null();
                    } else {
                        let mut content_vec = vec![0; len];
                        self.reader.read_exact(&mut content_vec).await?;
                        self.content_array.push(Some(content_vec));
                        self.bytes_read += len;
                    }

                    self.record_id_array
                        .push(self.header_state.record_id.map(|id| id.to_string()));
                    self.warc_type_array
                        .push(self.header_state.warc_type.take().map(|t| t.to_string()));
                    self.warc_date_array.push(
                        self.header_state
                            .warc_date
                            .and_then(|d| d.timestamp_nanos_opt()),
                    );
                    self.warc_content_length_array
                        .push(self.header_state.content_length.map(|len| len as i64));

                    self.unprocessed_record = true;
                    self.header_state.reset();
                    if self.bytes_read >= self.chunk_size {
                        self.unprocessed_record = false;
                        return Ok(Some(self.process_arrays()?));
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
                            _ => {
                                // Store non-mandatory headers.
                                self.header_state.header_lines.push((key, value));
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error reading line: {}", e);
                    return if self.unprocessed_record {
                        self.unprocessed_record = false;
                        Ok(Some(self.process_arrays()?))
                    } else {
                        Ok(None)
                    };
                }
            }
        }
    }

    fn process_arrays(&mut self) -> DaftResult<RecordBatch> {
        let num_records = self.content_array.len();

        let record_batch = create_record_batch(
            self.schema.clone(),
            vec![
                self.record_id_array.as_box(),
                self.warc_type_array.as_box(),
                self.warc_date_array.as_box(),
                self.warc_content_length_array.as_box(),
                self.content_array.as_box(),
                self.header_array.as_box(),
            ],
            num_records,
        )?;

        // Reset arrays.
        self.record_id_array = MutableUtf8Array::new();
        self.warc_type_array = MutableUtf8Array::new();
        self.warc_date_array = MutablePrimitiveArray::new();
        self.warc_content_length_array = MutablePrimitiveArray::new();
        self.content_array = MutableBinaryArray::new();
        self.header_array = MutableUtf8Array::new();

        Ok(record_batch)
    }
}

fn create_record_batch(
    schema: SchemaRef,
    arrays: Vec<Box<dyn arrow2::array::Array>>,
    num_records: usize,
) -> DaftResult<RecordBatch> {
    let mut series_vec = Vec::with_capacity(schema.fields.len());
    for ((_, field), array) in schema.fields.iter().zip(arrays.into_iter()) {
        let series = Series::from_arrow(Arc::new(field.clone()), array)?;
        series_vec.push(series);
    }
    RecordBatch::new_with_size(schema, series_vec, num_records)
}

pub async fn stream_warc(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    convert_options: WarcConvertOptions,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let predicate = convert_options.predicate.clone();
    let limit = convert_options.limit;
    let include_columns = convert_options.include_columns.clone();

    let (reader, buffer_size, chunk_size): (Box<dyn AsyncBufRead + Unpin + Send>, usize, usize) =
        match io_client
            .single_url_get(uri.to_string(), None, io_stats)
            .await?
        {
            GetResult::File(file) => {
                let buffer_size = 256 * 1024;
                let file_reader = File::open(file.path).await?;
                (
                    Box::new(BufReader::with_capacity(buffer_size, file_reader)),
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
        match reader.read_chunk().await {
            Ok(Some(batch)) => Some((Ok(batch), reader)),
            Ok(None) => None,
            Err(e) => Some((Err(e), reader)),
        }
    });

    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let compute_runtime = common_runtime::get_compute_runtime();
    let warc_stream_task = compute_runtime.spawn(async move {
        let filtered_stream = stream.map(move |table| {
            if let Some(predicate) = &predicate {
                let filtered = table?.filter(&[predicate.clone()])?;
                if let Some(include_columns) = &include_columns {
                    filtered.get_columns(include_columns.as_slice())
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
    let combined_stream = combine_stream(receiver_stream, warc_stream_task);

    Ok(combined_stream.boxed())
}

fn combine_stream<T, E>(
    stream: impl Stream<Item = Result<T, E>> + Unpin,
    future: impl Future<Output = Result<(), E>>,
) -> impl Stream<Item = Result<T, E>> {
    use futures::stream::unfold;

    let initial_state = (Some(future), stream);

    unfold(initial_state, |(future, mut stream)| async move {
        future.as_ref()?;

        match stream.next().await {
            Some(item) => Some((item, (future, stream))),
            None => match future.unwrap().await {
                Err(error) => Some((Err(error), (None, stream))),
                Ok(()) => None,
            },
        }
    })
}
