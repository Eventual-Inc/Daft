use core::str;
use std::io::{Chain, Cursor, Read};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{num::NonZeroUsize, sync::Arc, sync::Condvar, sync::Mutex};

use crate::metadata::read_csv_schema_single;
use crate::ArrowSnafu;
use crate::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use arrow2::{
    datatypes::Field,
    io::csv::read,
    io::csv::read::{Reader, ReaderBuilder},
    io::csv::read_async::local_read_rows,
};
use common_error::{DaftError, DaftResult};
use crossbeam_channel::Sender;
use daft_core::{schema::Schema, utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_decoding::deserialize::deserialize_column;
use daft_dsl::{optimization::get_required_columns, Expr};
use daft_io::{IOClient, IOStatsRef};
use daft_table::Table;
use futures::{stream::BoxStream, Stream, StreamExt};
use rayon::{
    iter::IndexedParallelIterator,
    prelude::{IntoParallelRefIterator, ParallelIterator},
};
use snafu::ResultExt;

use crate::read::{fields_to_projection_indices, tables_concat};

#[allow(clippy::doc_lazy_continuation)]
/// Our local CSV reader takes the following approach to reading CSV files:
/// 1. Read the CSV file in 4MB chunks from a slab pool.
/// 2. Adjust the chunks so that chunks are contiguous and contain complete
///    CSV records. See `get_file_chunk` for more details.
/// 3. In parallel with the above, convert the adjusted chunks into byte records,
///    which are stored within pre-allocated CSV buffers.
/// 4. In parallel with the above, deserialize each CSV buffer into a Daft table
///    and stream the results.
///
///                 Slab Pool                                    CSV Buffer Pool
///               ┌────────────────────┐                       ┌────────────────────┐
///               │ 4MB Chunks         │                       │ CSV Buffers        │
///               │┌───┐┌───┐┌───┐     │                       │┌───┐┌───┐┌───┐     │
///               ││   ││   ││   │ ... │                       ││   ││   ││   │ ... │
///               │└─┬─┘└─┬─┘└───┘     │                       │└─┬─┘└─┬─┘└───┘     │
///               └──┼────┼────────────┘                       └──┼────┼────────────┘
///                  │    │                                       │    │
///     ───────┐     │    │                                       │    │
///   /│       │     │    │                                       │    │
///  /─┘       │     │    │                                       │    │
/// │          │     ▼    ▼                                       ▼    ▼
/// │          |    ┌───┐ ┌───┐          ┌────┐  ┬--─┐           ┌───┐ ┌───┐               ┌───┐ ┌───┐
/// │         ─┼───►│   │ │   │  ──────► │   ┬┘┌─┘ ┬─┘  ───────► │   │ │   │  ──────────►  │   │ │   │
/// │ CSV File │    └───┘ └───┘          └───┴ └───┘             └───┘ └───┘               └───┘ └───┘
/// │          │  Chain of buffers      Adjusted chunks     Vectors of ByteRecords     Stream of Daft tables
/// │          │
/// └──────────┘

/// A pool of ByteRecord slabs. Used for deserializing CSV.
#[derive(Debug)]
struct CsvBufferPool {
    buffers: Mutex<Vec<Vec<read::ByteRecord>>>,
    buffer_size: usize,
    record_buffer_size: usize,
    num_fields: usize,
}

/// A slab of ByteRecords. Used for deserializing CSV.
struct CsvBuffer {
    pool: Arc<CsvBufferPool>,
    buffer: Vec<read::ByteRecord>,
}

impl CsvBufferPool {
    pub fn new(
        record_buffer_size: usize,
        num_fields: usize,
        chunk_size_rows: usize,
        initial_pool_size: usize,
    ) -> Self {
        let chunk_buffers = vec![
            vec![
                read::ByteRecord::with_capacity(record_buffer_size, num_fields);
                chunk_size_rows
            ];
            initial_pool_size
        ];
        CsvBufferPool {
            buffers: Mutex::new(chunk_buffers),
            buffer_size: chunk_size_rows,
            record_buffer_size,
            num_fields,
        }
    }

    pub fn get_buffer(self: &Arc<Self>) -> CsvBuffer {
        let mut buffers = self.buffers.lock().unwrap();
        let buffer = buffers.pop();
        let buffer = match buffer {
            Some(buffer) => buffer,
            None => vec![
                read::ByteRecord::with_capacity(self.record_buffer_size, self.num_fields);
                self.buffer_size
            ],
        };

        CsvBuffer {
            pool: Arc::clone(self),
            buffer,
        }
    }

    fn return_buffer(&self, buffer: Vec<read::ByteRecord>) {
        let mut buffers = self.buffers.lock().unwrap();
        buffers.push(buffer);
    }
}

// The default size of a slab used for reading CSV files in chunks. Currently set to 4 MiB. This can be tuned.
const SLABSIZE: usize = 4 * 1024 * 1024;
// The default number of slabs in a slab pool.
const SLABPOOL_DEFAULT_SIZE: usize = 20;

/// A pool of slabs. Used for reading CSV files in SLABSIZE chunks.
#[derive(Debug)]
struct SlabPool {
    buffers: Mutex<Vec<Arc<[u8; SLABSIZE]>>>,
    condvar: Condvar,
}

/// A slab of bytes. Used for reading CSV files in SLABSIZE chunks.
#[derive(Clone)]
struct Slab {
    pool: Arc<SlabPool>,
    // We wrap the Arc<buffer> in an Option so that when a Slab is being dropped, we can move the Slab's reference
    // to the Arc<buffer> back to the slab pool.
    buffer: Option<Arc<[u8; SLABSIZE]>>,
}

impl Drop for Slab {
    fn drop(&mut self) {
        // Move the buffer back to the slab pool.
        if let Some(buffer) = self.buffer.take() {
            self.pool.return_buffer(buffer);
        }
    }
}

impl SlabPool {
    pub fn new() -> Self {
        let chunk_buffers: Vec<Arc<[u8; SLABSIZE]>> = (0..SLABPOOL_DEFAULT_SIZE)
            .map(|_| Arc::new([0; SLABSIZE]))
            .collect();
        SlabPool {
            buffers: Mutex::new(chunk_buffers),
            condvar: Condvar::new(),
        }
    }

    pub fn get_buffer(self: &Arc<Self>) -> Arc<[u8; SLABSIZE]> {
        let mut buffers = self.buffers.lock().unwrap();
        while buffers.is_empty() {
            // Instead of creating a new slab when we're out, we wait for a slab to be returned before waking up.
            // This potentially allows us to rate limit the CSV reader until downstream consumers are ready for data.
            buffers = self.condvar.wait(buffers).unwrap();
        }
        buffers.pop().unwrap()
    }

    fn return_buffer(&self, buffer: Arc<[u8; SLABSIZE]>) {
        let mut buffers = self.buffers.lock().unwrap();
        buffers.push(buffer);
        self.condvar.notify_one();
    }
}

/// A data structure that holds either a single slice of bytes, or a chain of two slices of bytes.
/// See the description to `parse_json` for more details.
#[derive(Debug)]
enum BufferSource<'a> {
    Single(Cursor<&'a [u8]>),
    Chain(Chain<Cursor<&'a [u8]>, Cursor<&'a [u8]>>),
}

/// Read implementation that allows BufferSource to be used by csv::read::Reader.
impl<'a> Read for BufferSource<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            BufferSource::Single(cursor) => std::io::Read::read(cursor, buf),
            BufferSource::Chain(chain) => chain.read(buf),
        }
    }
}

pub async fn read_csv_local(
    uri: &str,
    convert_options: Option<CsvConvertOptions>,
    parse_options: CsvParseOptions,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<Table> {
    let stream = stream_csv_local(
        uri,
        convert_options,
        parse_options,
        read_options,
        io_client,
        io_stats,
        max_chunks_in_flight,
    )
    .await?;
    tables_concat(tables_stream_collect(Box::pin(stream)).await)
}

async fn tables_stream_collect(stream: BoxStream<'static, DaftResult<Table>>) -> Vec<Table> {
    stream
        .filter_map(|result| async {
            match result {
                Ok(table) => Some(table),
                Err(_) => None, // Skips errors; you could log them or handle differently
            }
        })
        .collect()
        .await
}

pub async fn stream_csv_local(
    uri: &str,
    convert_options: Option<CsvConvertOptions>,
    parse_options: CsvParseOptions,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<impl Stream<Item = DaftResult<Table>> + Send> {
    let uri = uri.trim_start_matches("file://");
    let file = std::fs::File::open(uri)?;

    let predicate = convert_options
        .as_ref()
        .and_then(|opts| opts.predicate.clone());

    let limit = convert_options.as_ref().and_then(|opts| opts.limit);

    let include_columns = convert_options
        .as_ref()
        .and_then(|opts| opts.include_columns.clone());

    let convert_options = match (convert_options, &predicate) {
        (None, _) => None,
        (co, None) => co,
        (Some(mut co), Some(predicate)) => {
            if let Some(ref mut include_columns) = co.include_columns {
                let required_columns_for_predicate = get_required_columns(predicate);
                for rc in required_columns_for_predicate {
                    if include_columns.iter().all(|c| c.as_str() != rc.as_str()) {
                        include_columns.push(rc)
                    }
                }
            }
            // If we have a limit and a predicate, remove limit for stream.
            co.limit = None;
            Some(co)
        }
    }
    .unwrap_or_default();

    let slabpool = Arc::new(SlabPool::new());
    let (schema, estimated_mean_row_size, estimated_std_row_size) =
        get_schema_and_estimators(uri, &convert_options, &parse_options, io_client, io_stats)
            .await?;

    let n_threads: usize = std::thread::available_parallelism()
        .unwrap_or(NonZeroUsize::new(2).unwrap())
        .into();
    let chunk_size = read_options
        .as_ref()
        .and_then(|opt| opt.chunk_size.or_else(|| opt.buffer_size.map(|bs| bs / 8)))
        .unwrap_or(DEFAULT_CHUNK_SIZE);
    let projection_indices = fields_to_projection_indices(
        &schema.clone().fields,
        &convert_options.clone().include_columns,
    );
    let fields = schema.clone().fields;
    let fields_subset = projection_indices
        .iter()
        .map(|i| fields.get(*i).unwrap().into())
        .collect::<Vec<daft_core::datatypes::Field>>();
    let read_schema = Arc::new(daft_core::schema::Schema::new(fields_subset)?);
    let read_daft_fields = Arc::new(
        read_schema
            .fields
            .values()
            .map(|f| Arc::new(f.clone()))
            .collect::<Vec<_>>(),
    );
    let record_buffer_size = (estimated_mean_row_size + estimated_std_row_size).ceil() as usize;
    let chunk_size_rows = (chunk_size as f64 / record_buffer_size as f64).ceil() as usize;
    let num_fields = schema.fields.len();
    // TODO(desmond): We might consider creating per-process buffer pools and slab pools.
    let buffer_pool = Arc::new(CsvBufferPool::new(
        record_buffer_size,
        num_fields,
        chunk_size_rows,
        n_threads * 2,
    ));
    // We suppose that each slab of CSV data produces (chunk size / slab size) number of Daft tables. We
    // then double this capacity to ensure that our channel is never full and our threads won't deadlock.
    let (sender, receiver) =
        crossbeam_channel::bounded(max_chunks_in_flight.unwrap_or(2 * chunk_size / SLABSIZE));
    rayon::spawn(move || {
        consume_csv_file(
            file,
            buffer_pool,
            slabpool,
            parse_options,
            projection_indices,
            read_daft_fields,
            read_schema,
            fields,
            num_fields,
            &include_columns,
            predicate,
            limit,
            sender,
        );
    });

    Ok(futures::stream::iter(receiver))
}

/// Helper function that reads up to 1 MiB of the CSV file to  estimate stats and/or infer the schema of the file.
async fn get_schema_and_estimators(
    uri: &str,
    convert_options: &CsvConvertOptions,
    parse_options: &CsvParseOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<(arrow2::datatypes::Schema, f64, f64)> {
    let (inferred_schema, read_stats) = read_csv_schema_single(
        uri,
        parse_options.clone(),
        // Read at most 1 MiB to estimate stats.
        Some(1024 * 1024),
        io_client.clone(),
        io_stats.clone(),
    )
    .await?;

    let mut schema = if let Some(schema) = convert_options.schema.clone() {
        schema.to_arrow()?
    } else {
        inferred_schema.to_arrow()?
    };
    // Rename fields, if necessary.
    if let Some(column_names) = convert_options.column_names.clone() {
        schema = schema
            .fields
            .into_iter()
            .zip(column_names.iter())
            .map(|(field, name)| {
                Field::new(name, field.data_type, field.is_nullable).with_metadata(field.metadata)
            })
            .collect::<Vec<_>>()
            .into();
    }
    Ok((
        schema,
        read_stats.mean_record_size_bytes,
        read_stats.stddev_record_size_bytes,
    ))
}

/// Consumes the CSV file and sends the results to `sender`.
#[allow(clippy::too_many_arguments)]
fn consume_csv_file(
    mut file: std::fs::File,
    buffer_pool: Arc<CsvBufferPool>,
    slabpool: Arc<SlabPool>,
    parse_options: CsvParseOptions,
    projection_indices: Arc<Vec<usize>>,
    read_daft_fields: Arc<Vec<Arc<daft_core::datatypes::Field>>>,
    read_schema: Arc<Schema>,
    fields: Vec<Field>,
    num_fields: usize,
    include_columns: &Option<Vec<String>>,
    predicate: Option<Arc<Expr>>,
    limit: Option<usize>,
    sender: Sender<Result<Table, DaftError>>,
) {
    let rows_read = Arc::new(AtomicUsize::new(0));
    let mut has_header = parse_options.has_header;
    let total_len = file.metadata().unwrap().len() as usize;
    let field_delimiter = parse_options.delimiter;
    let escape_char = parse_options.escape_char;
    let quote_char = parse_options.quote;
    let double_quote_escape_allowed = parse_options.double_quote;
    let mut total_bytes_read = 0;
    let mut next_slab = None;
    let mut next_buffer_len = 0;
    let mut first_buffer = true;
    loop {
        let limit_reached = limit.map_or(false, |limit| {
            let current_rows_read = rows_read.load(Ordering::Relaxed);
            current_rows_read >= limit
        });
        if limit_reached {
            break;
        }
        let (current_slab, current_buffer_len) = match next_slab.take() {
            Some(next_slab) => {
                total_bytes_read += next_buffer_len;
                (next_slab, next_buffer_len)
            }
            None => {
                let mut buffer = slabpool.get_buffer();
                match Arc::get_mut(&mut buffer) {
                    Some(inner_buffer) => {
                        let bytes_read = file.read(inner_buffer).unwrap();
                        if bytes_read == 0 {
                            slabpool.return_buffer(buffer);
                            break;
                        }
                        total_bytes_read += bytes_read;
                        (
                            Arc::new(Slab {
                                pool: Arc::clone(&slabpool),
                                buffer: Some(buffer),
                            }),
                            bytes_read,
                        )
                    }
                    None => {
                        slabpool.return_buffer(buffer);
                        break;
                    }
                }
            }
        };
        (next_slab, next_buffer_len) = if total_bytes_read < total_len {
            let mut next_buffer = slabpool.get_buffer();
            match Arc::get_mut(&mut next_buffer) {
                Some(inner_buffer) => {
                    let bytes_read = file.read(inner_buffer).unwrap();
                    if bytes_read == 0 {
                        slabpool.return_buffer(next_buffer);
                        (None, 0)
                    } else {
                        (
                            Some(Arc::new(Slab {
                                pool: Arc::clone(&slabpool),
                                buffer: Some(next_buffer),
                            })),
                            bytes_read,
                        )
                    }
                }
                None => {
                    slabpool.return_buffer(next_buffer);
                    break;
                }
            }
        } else {
            (None, 0)
        };
        let file_chunk = get_file_chunk(
            unsafe_clone_buffer(&current_slab.buffer),
            current_buffer_len,
            next_slab
                .as_ref()
                .map(|slab| unsafe_clone_buffer(&slab.buffer)),
            next_buffer_len,
            first_buffer,
            num_fields,
            quote_char,
            field_delimiter,
            escape_char,
            double_quote_escape_allowed,
        );
        first_buffer = false;
        if let (None, _) = file_chunk {
            // Return the buffer. It doesn't matter that we still have a reference to the slab. We're going to fallback
            // and the slabs will be useless.
            slabpool.return_buffer(unsafe_clone_buffer(&current_slab.buffer));
            // Exit early before spawning a new thread.
            break;
            // TODO(desmond): we should fallback instead.
        }
        let current_slab_clone = Arc::clone(&current_slab);
        let next_slab_clone = next_slab.clone();
        let parse_options = parse_options.clone();
        let csv_buffer = buffer_pool.get_buffer();
        let projection_indices = projection_indices.clone();
        let fields = fields.clone();
        let read_daft_fields = read_daft_fields.clone();
        let read_schema = read_schema.clone();
        let include_columns = include_columns.clone();
        let predicate = predicate.clone();
        let sender = sender.clone();
        let rows_read = Arc::clone(&rows_read);
        rayon::spawn(move || {
            let limit_reached = limit.map_or(false, |limit| {
                let current_rows_read = rows_read.load(Ordering::Relaxed);
                current_rows_read >= limit
            });
            if !limit_reached {
                match file_chunk {
                    (Some(start), None) => {
                        if let Some(buffer) = &current_slab_clone.buffer {
                            let buffer_source = BufferSource::Single(Cursor::new(
                                &buffer[start..current_buffer_len],
                            ));
                            dispatch_to_parse_csv(
                                has_header,
                                parse_options,
                                buffer_source,
                                projection_indices,
                                fields,
                                read_daft_fields,
                                read_schema,
                                csv_buffer,
                                &include_columns,
                                predicate,
                                sender,
                                rows_read,
                            );
                        } else {
                            panic!("Trying to read from a CSV buffer that doesn't exist. Please report this issue.")
                        }
                    }
                    (Some(start), Some(end)) => {
                        if let Some(next_slab_clone) = next_slab_clone
                            && let Some(current_buffer) = &current_slab_clone.buffer
                            && let Some(next_buffer) = &next_slab_clone.buffer
                        {
                            let buffer_source = BufferSource::Chain(std::io::Read::chain(
                                Cursor::new(&current_buffer[start..current_buffer_len]),
                                Cursor::new(&next_buffer[..end]),
                            ));
                            dispatch_to_parse_csv(
                                has_header,
                                parse_options,
                                buffer_source,
                                projection_indices,
                                fields,
                                read_daft_fields,
                                read_schema,
                                csv_buffer,
                                &include_columns,
                                predicate,
                                sender,
                                rows_read,
                            );
                        } else {
                            panic!("Trying to read from an overflow CSV buffer that doesn't exist. Please report this issue.")
                        }
                    }
                    _ => panic!(
                        "Something went wrong when parsing the CSV file. Please report this issue."
                    ),
                };
            }
        });
        has_header = false;
        if total_bytes_read >= total_len {
            break;
        }
    }
}

/// Unsafe helper function that extracts the buffer from an &Option<Arc<[u8]>>. Users should
/// ensure that the buffer is Some, otherwise this function causes the process to panic.
fn unsafe_clone_buffer(buffer: &Option<Arc<[u8; SLABSIZE]>>) -> Arc<[u8; SLABSIZE]> {
    match buffer {
        Some(buffer) => Arc::clone(buffer),
        None => panic!("Tried to clone a CSV slab that doesn't exist. Please report this error."),
    }
}

#[allow(clippy::doc_lazy_continuation)]
/// Helper function that determines what chunk of data to parse given a starting position within the
/// file, and the desired initial chunk size.
///
/// Given a starting position, we use our chunk size to compute a preliminary start and stop
/// position. For example, we can visualize all preliminary chunks in a file as follows.
///
///  Chunk 1         Chunk 2         Chunk 3              Chunk N
/// ┌──────────┐    ┌──────────┐    ┌──────────┐         ┌──────────┐
/// │          │    │\n        │    │   \n     │         │       \n │
/// │          │    │          │    │          │         │          │
/// │          │    │       \n │    │          │         │          │
/// │   \n     │    │          │    │      \n  │         │          │
/// │          │    │          │    │          │   ...   │     \n   │
/// │          │    │  \n      │    │          │         │          │
/// │ \n       │    │          │    │          │         │          │
/// │          │    │          │    │     \n   │         │  \n      │
/// └──────────┘    └──────────┘    └──────────┘         └──────────┘
///
/// However, record boundaries (i.e. the \n terminators) do not align nicely with these preliminary
/// chunk boundaries. So we adjust each preliminary chunk as follows:
/// - Find the first record terminator from the chunk's start. This is the new starting position.
/// - Find the first record terminator from the chunk's end. This is the new ending position.
/// - If a given preliminary chunk doesn't contain a record terminator, the adjusted chunk is empty.
///
/// For example:
///
///  Adjusted Chunk 1    Adj. Chunk 2        Adj. Chunk 3          Adj. Chunk N
/// ┌──────────────────┐┌─────────────────┐ ┌────────┐            ┌─┐
/// │                \n││               \n│ │      \n│         \n │ │
/// │          ┌───────┘│      ┌──────────┘ │  ┌─────┘            │ │
/// │          │    ┌───┘   \n │    ┌───────┘  │         ┌────────┘ │
/// │   \n     │    │          │    │      \n  │         │          │
/// │          │    │          │    │          │   ...   │     \n   │
/// │          │    │  \n      │    │          │         │          │
/// │ \n       │    │          │    │          │         │          │
/// │          │    │          │    │     \n   │         │  \n      │
/// └──────────┘    └──────────┘    └──────────┘         └──────────┘
///
/// Using this method, we now have adjusted chunks that are aligned with record boundaries, that do
/// not overlap, and that fully cover every byte in the CSV file. Parsing each adjusted chunk can
/// now happen in parallel.
///
/// This is the same method as described in:
/// Ge, Chang et al. “Speculative Distributed CSV Data Parsing for Big Data Analytics.” Proceedings of the 2019 International Conference on Management of Data (2019).
///
/// Another observation is that seeing a pure \n character is not necessarily indicative of a record
/// terminator. We need to consider whether the \n character was seen within a quoted field, since the
/// string "some text \n some text" is a valid CSV string field. To do this, we carry out the following
/// algorithm:
/// 1. Find a \n character.
/// 2. Check if the CSV string immediately following this \n character is valid, i.e. does it parse
///    as valid CSV, and does it produce the same number of fields as our schema.
/// 2a. If there is a valid record at this point, then we assume that the \n we saw was a valid terminator.
/// 2b. If the record at this point is invalid, then this was likely a \n in a quoted field. Find the next
///     \n character and go back to 2.
#[allow(clippy::too_many_arguments)]
fn get_file_chunk(
    current_buffer: Arc<[u8; SLABSIZE]>,
    current_buffer_len: usize,
    next_buffer: Option<Arc<[u8; SLABSIZE]>>,
    next_buffer_len: usize,
    first_buffer: bool,
    num_fields: usize,
    quote_char: u8,
    field_delimiter: u8,
    escape_char: Option<u8>,
    double_quote_escape_allowed: bool,
) -> (Option<usize>, Option<usize>) {
    // TODO(desmond): There is a potential fast path here when `escape_char` is None: simply check for \n characters.
    let start = if !first_buffer {
        let start = next_line_position(
            &current_buffer[..current_buffer_len],
            0,
            num_fields,
            quote_char,
            field_delimiter,
            escape_char,
            double_quote_escape_allowed,
        );
        match start {
            Some(_) => start,
            None => return (None, None), // If the record size is >= 4MB, return None and fallback.
        }
    } else {
        Some(0)
    };
    // If there is a next buffer, find the adjusted chunk in that buffer. If there's no next buffer, we're at the end of the file.
    let end = if let Some(next_buffer) = next_buffer {
        let end = next_line_position(
            &next_buffer[..next_buffer_len],
            0,
            num_fields,
            quote_char,
            field_delimiter,
            escape_char,
            double_quote_escape_allowed,
        );
        match end {
            Some(_) => end,
            None => return (None, None), // If the record size is >= 4MB, return None and fallback.
        }
    } else {
        None
    };
    (start, end)
}

/// Helper function that finds the first valid record terminator in a buffer.
fn next_line_position(
    buffer: &[u8],
    offset: usize,
    num_fields: usize,
    quote_char: u8,
    field_delimiter: u8,
    escape_char: Option<u8>,
    double_quote_escape_allowed: bool,
) -> Option<usize> {
    let mut start = offset;
    loop {
        start = match newline_position(&buffer[start..]) {
            // Start reading after the first record terminator from the start of the chunk.
            Some(pos) => start + pos + 1,
            None => return None,
        };
        if start >= buffer.len() {
            return None;
        }
        if validate_csv_record(
            &buffer[start..],
            num_fields,
            quote_char,
            field_delimiter,
            escape_char,
            double_quote_escape_allowed,
        ) {
            return Some(start);
        }
    }
}

// Daft does not currently support non-\n record terminators (e.g. carriage return \r, which only
// matters for pre-Mac OS X).
const NEWLINE: u8 = b'\n';
const DOUBLE_QUOTE: u8 = b'"';
const DEFAULT_CHUNK_SIZE: usize = SLABSIZE; // 4MiB. Like SLABSIZE, this can be tuned.

/// Helper function that finds the first new line character (\n) in the given byte slice.
fn newline_position(buffer: &[u8]) -> Option<usize> {
    // Assuming we are searching for the ASCII `\n` character, we don't need to do any special
    // handling for UTF-8, since a `\n` value always corresponds to an ASCII `\n`.
    // For more details, see: https://en.wikipedia.org/wiki/UTF-8#Encoding
    memchr::memchr(NEWLINE, buffer)
}

/// Csv states used by the state machine in `validate_csv_record`.
#[derive(Clone)]
enum CsvState {
    FieldStart,
    RecordEnd,
    UnquotedField,
    QuotedField,
    Unquote,
}

/// State machine that validates whether the current buffer starts at a valid csv record.
/// See `get_file_chunk` for more details.
fn validate_csv_record(
    buffer: &[u8],
    num_fields: usize,
    quote_char: u8,
    field_delimiter: u8,
    escape_char: Option<u8>,
    double_quote_escape_allowed: bool,
) -> bool {
    let mut state = CsvState::FieldStart;
    let mut index = 0;
    let mut num_fields_seen = 0;
    loop {
        if index >= buffer.len() {
            // We've reached the end of the buffer without seeing a valid record.
            return false;
        }
        match state {
            CsvState::FieldStart => {
                let byte = buffer[index];
                if byte == NEWLINE {
                    state = CsvState::RecordEnd;
                } else if byte == quote_char {
                    state = CsvState::QuotedField;
                    index += 1;
                } else {
                    state = CsvState::UnquotedField;
                }
            }
            CsvState::RecordEnd => {
                return num_fields_seen == num_fields;
            }
            CsvState::UnquotedField => {
                // We follow the convention where an unquoted field does consider escape characters.
                while index < buffer.len() {
                    let byte = buffer[index];
                    if byte == NEWLINE {
                        num_fields_seen += 1;
                        state = CsvState::RecordEnd;
                        break;
                    }
                    if byte == field_delimiter {
                        num_fields_seen += 1;
                        state = CsvState::FieldStart;
                        index += 1;
                        break;
                    }
                    index += 1;
                }
            }
            CsvState::QuotedField => {
                while index < buffer.len() {
                    let byte = buffer[index];
                    if byte == quote_char {
                        state = CsvState::Unquote;
                        index += 1;
                        break;
                    }
                    if let Some(escape_char) = escape_char
                        && byte == escape_char
                    {
                        // Skip the next character.
                        index += 1;
                    }
                    index += 1;
                }
            }
            CsvState::Unquote => {
                let byte = buffer[index];
                if let Some(escape_char) = escape_char
                    && byte == escape_char
                    && escape_char == quote_char
                    && (byte != DOUBLE_QUOTE || double_quote_escape_allowed)
                {
                    state = CsvState::QuotedField;
                    index += 1;
                    continue;
                }
                if byte == NEWLINE {
                    num_fields_seen += 1;
                    state = CsvState::RecordEnd;
                    continue;
                }
                if byte == field_delimiter {
                    num_fields_seen += 1;
                    state = CsvState::FieldStart;
                    index += 1;
                    continue;
                }
                // Other characters are not allowed after a quote. This is invalid CSV.
                return false;
            }
        }
    }
}

/// Helper function that takes in a BufferSource, calls parse_csv() to extract table values from
/// the buffer source, then streams the results to `sender`.
#[allow(clippy::too_many_arguments)]
fn dispatch_to_parse_csv(
    has_header: bool,
    parse_options: CsvParseOptions,
    buffer_source: BufferSource,
    projection_indices: Arc<Vec<usize>>,
    fields: Vec<Field>,
    read_daft_fields: Arc<Vec<Arc<daft_core::datatypes::Field>>>,
    read_schema: Arc<Schema>,
    csv_buffer: CsvBuffer,
    include_columns: &Option<Vec<String>>,
    predicate: Option<Arc<Expr>>,
    sender: Sender<Result<Table, DaftError>>,
    rows_read: Arc<AtomicUsize>,
) {
    let table_results = {
        let rdr = ReaderBuilder::new()
            .has_headers(has_header)
            .delimiter(parse_options.delimiter)
            .double_quote(parse_options.double_quote)
            .quote(parse_options.quote)
            .escape(parse_options.escape_char)
            .comment(parse_options.comment)
            .flexible(parse_options.allow_variable_columns)
            .from_reader(buffer_source);
        parse_csv_chunk(
            rdr,
            projection_indices,
            fields,
            read_daft_fields,
            read_schema,
            csv_buffer,
            include_columns,
            predicate,
        )
    };
    match table_results {
        Ok(tables) => {
            for table in tables {
                let table_len = table.len();
                sender.send(Ok(table)).unwrap();
                // Atomically update the number of rows read only after the result has
                // been sent. In theory we could wrap these steps in a mutex, but
                // applying limit at this layer can be best-effort with no adverse
                // side effects.
                rows_read.fetch_add(table_len, Ordering::Relaxed);
            }
        }
        Err(e) => sender.send(Err(e)).unwrap(),
    }
}

/// Helper function that consumes a CSV reader and turns it into a vector of Daft tables.
#[allow(clippy::too_many_arguments)]
fn parse_csv_chunk<R>(
    mut reader: Reader<R>,
    projection_indices: Arc<Vec<usize>>,
    fields: Vec<arrow2::datatypes::Field>,
    read_daft_fields: Arc<Vec<Arc<daft_core::datatypes::Field>>>,
    read_schema: Arc<Schema>,
    csv_buffer: CsvBuffer,
    include_columns: &Option<Vec<String>>,
    predicate: Option<Arc<Expr>>,
) -> DaftResult<Vec<Table>>
where
    R: std::io::Read,
{
    let mut chunk_buffer = csv_buffer.buffer;
    let mut tables = vec![];
    loop {
        //let time = Instant::now();
        let (rows_read, has_more) =
            local_read_rows(&mut reader, chunk_buffer.as_mut_slice()).context(ArrowSnafu {})?;
        //let time = Instant::now();
        let chunk = projection_indices
            .par_iter()
            .enumerate()
            .map(|(i, proj_idx)| {
                let deserialized_col = deserialize_column(
                    &chunk_buffer[0..rows_read],
                    *proj_idx,
                    fields[*proj_idx].data_type().clone(),
                    0,
                );
                Series::try_from_field_and_arrow_array(
                    read_daft_fields[i].clone(),
                    cast_array_for_daft_if_needed(deserialized_col?),
                )
            })
            .collect::<DaftResult<Vec<Series>>>()?;
        let num_rows = chunk.first().map(|s| s.len()).unwrap_or(0);
        let table = Table::new_unchecked(read_schema.clone(), chunk, num_rows);
        let table = if let Some(predicate) = &predicate {
            let filtered = table.filter(&[predicate.clone()])?;
            if let Some(include_columns) = &include_columns {
                filtered.get_columns(include_columns.as_slice())?
            } else {
                filtered
            }
        } else {
            table
        };
        tables.push(table);

        // The number of record might exceed the number of byte records we've allocated.
        // Retry until all byte records in this chunk are read.
        if !has_more {
            break;
        }
    }
    csv_buffer.pool.return_buffer(chunk_buffer);
    Ok(tables)
}
