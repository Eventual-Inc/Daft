use core::str;
use std::io::{Chain, Cursor, Read};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{num::NonZeroUsize, sync::Arc, sync::Condvar, sync::Mutex};

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
use daft_table::Table;
use futures::{stream::BoxStream, Stream, StreamExt};
use rayon::{
    iter::IndexedParallelIterator,
    prelude::{IntoParallelRefIterator, ParallelIterator},
};
use snafu::ResultExt;
use crate::local::pool::{read_slabs_windowed, WindowedSlab};
use crate::read::{fields_to_projection_indices, tables_concat};

mod pool;


#[allow(clippy::doc_lazy_continuation)]
/// Our local CSV reader has the following approach to reading CSV files:
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
/// │         ─┼───►┌───┐ ┌───┐          ┌────┐  ┬--─┐           ┌───┐ ┌───┐               ┌───┐ ┌───┐
/// │          │    │   │ │   │  ──────► │   ┬┘┌─┘ ┬─┘  ───────► │   │ │   │  ──────────►  │   │ │   │
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
            None => {
                println!("csv buf empty");
                vec![
                    read::ByteRecord::with_capacity(self.record_buffer_size, self.num_fields);
                    self.buffer_size
                ]
            }
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

// The default size of a slab used for reading CSV files in chunks. Currently set to 4MiB.
const SLAB_SIZE: usize = 4 * 1024 * 1024;
// The default number of slabs in a slab pool.
const SLAB_POOL_DEFAULT_SIZE: usize = 20;

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
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<Table> {
    let stream = stream_csv_local(
        uri,
        convert_options,
        parse_options,
        read_options,
        max_chunks_in_flight,
    ).await?;
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
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<impl Stream<Item=DaftResult<Table>> + Send> {
    let uri = uri.trim_start_matches("file://");
    let file = tokio::fs::File::open(uri).await?;

    // TODO(desmond): This logic is repeated multiple times in the csv reader files. Should dedup.
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
    // End of `should dedup`.

    // TODO(desmond): We should do better schema inference here.
    let schema = convert_options.clone().schema.unwrap().to_arrow()?;
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
    // TODO(desmond): Need better upfront estimators. Cory did something like what we need here: https://github.com/universalmind303/Daft/blob/7b40f23a5ff83aba4ab059b62ac781d7766be0b1/src/daft-json/src/local.rs#L338
    let estimated_mean_row_size = 100f64;
    let estimated_std_row_size = 20f64;
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
    let slabpool = Arc::new(SlabPool::new());
    // We suppose that each slab of CSV data produces (chunk size / slab size) number of Daft tables. We
    // then double this capacity to ensure that our channel is never full and our threads won't deadlock.
    let (sender, receiver) =
        crossbeam_channel::bounded(max_chunks_in_flight.unwrap_or(2 * chunk_size / SLAB_SIZE));
    tokio::spawn(async move || {
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
    let result_stream = futures::stream::iter(receiver);
    Ok(result_stream)
}

/// Consumes the CSV file and sends the results to `sender`.
#[allow(clippy::too_many_arguments)]
fn consume_csv_file(
    file: tokio::fs::File,
    buffer_pool: Arc<CsvBufferPool>,
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
    let mut csv_consumer = CsvConsumer::new(
        file,
        buffer_pool,
        slabpool,
        parse_options,
        projection_indices,
        read_daft_fields,
        read_schema,
        fields,
        num_fields,
        include_columns.clone(),
        predicate,
        limit,
        sender,
    );

    csv_consumer.consume();
}

/// A struct representing a CSV consumer that processes and parses CSV data.
struct CsvConsumer {
    /// The file being read.
    file: tokio::fs::File,
    /// Options for parsing the CSV file.
    parse_options: CsvParseOptions,
    /// Indices of columns to be projected (included in the output).
    projection_indices: Arc<Vec<usize>>,
    /// Fields to be read from the CSV, in Daft format.
    read_daft_fields: Arc<Vec<Arc<daft_core::datatypes::Field>>>,
    /// Schema of the data to be read.
    read_schema: Arc<Schema>,
    /// Fields of the CSV file.
    fields: Vec<Field>,
    /// Total number of fields in the CSV.
    num_fields: usize,
    /// Optional list of columns to include in the output.
    include_columns: Option<Vec<String>>,
    /// Optional predicate for filtering rows.
    predicate: Option<Arc<Expr>>,
    /// Optional limit on the number of rows to read.
    limit: Option<usize>,
    /// Channel sender for sending parsed tables or errors.
    sender: Sender<Result<Table, DaftError>>,
    /// Atomic counter for the number of rows read.
    rows_read: Arc<AtomicUsize>,
    /// Flag indicating whether the CSV has a header row.
    has_header: bool,
    /// Total length of the file in bytes.
    total_len: usize,
    /// Total number of bytes read so far.
    total_bytes_read: usize,
    /// Length of the next buffer to be processed.
    next_buffer_len: usize,
    /// Flag indicating if this is the first buffer being processed.
    first_buffer: bool,
}

impl CsvConsumer {
    /// Creates a new CsvConsumer instance.
    fn new(
        file: tokio::fs::File,
        buffer_pool: Arc<CsvBufferPool>,
        slabpool: Arc<SlabPool>,
        parse_options: CsvParseOptions,
        projection_indices: Arc<Vec<usize>>,
        read_daft_fields: Arc<Vec<Arc<daft_core::datatypes::Field>>>,
        read_schema: Arc<Schema>,
        fields: Vec<Field>,
        num_fields: usize,
        include_columns: Option<Vec<String>>,
        predicate: Option<Arc<Expr>>,
        limit: Option<usize>,
        sender: Sender<Result<Table, DaftError>>,
    ) -> Self {
        let total_len = file.metadata().unwrap().len() as usize;
        let has_header = parse_options.has_header;
        Self {
            file,
            buffer_pool,
            slabpool,
            parse_options,
            projection_indices,
            read_daft_fields,
            read_schema,
            fields,
            num_fields,
            include_columns,
            predicate,
            limit,
            sender,
            rows_read: Arc::new(AtomicUsize::new(0)),
            has_header,
            total_len,
            total_bytes_read: 0,
            next_slab: None,
            next_buffer_len: 0,
            first_buffer: true,
        }
    }

    /// Main method to consume and process the CSV file.
    async fn consume(mut self, file: tokio::fs::File) {
        let mut pool = read_slabs_windowed(file, SLAB_SIZE, SLAB_POOL_DEFAULT_SIZE);

        loop {
            if self.limit_reached() {
                break;
            }

            let Some(windowed_slab) = pool.next().await else {
                break;
            };

            if !self.process_slab(windowed_slab) {
                break;
            }
            self.has_header = false;
            if self.total_bytes_read >= self.total_len {
                break;
            }
        }
    }

    /// Checks if the row limit has been reached.
    fn limit_reached(&self) -> bool {
        self.limit.map_or(false, |limit| {
            let current_rows_read = self.rows_read.load(Ordering::Relaxed);
            current_rows_read >= limit
        })
    }

    /// Processes a single slab of data.
    fn process_slab(&mut self, windowed_slab: WindowedSlab) -> bool {
        let file_chunk = self.get_file_chunk(&current_slab, current_buffer_len);
        self.first_buffer = false;

        if let (None, _) = file_chunk {
            self.slabpool.return_buffer(unsafe_clone_buffer(&current_slab.buffer));
            return false;
        }

        self.spawn_parse_thread(current_slab, file_chunk);
        true
    }

    /// Retrieves the current slab to be processed.
    fn get_current_slab(&mut self) -> (Arc<Slab>, usize) {
        match self.next_slab.take() {
            Some(next_slab) => {
                self.total_bytes_read += self.next_buffer_len;
                (next_slab, self.next_buffer_len)
            }
            None => self.read_new_slab(),
        }
    }

    /// Reads a new slab from the file.
    fn read_new_slab(&mut self) -> (Arc<Slab>, usize) {
        let mut buffer = self.slabpool.get_buffer();
        match Arc::get_mut(&mut buffer) {
            Some(inner_buffer) => {
                let bytes_read = self.file.read(inner_buffer).unwrap();
                if bytes_read == 0 {
                    self.slabpool.return_buffer(buffer);
                    return (Arc::new(Slab {
                        pool: Arc::clone(&self.slabpool),
                        buffer: None,
                    }), 0);
                }
                self.total_bytes_read += bytes_read;
                (
                    Arc::new(Slab {
                        pool: Arc::clone(&self.slabpool),
                        buffer: Some(buffer),
                    }),
                    bytes_read,
                )
            }
            None => {
                self.slabpool.return_buffer(buffer);
                (Arc::new(Slab {
                    pool: Arc::clone(&self.slabpool),
                    buffer: None,
                }), 0)
            }
        }
    }

    /// Prepares the next slab for processing.
    fn prepare_next_slab(&mut self) {
        if self.total_bytes_read < self.total_len {
            let mut next_buffer = self.slabpool.get_buffer();
            match Arc::get_mut(&mut next_buffer) {
                Some(inner_buffer) => {
                    let bytes_read = self.file.read(inner_buffer).unwrap();
                    if bytes_read == 0 {
                        self.slabpool.return_buffer(next_buffer);
                        self.next_slab = None;
                        self.next_buffer_len = 0;
                    } else {
                        self.next_slab = Some(Arc::new(Slab {
                            pool: Arc::clone(&self.slabpool),
                            buffer: Some(next_buffer),
                        }));
                        self.next_buffer_len = bytes_read;
                    }
                }
                None => {
                    self.slabpool.return_buffer(next_buffer);
                    self.next_slab = None;
                    self.next_buffer_len = 0;
                }
            }
        } else {
            self.next_slab = None;
            self.next_buffer_len = 0;
        }
    }

    /// Retrieves the chunk of the file to be processed.
    fn get_file_chunk(&self, current_slab: &WindowedSlab, first_buffer: bool) -> (Option<usize>, Option<usize>) {
        let (left, right) = match current_slab.as_slice() {
            [left, right] => (left.as_slice(), Some(right.as_slice())),
            [left] => (left.as_slice(), None),
            _ => unreachable!("Unexpected windowed slab size"),
        };


        get_file_chunk(
            left,
            right,
            first_buffer,
            self.num_fields,
            self.parse_options.quote,
            self.parse_options.delimiter,
            self.parse_options.escape_char,
            self.parse_options.double_quote,
        )
    }

    /// Spawns a new thread to parse the CSV chunk.
    fn spawn_parse_thread(&self, current_slab: Arc<Slab>, file_chunk: (Option<usize>, Option<usize>)) {
        let current_slab_clone = Arc::clone(&current_slab);
        let next_slab_clone = self.next_slab.clone();
        let parse_options = self.parse_options.clone();
        let csv_buffer = self.buffer_pool.get_buffer();
        let projection_indices = self.projection_indices.clone();
        let fields = self.fields.clone();
        let read_daft_fields = self.read_daft_fields.clone();
        let read_schema = self.read_schema.clone();
        let include_columns = self.include_columns.clone();
        let predicate = self.predicate.clone();
        let sender = self.sender.clone();
        let rows_read = Arc::clone(&self.rows_read);
        let limit = self.limit;
        let has_header = self.has_header;

        rayon::spawn(move || {
            if !Self::thread_limit_reached(&rows_read, limit) {
                Self::parse_chunk(
                    has_header,
                    parse_options,
                    current_slab_clone,
                    next_slab_clone,
                    file_chunk,
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
            }
        });
    }

    /// Checks if the thread has reached its row limit.
    fn thread_limit_reached(rows_read: &Arc<AtomicUsize>, limit: Option<usize>) -> bool {
        limit.map_or(false, |limit| {
            let current_rows_read = rows_read.load(Ordering::Relaxed);
            current_rows_read >= limit
        })
    }

    /// Parses a chunk of the CSV file.
    #[allow(clippy::too_many_arguments)]
    fn parse_chunk(
        has_header: bool,
        parse_options: CsvParseOptions,
        current_slab: Arc<Slab>,
        next_slab: Option<Arc<Slab>>,
        file_chunk: (Option<usize>, Option<usize>),
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
        match file_chunk {
            (Some(start), None) => {
                if let Some(buffer) = &current_slab.buffer {
                    let buffer_source = BufferSource::Single(Cursor::new(
                        &buffer[start..],
                    ));
                    Self::dispatch_to_parse_csv(
                        has_header,
                        parse_options,
                        buffer_source,
                        projection_indices,
                        fields,
                        read_daft_fields,
                        read_schema,
                        csv_buffer,
                        include_columns,
                        predicate,
                        sender,
                        rows_read,
                    );
                } else {
                    panic!("Trying to read from a CSV buffer that doesn't exist. Please report this issue.")
                }
            }
            (Some(start), Some(end)) => {
                if let Some(next_slab) = next_slab
                    && let Some(current_buffer) = &current_slab.buffer
                    && let Some(next_buffer) = &next_slab.buffer
                {
                    let buffer_source = BufferSource::Chain(Read::chain(
                        Cursor::new(&current_buffer[start..]),
                        Cursor::new(&next_buffer[..end]),
                    ));
                    Self::dispatch_to_parse_csv(
                        has_header,
                        parse_options,
                        buffer_source,
                        projection_indices,
                        fields,
                        read_daft_fields,
                        read_schema,
                        csv_buffer,
                        include_columns,
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
}

/// Unsafe helper function that extracts the buffer from an &Option<Arc<[u8]>>. Users should
/// ensure that the buffer is Some, otherwise this function causes the process to panic.
fn unsafe_clone_buffer(buffer: &Option<Arc<[u8; SLAB_SIZE]>>) -> Arc<[u8; SLAB_SIZE]> {
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
    current_buffer: &[u8],
    next_buffer: Option<&[u8]>,
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
const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024; // 1MiB. TODO(desmond): This should be tuned.

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
    R: Read,
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
