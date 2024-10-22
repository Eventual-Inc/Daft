use core::str;
use std::{
    io::Read,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
};

use arrow2::{
    datatypes::Field,
    io::csv::{
        read::{self, Reader, ReaderBuilder},
        read_async::local_read_rows,
    },
};
use common_error::{DaftError, DaftResult};
use crossbeam_channel::Sender;
use daft_core::{
    prelude::{Schema, Series},
    utils::arrow::cast_array_for_daft_if_needed,
};
use daft_decoding::deserialize::deserialize_column;
use daft_dsl::{optimization::get_required_columns, Expr};
use daft_io::{IOClient, IOStatsRef};
use daft_table::Table;
use futures::Stream;
use rayon::{
    iter::IndexedParallelIterator,
    prelude::{IntoParallelRefIterator, ParallelIterator},
};
use snafu::ResultExt;

use crate::{
    metadata::read_csv_schema_single,
    read::{fields_to_projection_indices, tables_concat},
    ArrowSnafu, CsvConvertOptions, CsvParseOptions, CsvReadOptions,
};

// Our local CSV reader takes the following approach to reading CSV files:
// 1. Read the CSV file in 4MB chunks from a slab pool.
// 2. Adjust the chunks so that chunks are contiguous and contain complete
//    CSV records. See `get_file_chunk` for more details.
// 3. In parallel with the above, convert the adjusted chunks into byte records,
//    which are stored within pre-allocated CSV buffers.
// 4. In parallel with the above, deserialize each CSV buffer into a Daft table
//    and stream the results.
//
//                 Slab Pool                                    CSV Buffer Pool
//               ┌────────────────────┐                       ┌────────────────────┐
//               │ 4MB Chunks         │                       │ CSV Buffers        │
//               │┌───┐┌───┐┌───┐     │                       │┌───┐┌───┐┌───┐     │
//               ││   ││   ││   │ ... │                       ││   ││   ││   │ ... │
//               │└─┬─┘└─┬─┘└───┘     │                       │└─┬─┘└─┬─┘└───┘     │
//               └──┼────┼────────────┘                       └──┼────┼────────────┘
//                  │    │                                       │    │
//     ───────┐     │    │                                       │    │
//   /│       │     │    │                                       │    │
//  /─┘       │     │    │                                       │    │
// │          │     ▼    ▼                                       ▼    ▼
// │          |    ┌───┐ ┌───┐          ┌────┐  ┬--─┐           ┌───┐ ┌───┐               ┌───┐ ┌───┐
// │         ─┼───►│   │ │   │  ──────► │   ┬┘┌─┘ ┬─┘  ───────► │   │ │   │  ──────────►  │   │ │   │
// │ CSV File │    └───┘ └───┘          └───┴ └───┘             └───┘ └───┘               └───┘ └───┘
// │          │  Chain of buffers      Adjusted chunks     Vectors of ByteRecords     Stream of Daft tables
// │          │
// └──────────┘

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
    buffer: Vec<read::ByteRecord>,
    pool: Weak<CsvBufferPool>,
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
        Self {
            buffers: Mutex::new(chunk_buffers),
            buffer_size: chunk_size_rows,
            record_buffer_size,
            num_fields,
        }
    }

    pub fn get_buffer(self: &Arc<Self>) -> CsvBuffer {
        let buffer = {
            let mut buffers = self.buffers.lock().unwrap();
            let buffer = buffers.pop();
            match buffer {
                Some(buffer) => buffer,
                None => {
                    vec![
                        read::ByteRecord::with_capacity(self.record_buffer_size, self.num_fields);
                        self.buffer_size
                    ]
                }
            }
        };

        CsvBuffer {
            buffer,
            pool: Arc::downgrade(self),
        }
    }

    fn return_buffer(&self, buffer: Vec<read::ByteRecord>) {
        if let Ok(mut buffers) = self.buffers.lock() {
            buffers.push(buffer);
        }
    }
}

impl Drop for CsvBuffer {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let buffer = std::mem::take(&mut self.buffer);
            pool.return_buffer(buffer);
        }
    }
}

// The default size of a slab used for reading CSV files in chunks. Currently set to 4 MiB. This can be tuned.
const SLABSIZE: usize = 4 * 1024 * 1024;
// The default number of slabs in a slab pool. With 20 slabs, we reserve a total of 80 MiB of memory for reading file data.
const SLABPOOL_DEFAULT_SIZE: usize = 20;

/// A pool of slabs. Used for reading CSV files in SLABSIZE chunks.
#[derive(Debug)]
struct FileSlabPool {
    slabs: Mutex<Vec<RwLock<FileSlabState>>>,
}

#[derive(Debug, Default)]
struct FileSlabState {
    buffer: Box<[u8]>,
    valid_bytes: usize,
}

impl FileSlabState {
    fn find_newline(&self, offset: usize) -> Option<usize> {
        newline_position(&self.buffer[offset..self.valid_bytes])
    }

    fn validate_record(&self, validator: &mut CsvValidator, start: usize) -> Option<bool> {
        validator.validate_record(&mut self.buffer[start..self.valid_bytes].iter())
    }
}

/// A slab of bytes. Used for reading CSV files in SLABSIZE chunks.
#[derive(Debug)]
struct FileSlab {
    state: RwLock<FileSlabState>,
    pool: Weak<FileSlabPool>,
}

impl FileSlab {
    fn find_newline(&self, offset: usize) -> Option<usize> {
        let guard = self.state.read().unwrap();
        guard.find_newline(offset)
    }
}

// Modify the Drop method for FileSlabs so that their states are returned to their parent slab pool.
impl Drop for FileSlab {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let file_slab_state = std::mem::take(&mut self.state);
            pool.return_slab(file_slab_state);
        }
    }
}

impl FileSlabPool {
    fn new() -> Arc<Self> {
        let slabs: Vec<RwLock<FileSlabState>> = (0..SLABPOOL_DEFAULT_SIZE)
            // We get uninitialized buffers because we will always populate the buffers with a file read before use.
            .map(|_| Box::new_uninit_slice(SLABSIZE))
            .map(|x| unsafe { x.assume_init() })
            .map(|buffer| {
                RwLock::new(FileSlabState {
                    buffer,
                    valid_bytes: 0,
                })
            })
            .collect();
        Arc::new(Self {
            slabs: Mutex::new(slabs),
        })
    }

    fn get_slab(self: &Arc<Self>) -> Arc<FileSlab> {
        let slab = {
            let mut slabs = self.slabs.lock().unwrap();
            let slab = slabs.pop();
            match slab {
                Some(slab) => slab,
                None => RwLock::new(FileSlabState {
                    buffer: unsafe { Box::new_uninit_slice(SLABSIZE).assume_init() },
                    valid_bytes: 0,
                }),
            }
        };

        Arc::new(FileSlab {
            state: slab,
            pool: Arc::downgrade(self),
        })
    }

    fn return_slab(&self, slab: RwLock<FileSlabState>) {
        if let Ok(mut slabs) = self.slabs.lock() {
            slabs.push(slab);
        }
    }
}

/// Reads a single local CSV file in a non-streaming fashion.
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
        convert_options.clone(),
        parse_options.clone(),
        read_options,
        io_client.clone(),
        io_stats.clone(),
        max_chunks_in_flight,
    )
    .await?;
    let tables = Box::pin(stream);
    // Apply limit.
    let limit = convert_options.as_ref().and_then(|opts| opts.limit);
    let mut remaining_rows = limit.map(|limit| limit as i64);
    use futures::TryStreamExt;
    let collected_tables = tables
        .try_take_while(|result| {
            match (result, remaining_rows) {
                // Limit has been met, early-terminate.
                (_, Some(rows_left)) if rows_left <= 0 => futures::future::ready(Ok(false)),
                // Limit has not yet been met, update remaining limit slack and continue.
                (table, Some(rows_left)) => {
                    remaining_rows = Some(rows_left - table.len() as i64);
                    futures::future::ready(Ok(true))
                }
                // (1) No limit, never early-terminate.
                // (2) Encountered error, propagate error to try_collect to allow it to short-circuit.
                (_, None) => futures::future::ready(Ok(true)),
            }
        })
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .collect::<Vec<_>>();
    // Handle empty table case.
    if collected_tables.is_empty() {
        let (schema, _, _) = get_schema_and_estimators(
            uri,
            &convert_options.unwrap_or_default(),
            &parse_options,
            io_client,
            io_stats,
        )
        .await?;
        return Table::empty(Some(Arc::new(Schema::try_from(&schema)?)));
    }
    let concated_table = tables_concat(collected_tables)?;
    if let Some(limit) = limit
        && concated_table.len() > limit
    {
        // Apply head in case that last chunk went over limit.
        concated_table.head(limit)
    } else {
        Ok(concated_table)
    }
}

/// Reads a single local CSV file in a streaming fashion.
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
    let file_len = file.metadata()?.len();

    // Process the CSV convert options.
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
                        include_columns.push(rc);
                    }
                }
            }
            // If we have a limit and a predicate, remove limit for stream.
            co.limit = None;
            Some(co)
        }
    }
    .unwrap_or_default();

    // Get schema and row estimations.
    let (schema, estimated_mean_row_size, estimated_std_row_size) =
        get_schema_and_estimators(uri, &convert_options, &parse_options, io_client, io_stats)
            .await?;
    let num_fields = schema.fields.len();
    let projection_indices =
        fields_to_projection_indices(&schema.fields, &convert_options.clone().include_columns);
    let fields_subset = projection_indices
        .iter()
        .map(|i| schema.fields.get(*i).unwrap().into())
        .collect::<Vec<daft_core::datatypes::Field>>();
    let read_schema = Arc::new(Schema::new(fields_subset)?);
    let read_daft_fields = Arc::new(
        read_schema
            .fields
            .values()
            .map(|f| Arc::new(f.clone()))
            .collect::<Vec<_>>(),
    );

    // Create CSV buffer pool.
    let n_threads: usize = std::thread::available_parallelism()
        .unwrap_or(NonZeroUsize::new(2).unwrap())
        .into();
    let record_buffer_size = (estimated_mean_row_size + estimated_std_row_size).ceil() as usize;
    let chunk_size = read_options
        .as_ref()
        .and_then(|opt| opt.chunk_size.or_else(|| opt.buffer_size.map(|bs| bs / 8)))
        .unwrap_or(DEFAULT_CHUNK_SIZE);
    let chunk_size_rows = (chunk_size as f64 / record_buffer_size as f64).ceil() as usize;

    // We produce `file_len / SLABSIZE` number of file slabs, each of which might be split into two, so the maximum number
    // of tasks we might spawn is 2 times this number.
    let (sender, receiver) = crossbeam_channel::bounded(
        max_chunks_in_flight
            .unwrap_or_else(|| 2 * (file_len as f64 / SLABSIZE as f64).ceil() as usize),
    );

    // Consume the CSV file asynchronously.
    rayon::spawn(move || {
        // TODO(desmond): We might consider creating per-process buffer pools and slab pools.
        let buffer_pool = Arc::new(CsvBufferPool::new(
            record_buffer_size,
            num_fields,
            chunk_size_rows,
            n_threads * 2,
        ));
        consume_slab_iterator(
            file,
            buffer_pool,
            num_fields,
            parse_options,
            projection_indices,
            read_daft_fields,
            read_schema,
            schema.fields,
            include_columns,
            predicate,
            limit,
            sender,
        );
    });

    let flattened_receiver = receiver.into_iter().flat_map(|rx| rx.into_iter());
    let stream = futures::stream::iter(flattened_receiver);
    Ok(stream)
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
        read_stats.mean_record_size_bytes.max(8_f64),
        read_stats.stddev_record_size_bytes.max(8_f64),
    ))
}

struct SlabIterator {
    file: std::fs::File,
    slabpool: Arc<FileSlabPool>,
    total_bytes_read: usize,
}

impl SlabIterator {
    fn new(file: std::fs::File, slabpool: Arc<FileSlabPool>) -> Self {
        Self {
            file,
            slabpool,
            total_bytes_read: 0,
        }
    }
}

impl Iterator for SlabIterator {
    type Item = (Arc<FileSlab>, usize, bool);
    fn next(&mut self) -> Option<Self::Item> {
        let slab = self.slabpool.get_slab();
        let (bytes_read, filled_buffer) = {
            let mut writer = slab.state.write().unwrap();
            let bytes_read = self.file.read(&mut writer.buffer).unwrap();
            if bytes_read == 0 {
                return None;
            }
            let filled_buffer = bytes_read == writer.buffer.len();
            self.total_bytes_read += bytes_read;
            writer.valid_bytes = bytes_read;
            (bytes_read, filled_buffer)
        };

        Some((slab, bytes_read, filled_buffer))
    }
}

#[allow(clippy::too_many_arguments)]
fn consume_slab_iterator(
    file: std::fs::File,
    buffer_pool: Arc<CsvBufferPool>,
    num_fields: usize,
    parse_options: CsvParseOptions,
    projection_indices: Arc<Vec<usize>>,
    read_daft_fields: Arc<Vec<Arc<daft_core::datatypes::Field>>>,
    read_schema: Arc<Schema>,
    fields: Vec<Field>,
    include_columns: Option<Vec<String>>,
    predicate: Option<Arc<Expr>>,
    limit: Option<usize>,
    sender: Sender<crossbeam_channel::Receiver<Result<Table, DaftError>>>,
) {
    // Create slab pool for file reads.
    let slabpool = FileSlabPool::new();
    let rows_read = Arc::new(AtomicUsize::new(0));
    let mut slab_iterator = SlabIterator::new(file, slabpool);
    let mut has_header = parse_options.has_header;
    let field_delimiter = parse_options.delimiter;
    let escape_char = parse_options.escape_char;
    let quote_char = parse_options.quote;
    let double_quote_escape_allowed = parse_options.double_quote;

    let mut curr_chunk = ChunkStateHolder::empty();
    let mut csv_validator = CsvValidator::new(
        num_fields,
        quote_char,
        field_delimiter,
        escape_char,
        double_quote_escape_allowed,
    );
    loop {
        // Check limit.
        let limit_reached = limit.map_or(false, |limit| {
            let current_rows_read = rows_read.load(Ordering::Relaxed);
            current_rows_read >= limit
        });
        if limit_reached {
            break;
        }
        // Grab a starting file slab if the current CSV chunk is empty.
        if curr_chunk.is_empty() {
            if let Some((next, bytes_read, _)) = slab_iterator.next() {
                curr_chunk.states.push(ChunkState::Start {
                    slab: next,
                    start: 0,
                    end: bytes_read,
                });
                curr_chunk.reset();
            } else {
                // EOF.
                break;
            }
            continue;
        }
        // Grab file slabs until we find a valid CSV chunk.
        loop {
            if let Some((next, bytes_read, filled_buffer)) = slab_iterator.next() {
                // If the next buffer is not completely filled, we take this to mean that we've reached EOF.
                if !filled_buffer {
                    curr_chunk.states.push(ChunkState::Final {
                        end: bytes_read,
                        slab: next,
                    });
                    break;
                }
                curr_chunk.states.push(ChunkState::Continue {
                    slab: next,
                    end: bytes_read,
                });
                while curr_chunk.goto_next_newline() {
                    if curr_chunk.validate_csv_record(&mut csv_validator, &mut slab_iterator) {
                        break;
                    }
                }
                if curr_chunk.is_valid() {
                    break;
                }
            } else {
                // If there is no next file slab, turn the last ChunkState into a final ChunkState.
                if let Some(last_state) = curr_chunk.states.pop() {
                    match last_state {
                        ChunkState::Start { slab, start, end } => {
                            curr_chunk
                                .states
                                .push(ChunkState::StartAndFinal { slab, start, end });
                        }
                        ChunkState::Continue { slab, end } => {
                            curr_chunk.states.push(ChunkState::Final { slab, end });
                        }
                        _ => panic!("There should be no final CSV chunk states at this point."),
                    }
                } else {
                    panic!("There should be at least one CSV chunk state at this point.")
                }
                break;
            }
        }
        let states_to_read = curr_chunk.states;
        curr_chunk.states = std::mem::take(&mut curr_chunk.next_states);
        curr_chunk.reset();
        let parse_options = parse_options.clone();
        let projection_indices = projection_indices.clone();
        let fields = fields.clone();
        let read_daft_fields = read_daft_fields.clone();
        let read_schema = read_schema.clone();
        let include_columns = include_columns.clone();
        let predicate = predicate.clone();
        let stream_sender = sender.clone();
        let rows_read = Arc::clone(&rows_read);
        let mut csv_buffer = buffer_pool.get_buffer();
        // We produce roughly `SLABSIZE / DEFAULT_CHUNK_SIZE` tables per state, with maybe an overflow of 1 table per state.
        let (tx, rx) = crossbeam_channel::bounded(
            2 * (states_to_read.len() as f64 * SLABSIZE as f64 / DEFAULT_CHUNK_SIZE as f64).ceil()
                as usize,
        );
        rayon::spawn(move || {
            if let Some(state) = states_to_read.last() {
                assert!(state.is_final());
            } else {
                return;
            }
            let multi_slice_reader = MultiSliceReader::new(states_to_read.as_slice());
            dispatch_to_parse_csv(
                has_header,
                parse_options,
                multi_slice_reader,
                projection_indices,
                fields,
                read_daft_fields,
                read_schema,
                &mut csv_buffer,
                include_columns,
                predicate,
                tx,
                rows_read,
            );
        });
        has_header = false;

        let _ = stream_sender.send(rx);
    }
}

struct ChunkStateHolder {
    states: Vec<ChunkState>,
    next_states: Vec<ChunkState>,
    curr_newline_idx: usize,
    curr_newline_offset: usize,
    curr_byte_read_idx: usize,
    curr_byte_read_offset: usize,
    valid_chunk: bool,
    // current_reader: Option<FileSlabStateReader<>>,
}

impl ChunkStateHolder {
    fn new(states: Vec<ChunkState>) -> Self {
        Self {
            states,
            next_states: vec![],
            curr_newline_idx: 1,
            curr_newline_offset: 0,
            curr_byte_read_idx: 0,
            curr_byte_read_offset: 0,
            valid_chunk: false,
            // current_reader: None,
        }
    }

    fn empty() -> Self {
        Self::new(vec![])
    }

    fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    #[allow(clippy::doc_lazy_continuation)]
    /// `goto_next_line` and `validate_csv_record` are two helper function that determines what chunk of
    /// data to parse given a starting position within the file, and the desired initial chunk size.
    ///
    /// Given a starting position, we use our chunk size to compute a preliminary start and stop
    /// position. For example, we can visualize all preliminary chunks in a file as follows.
    ///
    /// Chunk 1         Chunk 2         Chunk 3              Chunk N
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
    /// Adjusted Chunk 1    Adj. Chunk 2        Adj. Chunk 3          Adj. Chunk N
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
    fn goto_next_newline(&mut self) -> bool {
        self.valid_chunk = false;
        loop {
            if self.curr_newline_idx >= self.states.len() {
                return false;
            }
            if self.curr_newline_offset >= self.states[self.curr_newline_idx].len() {
                self.curr_newline_offset = 0;
                self.curr_newline_idx += 1;
                continue;
            }
            if let Some(pos) =
                self.states[self.curr_newline_idx].find_newline(self.curr_newline_offset)
            {
                self.curr_newline_offset = pos + 1;
                if self.curr_newline_offset >= self.states[self.curr_newline_idx].len() {
                    self.curr_newline_offset = 0;
                    self.curr_newline_idx += 1;
                    continue;
                }
                self.curr_byte_read_idx = self.curr_newline_idx;
                self.curr_byte_read_offset = self.curr_newline_offset;
                return true;
            } else {
                self.curr_newline_offset = 0;
                self.curr_newline_idx += 1;
                continue;
            }
        }
    }

    fn validate_csv_record(
        &mut self,
        validator: &mut CsvValidator,
        slab_iter: &mut impl Iterator<Item = (Arc<FileSlab>, usize, bool)>,
    ) -> bool {
        validator.reset();
        loop {
            // Run the CSV state machine to see if we're currently at a valid record boundary.
            if self.curr_byte_read_idx < self.states.len() {
                let validity = {
                    let state = &self.states[self.curr_byte_read_idx];
                    // Only the first slab that we're validating might not start from offset 0 (it starts from the offset where we found a newline).
                    if self.curr_byte_read_offset >= state.len() {
                        self.curr_byte_read_idx += 1;
                        self.curr_byte_read_offset = 0;
                        continue;
                    }
                    let guard = state.get_slab().state.read().unwrap();
                    guard.validate_record(validator, self.curr_byte_read_offset)
                };
                if let Some(valid) = validity {
                    self.valid_chunk = valid;
                    if valid {
                        let (ending_idx, ending_offset) = if self.curr_byte_read_offset == 0 {
                            (
                                self.curr_byte_read_idx - 1,
                                self.states[self.curr_byte_read_idx - 1].len(),
                            )
                        } else {
                            (self.curr_byte_read_idx, self.curr_byte_read_offset)
                        };
                        self.next_states = self.states.split_off(ending_idx);
                        if let Some(front_of_next) = self.next_states.get_mut(0) {
                            self.states.push(ChunkState::Final {
                                slab: Arc::clone(front_of_next.get_slab()),
                                end: ending_offset,
                            });
                            *front_of_next = ChunkState::Start {
                                slab: Arc::clone(front_of_next.get_slab()),
                                start: ending_offset,
                                end: front_of_next.len(),
                            };
                        } else {
                            panic!("There should be at least one chunk state that's split off.");
                        }
                    }
                    return valid;
                }
            }
            // We ran out of bytes while running the CSV state machine. Read another file slab then
            // continue running the state machine.
            if let Some((next, bytes_read, filled_buffer)) = slab_iter.next() {
                if !filled_buffer {
                    // EOF. Make this chunk state holder valid and exit.
                    self.states.push(ChunkState::Final {
                        slab: next,
                        end: bytes_read,
                    });
                    self.valid_chunk = true;
                    return true;
                }
                self.states.push(ChunkState::Continue {
                    slab: next,
                    end: bytes_read,
                });
            } else {
                // EOF. Make this chunk state holder valid and exit.
                self.valid_chunk = true;
                return true;
            }
        }
    }

    fn is_valid(&self) -> bool {
        self.valid_chunk
    }

    fn reset(&mut self) {
        self.curr_newline_idx = 1;
        self.curr_newline_offset = 0;
        self.curr_byte_read_idx = 0;
        self.curr_byte_read_offset = 0;
    }
}

#[derive(Debug)]
enum ChunkState {
    Start {
        slab: Arc<FileSlab>,
        start: usize,
        end: usize,
    },
    StartAndFinal {
        slab: Arc<FileSlab>,
        start: usize,
        end: usize,
    },
    Continue {
        slab: Arc<FileSlab>,
        end: usize,
    },
    Final {
        slab: Arc<FileSlab>,
        end: usize,
    },
}

impl ChunkState {
    #[inline]
    fn is_final(&self) -> bool {
        matches!(self, Self::Final { .. } | Self::StartAndFinal { .. })
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::Start { start, end, .. } => end - start,
            Self::StartAndFinal { start, end, .. } => end - start,
            Self::Continue { end, .. } => *end,
            Self::Final { end, .. } => *end,
        }
    }

    #[inline]
    fn find_newline(&self, offset: usize) -> Option<usize> {
        match self {
            Self::Continue { slab, .. } => slab.find_newline(offset),
            // This function is not needed for non-Continue chunk states.
            _ => None,
        }
    }

    #[inline]
    fn get_slab(&self) -> &Arc<FileSlab> {
        match self {
            Self::Start { slab, .. } => slab,
            Self::StartAndFinal { slab, .. } => slab,
            Self::Continue { slab, .. } => slab,
            Self::Final { slab, .. } => slab,
        }
    }
}

struct MultiSliceReader<'a> {
    states: &'a [ChunkState],
    curr_read_idx: usize,
    curr_read_offset: usize,
}

impl<'a> MultiSliceReader<'a> {
    fn new(states: &'a [ChunkState]) -> Self {
        Self {
            states,
            curr_read_idx: 0,
            curr_read_offset: 0,
        }
    }
}

impl<'a> Read for MultiSliceReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        let mut position = 0;
        while self.curr_read_idx < self.states.len() && position < buf_len {
            let state = &self.states[self.curr_read_idx];
            let (start, end, guard) = match state {
                ChunkState::Start { slab, start, end } => {
                    let guard: std::sync::RwLockReadGuard<'_, FileSlabState> =
                        slab.state.read().unwrap();
                    (*start, *end, guard)
                }
                ChunkState::StartAndFinal { slab, start, end } => {
                    let guard = slab.state.read().unwrap();
                    (*start, *end, guard)
                }
                ChunkState::Continue { slab, end } => {
                    let guard = slab.state.read().unwrap();
                    (0, *end, guard)
                }
                ChunkState::Final { slab, end } => {
                    let guard = slab.state.read().unwrap();
                    (0, *end, guard)
                }
            };
            let slice = &guard.buffer[start..end];
            if self.curr_read_offset < slice.len() {
                let read_size = (buf_len - position).min(slice.len() - self.curr_read_offset);
                buf[position..position + read_size].copy_from_slice(
                    &slice[self.curr_read_offset..self.curr_read_offset + read_size],
                );
                self.curr_read_offset += read_size;
                position += read_size;
            }
            if self.curr_read_offset >= slice.len() {
                self.curr_read_offset = 0;
                self.curr_read_idx += 1;
            }
        }
        Ok(position)
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

struct CsvValidator {
    state: CsvState,
    num_fields: usize,
    num_fields_seen: usize,
    quote_char: u8,
    field_delimiter: u8,
    escape_char: Option<u8>,
    double_quote_escape_allowed: bool,
    // The transition table only needs to consider 256 possible inputs, because the only characters
    // that are valid for transitioning the table state are single-byte ASCII characters. Furthermore,
    // even when reading UTF-8, upon encountering a byte that matches the value for an ASCII character,
    // this byte will always correspond to the ASCII character.
    // For more details, see: https://en.wikipedia.org/wiki/UTF-8#Encoding
    transition_table: [[CsvState; 256]; 6],
}

/// Csv states used by the state machine in `validate_csv_record`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CsvState {
    FieldStart,
    RecordEnd,
    UnquotedField,
    QuotedField,
    Unquote,
    Escape,
    Invalid,
}

impl CsvValidator {
    fn new(
        num_fields: usize,
        quote_char: u8,
        field_delimiter: u8,
        escape_char: Option<u8>,
        double_quote_escape_allowed: bool,
    ) -> Self {
        let mut validator = Self {
            state: CsvState::FieldStart,
            num_fields,
            num_fields_seen: 0,
            quote_char,
            field_delimiter,
            escape_char,
            double_quote_escape_allowed,
            transition_table: [[CsvState::Invalid; 256]; 6],
        };
        validator.build_transition_table();
        validator
    }

    fn build_transition_table(&mut self) {
        // FieldStart transitions.
        self.transition_table[CsvState::FieldStart as usize] = [CsvState::UnquotedField; 256];
        self.transition_table[CsvState::FieldStart as usize][NEWLINE as usize] =
            CsvState::RecordEnd;
        self.transition_table[CsvState::FieldStart as usize][self.quote_char as usize] =
            CsvState::QuotedField;
        self.transition_table[CsvState::FieldStart as usize][self.field_delimiter as usize] =
            CsvState::FieldStart;

        // UnquotedField transitions.
        self.transition_table[CsvState::UnquotedField as usize] = [CsvState::UnquotedField; 256];
        self.transition_table[CsvState::UnquotedField as usize][NEWLINE as usize] =
            CsvState::RecordEnd;
        self.transition_table[CsvState::UnquotedField as usize][self.field_delimiter as usize] =
            CsvState::FieldStart;

        // QuotedField transitions.
        self.transition_table[CsvState::QuotedField as usize] = [CsvState::QuotedField; 256];
        if let Some(escape_char) = self.escape_char {
            self.transition_table[CsvState::QuotedField as usize][escape_char as usize] =
                CsvState::Escape;
        }
        // The quote char transition must be defined after the escape transition, because the most common
        // escape char in CSV is the quote char itself ("._.)
        self.transition_table[CsvState::QuotedField as usize][self.quote_char as usize] =
            CsvState::Unquote;

        // Unquote transitions.
        self.transition_table[CsvState::Unquote as usize][NEWLINE as usize] = CsvState::RecordEnd;
        self.transition_table[CsvState::Unquote as usize][self.field_delimiter as usize] =
            CsvState::FieldStart;
        if let Some(escape_char) = self.escape_char
            && escape_char == self.quote_char
            && (self.quote_char != DOUBLE_QUOTE || self.double_quote_escape_allowed)
        {
            self.transition_table[CsvState::Unquote as usize][self.quote_char as usize] =
                CsvState::QuotedField;
        }

        // Escape transitions.
        self.transition_table[CsvState::Escape as usize] = [CsvState::QuotedField; 256];
    }

    fn reset(&mut self) {
        self.num_fields_seen = 1;
        self.state = CsvState::FieldStart;
    }

    fn validate_record<'a>(&mut self, iter: &mut impl Iterator<Item = &'a u8>) -> Option<bool> {
        for &byte in iter {
            let next_state = self.transition_table[self.state as usize][byte as usize];

            match next_state {
                CsvState::FieldStart => {
                    self.num_fields_seen += 1;
                    if self.num_fields_seen > self.num_fields {
                        return Some(false);
                    }
                }
                CsvState::RecordEnd => {
                    return Some(self.num_fields_seen == self.num_fields);
                }
                CsvState::Invalid => return Some(false),
                _ => {}
            }

            self.state = next_state;
        }

        None
    }
}

/// Helper function that takes in a BufferSource, calls parse_csv() to extract table values from
/// the buffer source, then streams the results to `sender`.
#[allow(clippy::too_many_arguments)]
fn dispatch_to_parse_csv<R>(
    has_header: bool,
    parse_options: CsvParseOptions,
    buffer_source: R,
    projection_indices: Arc<Vec<usize>>,
    fields: Vec<Field>,
    read_daft_fields: Arc<Vec<Arc<daft_core::datatypes::Field>>>,
    read_schema: Arc<Schema>,
    csv_buffer: &mut CsvBuffer,
    include_columns: Option<Vec<String>>,
    predicate: Option<Arc<Expr>>,
    sender: crossbeam_channel::Sender<Result<Table, DaftError>>,
    rows_read: Arc<AtomicUsize>,
) where
    R: std::io::Read,
{
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
                let _ = sender.send(Ok(table));
                rows_read.fetch_add(table_len, Ordering::SeqCst);
            }
        }
        Err(e) => {
            let _ = sender.send(Err(e));
        }
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
    csv_buffer: &mut CsvBuffer,
    include_columns: Option<Vec<String>>,
    predicate: Option<Arc<Expr>>,
) -> DaftResult<Vec<Table>>
where
    R: std::io::Read,
{
    let mut tables = vec![];
    loop {
        let (rows_read, has_more) = local_read_rows(&mut reader, csv_buffer.buffer.as_mut_slice())
            .context(ArrowSnafu {})?;
        let chunk = projection_indices
            .par_iter()
            .enumerate()
            .map(|(i, proj_idx)| {
                let deserialized_col = deserialize_column(
                    &csv_buffer.buffer[0..rows_read],
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
    Ok(tables)
}
