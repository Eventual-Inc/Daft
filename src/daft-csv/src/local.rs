use core::str;
use std::{io::Read, num::NonZeroUsize, sync::Arc};

use common_error::DaftResult;
use daft_arrow::{
    datatypes::Field,
    io::csv::{
        read::{Reader, ReaderBuilder},
        read_async::local_read_rows,
    },
};
use daft_core::{
    prelude::{Schema, Series},
    utils::arrow::cast_array_for_daft_if_needed,
};
use daft_decoding::deserialize::deserialize_column;
use daft_dsl::{Expr, expr::bound_expr::BoundExpr, optimization::get_required_columns};
use daft_io::{IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt};
use rayon::{
    iter::IndexedParallelIterator,
    prelude::{IntoParallelRefIterator, ParallelIterator},
};
use smallvec::SmallVec;
use snafu::ResultExt;

use crate::{
    ArrowSnafu, CsvConvertOptions, CsvParseOptions, CsvReadOptions, JoinSnafu,
    metadata::read_csv_schema_single,
    read::{fields_to_projection_indices, tables_concat},
};

mod pool;
use pool::{CsvBuffer, CsvBufferPool, FileSlab, FileSlabPool, SLABSIZE};

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
//               | 4MB Chunks         |                       | CSV Buffers        |
//               |┌───┐┌───┐┌───┐     |                       |┌───┐┌───┐┌───┐     |
//               ||   ||   ||   | ... |                       ||   ||   ||   | ... |
//               |└─┬─┘└─┬─┘└───┘     |                       |└─┬─┘└─┬─┘└───┘     |
//               └──┼────┼────────────┘                       └──┼────┼────────────┘
//                  |    |                                       |    |
//     ───────┐     |    |                                       |    |
//   /|       |     |    |                                       |    |
//  /─┘       |     |    |                                       |    |
// |          |     ▼    ▼                                       ▼    ▼
// |          |    ┌───┐ ┌───┐          ┌────┐  ┬--─┐           ┌───┐ ┌───┐               ┌───┐ ┌───┐
// |         ─┼───►|   | |   |  ──────► |   ┬┘┌─┘ ┬─┘  ───────► |   | |   |  ──────────►  |   | |   |
// | CSV File |    └───┘ └───┘          └───┴ └───┘             └───┘ └───┘               └───┘ └───┘
// |          |  Chain of slabs        Adjusted chunks     Vectors of ByteRecords     Stream of Daft tables
// |          |
// └──────────┘
//
//
// The above flow is concretely implemented via a series of iterators:
// 1. SlabIterator provides an iterator of slabs populated with file data.
// 2. ChunkyIterator takes the SlabIterator and provides an iterator of `Start`, `Continue`, and
//    `Final` chunks. Each chunk contains a reference to a file slab, and the start-end range of bytes of the
//    slab that it is valid for.
// 3. ChunkWindowIterator takes a ChunkyIterator, and creates windows of Start-Continue*-Final adjusted chunks.
//    An adjusted chunk (described in greater detail below), always starts with `ChunkState::Start`,
//    ends with `ChunkState::Final`, and has any number of `ChunkState::Continue` chunks in between.
//
// We take items off the ChunkWindowIterator, and pass them to a CSV decoder to be turned into Daft tables in parallel.
//
//
// What we call "Chunk Windows" are also known as "Adjusted chunks" in the literature, and refers to contiguous
// bytes that consist of complete CSV records. This contiguous bytes are typically made up of one or more file slabs.
// This is abstraction is needed for us to process file slabs in parallel without having to stitch together records that
// are split across multiple slabs. Here's a way to think about adjusted chunks:
//
// Given a starting position, we use our slab size to compute a preliminary start and stop
// position. For example, we can visualize all slabs in a file as follows.
//
// Slab 1          Slab 2          Slab 3               Slab N
// ┌──────────┐    ┌──────────┐    ┌──────────┐         ┌──────────┐
// |          |    |\n        |    |          |   \n    |       \n |
// |          |    |          |    |          |         |          |
// |          |    |          |    |          |         |          |
// |   \n     |    |          |    |          |         |          |
// |          |    |          |    |          |   ...   |     \n   |
// |          |    |          |    |          |         |          |
// | \n       |    |          |    |          |         |          |
// |          |    |          |    |          |         |  \n      |
// └──────────┘    └──────────┘    └──────────┘         └──────────┘
//
// However, record boundaries (i.e. the \n terminators) do not align nicely with the boundaries of slabs.
// So we create "adjusted chunks" from slabs as follows:
// 1. Find the first record terminator from a slabs's start. This is the starting position of the adjusted chunk.
// 2. Find the first record terminator from the next slabs's start. This is the ending positiono of the adjusted chunk.
// 3. If a given slab doesn't contain a record terminator (that we can find), then we add the current slab wholly into
//    the adjusted chunk, then repeat step 2. with the next slab.
//
// For example:
//
// Adjusted Chunk 1     Adj. Chunk 2                         Adj. Chunk N
// ┌──────────────────┐┌─────────────────────────────┐            ┌─┐
// |                \n||                           \n|         \n | |
// |          ┌───────┘|                       ┌─────┘            | |
// |          |    ┌───┘                       |         ┌────────┘ |
// |   \n     |    |                           |         |          |
// |          |    |                           |   ...   |     \n   |
// |          |    |                           |         |          |
// | \n       |    |                           |         |          |
// |          |    |                           |         |  \n      |
// └──────────┘    └───────────────────────────┘         └──────────┘
//
// Using this method, we now have adjusted chunks that are aligned with record boundaries, that do
// not overlap, and that fully cover every byte in the CSV file. Parsing each adjusted chunk can
// now happen in parallel.
//
// This is similar to the method described in:
// Ge, Chang et al. “Speculative Distributed CSV Data Parsing for Big Data Analytics.” Proceedings of the 2019 International Conference on Management of Data (2019).
//
// Another observation is that seeing a pure \n character is not necessarily indicative of a record
// terminator. We need to consider whether the \n character was seen within a quoted field, since the
// string "some text \n some text" is a valid CSV string field. To do this, we carry out the following
// algorithm:
// 1. Find a \n character.
// 2. Check if the CSV string immediately following this \n character is valid, i.e. does it parse
//    as valid CSV, and does it produce the same number of fields as our schema.
// 2a. If there is a valid record at this point, then we assume that the \n we saw was a valid terminator.
// 2b. If the record at this point is invalid, then this was likely a \n in a quoted field. Find the next
//     \n character and go back to 2.
// 2c. If we hit the end of a chunk without coming to a decision on whether we found a valid CSV record,
//     we simply move on to the next chunk of bytes and try to find a valid CSV record there. This is a
//     simplification that makes the implementation a lot easier to maintain.

// Default size for CSV buffers.
const DEFAULT_CSV_BUFFER_SIZE: usize = SLABSIZE; // 4MiB. Like SLABSIZE, this can be tuned.

/// Reads a single local CSV file in a non-streaming fashion.
pub async fn read_csv_local(
    uri: &str,
    convert_options: Option<CsvConvertOptions>,
    parse_options: CsvParseOptions,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<RecordBatch> {
    let stream = stream_csv_local(
        uri.to_string(),
        convert_options.clone(),
        parse_options.clone(),
        read_options,
        io_client.clone(),
        io_stats.clone(),
        max_chunks_in_flight,
    )
    .await?;
    let tables = Box::pin(stream);
    let collected_tables = tables
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
        return Ok(RecordBatch::empty(Some(Arc::new(schema.into()))));
    }
    let concated_table = tables_concat(collected_tables)?;

    // Apply head in case the last chunk went over limit.
    let limit = convert_options.as_ref().and_then(|opts| opts.limit);
    if let Some(limit) = limit
        && concated_table.len() > limit
    {
        concated_table.head(limit)
    } else {
        Ok(concated_table)
    }
}

/// Reads a single local CSV file in a streaming fashion.
pub async fn stream_csv_local(
    uri: String,
    convert_options: Option<CsvConvertOptions>,
    parse_options: CsvParseOptions,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<impl Stream<Item = DaftResult<RecordBatch>> + Send> {
    let uri = uri.trim_start_matches("file://");
    let file = std::fs::File::open(uri)?;

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
    let read_schema = Arc::new(Schema::new(fields_subset));
    let read_daft_fields = Arc::new(
        read_schema
            .into_iter()
            .cloned()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );

    // Create CSV buffer pool.
    let record_buffer_size = (estimated_mean_row_size + estimated_std_row_size).ceil() as usize;
    let chunk_size = read_options
        .as_ref()
        .and_then(|opt| opt.chunk_size.or_else(|| opt.buffer_size.map(|bs| bs / 8)))
        .unwrap_or(DEFAULT_CSV_BUFFER_SIZE);
    let chunk_size_rows = (chunk_size as f64 / record_buffer_size as f64).ceil() as usize;
    // TODO(desmond): We might consider creating per-process buffer pools and slab pools.
    let buffer_pool = Arc::new(CsvBufferPool::new(
        record_buffer_size,
        num_fields,
        chunk_size_rows,
    ));
    let n_threads: usize = std::thread::available_parallelism()
        .unwrap_or(NonZeroUsize::new(2).unwrap())
        .into();
    stream_csv_as_tables(
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
        max_chunks_in_flight.unwrap_or(n_threads),
    )
}

/// Helper function that reads up to 1 MiB of the CSV file to estimate stats and/or infer the schema of the file.
async fn get_schema_and_estimators(
    uri: &str,
    convert_options: &CsvConvertOptions,
    parse_options: &CsvParseOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<(daft_arrow::datatypes::Schema, f64, f64)> {
    let (inferred_schema, read_stats) = read_csv_schema_single(
        uri,
        parse_options.clone(),
        // Read at most 1 MiB to estimate stats.
        Some(1024 * 1024),
        io_client.clone(),
        io_stats.clone(),
    )
    .await?;

    #[allow(deprecated, reason = "arrow2 migration")]
    let mut schema = if let Some(schema) = convert_options.schema.clone() {
        schema.to_arrow2()?
    } else {
        inferred_schema.to_arrow2()?
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

/// An iterator of FileSlabs that takes in a File and FileSlabPool and yields FileSlabs
/// over the given file.
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

type SlabRow = (Arc<FileSlab>, usize);

impl Iterator for SlabIterator {
    type Item = SlabRow;
    fn next(&mut self) -> Option<Self::Item> {
        let slab = self.slabpool.get_slab();
        let bytes_read = {
            let mut guard = slab.write();
            let bytes_read = self.file.read(&mut guard.buffer).unwrap();
            if bytes_read == 0 {
                return None;
            }
            self.total_bytes_read += bytes_read;
            guard.valid_bytes = bytes_read;
            bytes_read
        };

        Some((slab, bytes_read))
    }
}

/// ChunkStates are a wrapper over slabs that dictate the position of a slab in a chunk window,
/// and which bytes of the slab should be used for parsing CSV records.
#[derive(Debug, Clone)]
enum ChunkState {
    // Represents the first chunk in a chunk window.
    Start {
        slab: Arc<FileSlab>,
        start: usize,
        end: usize,
    },
    // Represents any number of chunks between the Start and Final chunk in a chunk window.
    Continue {
        slab: Arc<FileSlab>,
        end: usize,
    },
    // Represents the last chunk in a chunk window.
    Final {
        slab: Arc<FileSlab>,
        end: usize,
        valid_bytes: usize,
    },
}

impl std::fmt::Display for ChunkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Start {
                slab: _,
                start,
                end,
            } => write!(f, "ChunkState::Start-{start}-{end}"),
            Self::Final {
                slab: _,
                end,
                valid_bytes,
            } => write!(f, "ChunkState::Final-{end}-{valid_bytes}"),
            Self::Continue { slab: _, end } => write!(f, "ChunkState::Continue-{end}"),
        }
    }
}

/// An iterator of ChunkStates that takes in a SlabIterator and yields Start, Continue, and Final
/// ChunkStates over the given slabs.
struct ChunkyIterator<I> {
    slab_iter: I,
    last_chunk: Option<ChunkState>,
    validator: CsvValidator,
}

impl<I> ChunkyIterator<I>
where
    I: Iterator<Item = SlabRow>,
{
    fn new(slab_iter: I, validator: CsvValidator) -> Self {
        Self {
            slab_iter,
            last_chunk: None,
            validator,
        }
    }
}

impl<I> Iterator for ChunkyIterator<I>
where
    I: Iterator<Item = SlabRow>,
{
    type Item = ChunkState;
    fn next(&mut self) -> Option<Self::Item> {
        let curr_chunk = match &self.last_chunk {
            Some(ChunkState::Start { .. } | ChunkState::Continue { .. }) => {
                if let Some((slab, valid_bytes)) = self.slab_iter.next() {
                    let mut curr_pos = 0;
                    let mut chunk_state: Option<ChunkState> = None;
                    while chunk_state.is_none()
                        && let Some(pos) = slab.find_first_newline_from(curr_pos)
                        && curr_pos < valid_bytes
                    {
                        let offset = curr_pos + pos;
                        let guard = slab.read();
                        chunk_state = match guard.validate_record(&mut self.validator, offset + 1) {
                            Some(true) => Some(ChunkState::Final {
                                slab: slab.clone(),
                                end: offset,
                                valid_bytes,
                            }),
                            None => Some(ChunkState::Continue {
                                slab: slab.clone(),
                                end: valid_bytes,
                            }),
                            Some(false) => {
                                curr_pos = offset + 1;
                                None
                            }
                        }
                    }
                    if let Some(chunk_state) = chunk_state {
                        Some(chunk_state)
                    } else {
                        Some(ChunkState::Continue {
                            slab: slab.clone(),
                            end: valid_bytes,
                        })
                    }
                } else {
                    None
                }
            }
            Some(ChunkState::Final {
                slab,
                end,
                valid_bytes,
            }) => Some(ChunkState::Start {
                slab: slab.clone(),
                start: end + 1,
                end: *valid_bytes,
            }),
            None => {
                if let Some((slab, valid_bytes)) = self.slab_iter.next() {
                    Some(ChunkState::Start {
                        slab,
                        start: 0,
                        end: valid_bytes,
                    })
                } else {
                    None
                }
            }
        };
        self.last_chunk.clone_from(&curr_chunk);
        curr_chunk
    }
}

/// An iterator of ChunkWindows that takes in aa ChunkyIterator and yields vectors of ChunkStates
/// that contain Start-Continue*-Final chunks that are valid for CSV parsing.
struct ChunkWindowIterator<I> {
    chunk_iter: I,
}

impl<I> ChunkWindowIterator<I> {
    fn new(chunk_iter: I) -> Self {
        Self { chunk_iter }
    }
}

impl<I> Iterator for ChunkWindowIterator<I>
where
    I: Iterator<Item = ChunkState>,
{
    type Item = SmallVec<[ChunkState; 2]>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut chunks = SmallVec::with_capacity(2);
        for chunk in self.chunk_iter.by_ref() {
            chunks.push(chunk);
            if let ChunkState::Final { .. } = chunks.last().expect("We just pushed a chunk") {
                break;
            }
        }
        if chunks.is_empty() {
            None
        } else {
            Some(chunks)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn stream_csv_as_tables(
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
    n_threads: usize,
) -> DaftResult<impl Stream<Item = DaftResult<RecordBatch>> + Send> {
    // Create a slab iterator over the file.
    let slabpool = FileSlabPool::new();
    let slab_iterator = SlabIterator::new(file, slabpool);

    // Create a chunk iterator over the slab iterator.
    let csv_validator = CsvValidator::new(
        num_fields,
        parse_options.quote,
        parse_options.delimiter,
        parse_options.escape_char,
        parse_options.double_quote,
    );
    let chunk_iterator = ChunkyIterator::new(slab_iterator, csv_validator);

    // Create a chunk window iterator over the chunk iterator.
    let chunk_window_iterator = ChunkWindowIterator::new(chunk_iterator);

    // Stream tables from each chunk window.
    let has_header = parse_options.has_header;
    let parse_options = Arc::new(parse_options);
    let stream = futures::stream::iter(chunk_window_iterator.enumerate())
        .map(move |(i, w)| {
            let has_header = has_header && (i == 0);
            let mut buffer = buffer_pool.get_buffer();
            let parse_options = parse_options.clone();
            let projection_indices = projection_indices.clone();
            let fields = fields.clone();
            let read_daft_fields = read_daft_fields.clone();
            let read_schema = read_schema.clone();
            let include_columns = include_columns.clone();
            let predicate = predicate.clone();
            tokio::spawn(async move {
                let (tx, rx) = tokio::sync::oneshot::channel();
                rayon::spawn(move || {
                    let reader = MultiSliceReader::new(&w);
                    let tables = collect_tables(
                        has_header,
                        &parse_options,
                        reader,
                        projection_indices,
                        fields,
                        read_daft_fields,
                        read_schema,
                        &mut buffer,
                        include_columns,
                        predicate,
                        limit,
                    );
                    // We throw away the error because we might close the oneshot channel in the case where
                    // a limit is applied and we early-terminate.
                    let _ = tx.send(tables);
                });
                rx.await
            })
        })
        .buffered(n_threads)
        .map(|v| {
            v.context(JoinSnafu {})?
                .context(super::OneShotRecvSnafu {})?
        });
    let flattened = stream
        .map(|result: DaftResult<Vec<RecordBatch>>| {
            let tables = result?;
            DaftResult::Ok(futures::stream::iter(tables.into_iter().map(Ok)))
        })
        .try_flatten();

    // Apply limit.
    let mut remaining_rows = limit.map(|limit| limit as i64);
    let limited = flattened.try_take_while(move |result| {
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
    });

    Ok(limited)
}

/// A helper struct that implements `std::io::Read` over a slice of ChunkStates.
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

impl Read for MultiSliceReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        let mut position = 0;
        while self.curr_read_idx < self.states.len() && position < buf_len {
            let state = &self.states[self.curr_read_idx];
            let (start, end, guard) = match state {
                ChunkState::Start { slab, start, end } => {
                    let guard = slab.read();
                    (*start, *end, guard)
                }
                ChunkState::Continue { slab, end } => {
                    let guard = slab.read();
                    (0, *end, guard)
                }
                ChunkState::Final { slab, end, .. } => {
                    let guard = slab.read();
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

// TODO(desmond): The Daft read_csv API does not currently accept non-\n record terminators. We should
// make this user configurable.
const CARRIAGE_RETURN: u8 = b'\r';
const NEWLINE: u8 = b'\n';
const DOUBLE_QUOTE: u8 = b'"';

/// State machine that validates CSV records.
struct CsvValidator {
    state: CsvState,
    num_fields: usize,
    num_fields_seen: usize,
    quote_char: u8,
    field_delimiter: u8,
    escape_char: u8,
    double_quote_escape_allowed: bool,
    // The transition table only needs to consider 256 possible inputs, because the only characters
    // that are valid for transitioning the table state are single-byte ASCII characters. Furthermore,
    // even when reading UTF-8, upon encountering a byte that matches the value for an ASCII character,
    // this byte will always correspond to the ASCII character.
    // For more details, see: https://en.wikipedia.org/wiki/UTF-8#Encoding
    transition_table: [[CsvState; 256]; 7],
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
    CarriageReturn,
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
        let escape_char = escape_char.unwrap_or(DOUBLE_QUOTE);
        let mut validator = Self {
            state: CsvState::FieldStart,
            num_fields,
            num_fields_seen: 0,
            quote_char,
            field_delimiter,
            escape_char,
            double_quote_escape_allowed,
            transition_table: [[CsvState::Invalid; 256]; 7],
        };
        validator.build_transition_table();
        validator
    }

    fn build_transition_table(&mut self) {
        // FieldStart transitions.
        self.transition_table[CsvState::FieldStart as usize] = [CsvState::UnquotedField; 256];
        self.transition_table[CsvState::FieldStart as usize][NEWLINE as usize] =
            CsvState::RecordEnd;
        self.transition_table[CsvState::FieldStart as usize][CARRIAGE_RETURN as usize] =
            CsvState::CarriageReturn;
        self.transition_table[CsvState::FieldStart as usize][self.quote_char as usize] =
            CsvState::QuotedField;
        self.transition_table[CsvState::FieldStart as usize][self.field_delimiter as usize] =
            CsvState::FieldStart;

        // UnquotedField transitions.
        self.transition_table[CsvState::UnquotedField as usize] = [CsvState::UnquotedField; 256];
        self.transition_table[CsvState::UnquotedField as usize][NEWLINE as usize] =
            CsvState::RecordEnd;
        self.transition_table[CsvState::UnquotedField as usize][CARRIAGE_RETURN as usize] =
            CsvState::CarriageReturn;
        self.transition_table[CsvState::UnquotedField as usize][self.field_delimiter as usize] =
            CsvState::FieldStart;

        // QuotedField transitions.
        self.transition_table[CsvState::QuotedField as usize] = [CsvState::QuotedField; 256];
        self.transition_table[CsvState::QuotedField as usize][self.escape_char as usize] =
            CsvState::Escape;
        // The quote char transition must be defined after the escape transition, because the most common
        // escape char in CSV is the quote char itself ("._.)
        self.transition_table[CsvState::QuotedField as usize][self.quote_char as usize] =
            CsvState::Unquote;

        // Unquote transitions.
        self.transition_table[CsvState::Unquote as usize][NEWLINE as usize] = CsvState::RecordEnd;
        self.transition_table[CsvState::Unquote as usize][CARRIAGE_RETURN as usize] =
            CsvState::CarriageReturn;
        self.transition_table[CsvState::Unquote as usize][self.field_delimiter as usize] =
            CsvState::FieldStart;

        if self.escape_char == self.quote_char
            && (self.quote_char != DOUBLE_QUOTE || self.double_quote_escape_allowed)
        {
            self.transition_table[CsvState::Unquote as usize][self.quote_char as usize] =
                CsvState::QuotedField;
        }

        // Escape transitions.
        self.transition_table[CsvState::Escape as usize] = [CsvState::QuotedField; 256];

        // CarriageReturn transitions.
        self.transition_table[CsvState::CarriageReturn as usize][NEWLINE as usize] =
            CsvState::RecordEnd;
    }

    fn validate_record<'a>(&mut self, iter: &mut impl Iterator<Item = &'a u8>) -> Option<bool> {
        // Reset state machine for each new validation attempt.
        self.state = CsvState::FieldStart;
        self.num_fields_seen = 1;
        // Start running the state machine against each byte.
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

/// Helper function that takes in a source of bytes, calls parse_csv() to extract table values from
/// the buffer source, then returns the vector of Daft tables.
#[allow(clippy::too_many_arguments)]
fn collect_tables<R>(
    has_header: bool,
    parse_options: &CsvParseOptions,
    byte_reader: R,
    projection_indices: Arc<Vec<usize>>,
    fields: Vec<Field>,
    read_daft_fields: Arc<Vec<Arc<daft_core::datatypes::Field>>>,
    read_schema: Arc<Schema>,
    csv_buffer: &mut CsvBuffer,
    include_columns: Option<Vec<String>>,
    predicate: Option<Arc<Expr>>,
    limit: Option<usize>,
) -> DaftResult<Vec<RecordBatch>>
where
    R: std::io::Read,
{
    let rdr = ReaderBuilder::new()
        .has_headers(has_header)
        .delimiter(parse_options.delimiter)
        .double_quote(parse_options.double_quote)
        .quote(parse_options.quote)
        .escape(parse_options.escape_char)
        .comment(parse_options.comment)
        .flexible(parse_options.allow_variable_columns)
        .from_reader(byte_reader);
    // The header should not count towards the limit.
    let limit = limit.map(|limit| limit + (has_header as usize));
    parse_csv_chunk(
        rdr,
        projection_indices,
        fields,
        read_daft_fields,
        read_schema,
        csv_buffer,
        include_columns,
        predicate,
        limit,
    )
}

/// Helper function that consumes a CSV reader and turns it into a vector of Daft tables.
#[allow(clippy::too_many_arguments)]
fn parse_csv_chunk<R>(
    mut reader: Reader<R>,
    projection_indices: Arc<Vec<usize>>,
    fields: Vec<daft_arrow::datatypes::Field>,
    read_daft_fields: Arc<Vec<Arc<daft_core::datatypes::Field>>>,
    read_schema: Arc<Schema>,
    csv_buffer: &mut CsvBuffer,
    include_columns: Option<Vec<String>>,
    predicate: Option<Arc<Expr>>,
    limit: Option<usize>,
) -> DaftResult<Vec<RecordBatch>>
where
    R: std::io::Read,
{
    let mut tables = vec![];
    let mut local_limit = limit;
    loop {
        let (rows_read, has_more) =
            local_read_rows(&mut reader, csv_buffer.buffer.as_mut_slice(), local_limit)
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
        let table = RecordBatch::new_unchecked(read_schema.clone(), chunk, num_rows);
        let table = if let Some(predicate) = &predicate {
            let predicate = BoundExpr::try_new(predicate.clone(), &read_schema)?;
            let filtered = table.filter(std::slice::from_ref(&predicate))?;
            if let Some(include_columns) = &include_columns {
                let include_column_indices = include_columns
                    .iter()
                    .map(|name| read_schema.get_index(name))
                    .collect::<DaftResult<Vec<_>>>()?;

                filtered.get_columns(&include_column_indices)
            } else {
                filtered
            }
        } else {
            table
        };
        // Check the local limit.
        let limit_reached = if let Some(local_limit) = &mut local_limit {
            *local_limit -= table.len();
            *local_limit == 0
        } else {
            false
        };
        tables.push(table);

        // The number of record might exceed the number of byte records we've allocated.
        // Retry until all byte records in this chunk are read.
        if !has_more || limit_reached {
            break;
        }
    }
    Ok(tables)
}
