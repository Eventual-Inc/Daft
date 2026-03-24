use std::{fs::File, sync::Arc};

use arrow_ipc::reader::{FileReader, StreamReader};
use common_error::{DaftError, DaftResult};
use daft_io::{GetResult, IOClient, IOStatsRef, LocalFile};
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, stream::BoxStream};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArrowIpcReadOptions {
    schema: SchemaRef,
    limit: Option<usize>,
}

impl ArrowIpcReadOptions {
    pub fn new(schema: SchemaRef, limit: Option<usize>) -> Self {
        Self { schema, limit }
    }
}

fn send_ipc_record_batches(
    reader: impl Iterator<
        Item = std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>,
    >,
    options: ArrowIpcReadOptions,
    tx: tokio::sync::mpsc::Sender<DaftResult<RecordBatch>>,
) -> DaftResult<()> {
    let mut limit = options.limit;
    for batch_result in reader {
        let arrow_batch = batch_result.map_err(DaftError::from)?;
        let mut table =
            RecordBatch::from_arrow(options.schema.clone(), arrow_batch.columns().to_vec())?;

        if let Some(lim) = limit {
            if table.len() >= lim {
                table = table.head(lim)?;
                let _ = tx.blocking_send(Ok(table));
                break;
            }
            limit = Some(lim - table.len());
        }
        if tx.blocking_send(Ok(table)).is_err() {
            break;
        }
    }
    Ok(())
}

/// Local file: prefer memory-efficient IPC reading — `FileReader` over `std::fs::File` walks the file
/// and decodes one record batch at a time without loading the whole file into RAM.
async fn read_from_local(
    local_file: LocalFile,
    options: ArrowIpcReadOptions,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    debug_assert!(
        local_file.range.is_none(),
        "Range is not supported for local files"
    );

    let path = local_file.path;
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let tx_err = tx.clone();

    tokio::task::spawn_blocking(move || {
        let inner = || -> DaftResult<()> {
            let file = File::open(&path)?;
            let mut reader = FileReader::try_new_buffered(file, None)?;
            unsafe {
                reader = reader.with_skip_validation(!cfg!(debug_assertions));
            }
            send_ipc_record_batches(reader, options, tx)?;
            Ok(())
        };

        if let Err(e) = inner() {
            let _ = tx_err.blocking_send(Err(e));
        }
    });

    Ok(ReceiverStream::new(rx).boxed())
}

async fn read_from_remote(
    _stream: BoxStream<'static, daft_io::Result<bytes::Bytes>>,
    _options: ArrowIpcReadOptions,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    todo!()
}

/// Read all record batches from an Arrow IPC file and yield them as a stream.
pub async fn stream_ipc_file(
    uri: String,
    options: ArrowIpcReadOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let get_result = io_client.single_url_get(uri, None, io_stats).await?;

    match get_result {
        GetResult::File(file) => read_from_local(file, options).await,
        GetResult::Stream(stream, ..) => read_from_remote(stream, options).await,
    }
}

pub async fn stream_ipc_stream(
    uri: String,
    options: ArrowIpcReadOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let get_result = io_client
        .single_url_get(uri.clone(), None, io_stats)
        .await?;

    let GetResult::File(file) = get_result else {
        return Err(DaftError::ValueError(format!(
            "Expected a file for Arrow IPC stream, got a stream for `{uri}`"
        )));
    };

    let path = file.path;
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let tx_err = tx.clone();

    tokio::task::spawn_blocking(move || {
        let inner = || -> DaftResult<()> {
            let file = File::open(&path)?;
            let mut reader = StreamReader::try_new_buffered(file, None)?;
            unsafe {
                reader = reader.with_skip_validation(!cfg!(debug_assertions));
            }
            send_ipc_record_batches(reader, options, tx)?;
            Ok(())
        };

        if let Err(e) = inner() {
            let _ = tx_err.blocking_send(Err(e));
        }
    });

    Ok(ReceiverStream::new(rx).boxed())
}
