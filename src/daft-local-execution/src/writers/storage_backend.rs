use std::{
    io::{BufWriter, Write},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use daft_common::error::{DaftError, DaftResult};
use daft_common::runtime::{RuntimeTask, get_io_runtime};
use daft_io::{IOConfig, get_io_client, multipart::MultipartBuffer};
use parking_lot::Mutex;

/// A trait for storage backends. Currently only supports files and some object storages which support multipart upload as backends.
#[async_trait]
pub(crate) trait StorageBackend: Send + Sync + 'static {
    type Writer: Write + Send + Sync;

    /// Create the output buffer, the upstream(e.g. parquet writer) will write data to this buffer,
    /// and the writer will flush the buffer to destination storage.
    async fn create_writer(&mut self, filename: &Path) -> DaftResult<Self::Writer>;

    /// Finalize the write operation (close file, flush buffer and await upload to object storage, etc).
    async fn finalize(&mut self) -> DaftResult<()>;
}

pub(crate) struct FileStorageBackend {}

impl FileStorageBackend {
    // Buffer potentially small writes for highly compressed columns.
    const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024;
}

#[async_trait]
impl StorageBackend for FileStorageBackend {
    type Writer = BufWriter<std::fs::File>;

    async fn create_writer(&mut self, filename: &Path) -> DaftResult<Self::Writer> {
        // Create directories if they don't exist.
        if let Some(parent) = filename.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::File::create(filename)?;
        Ok(BufWriter::with_capacity(
            Self::DEFAULT_WRITE_BUFFER_SIZE,
            file,
        ))
    }
    async fn finalize(&mut self) -> DaftResult<()> {
        // Nothing needed for finalizing file storage.
        Ok(())
    }
}

/// A Send and Sync wrapper around MultipartBuffer.
pub(crate) struct SharedMultiPartBuffer {
    inner: Arc<Mutex<MultipartBuffer>>,
}

impl Write for SharedMultiPartBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.lock().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.lock().flush()
    }
}

pub(crate) struct ObjectStorageBackend {
    scheme: String,
    io_config: IOConfig,
    upload_task: Option<RuntimeTask<DaftResult<()>>>,
    writer_buffer: Option<Arc<Mutex<MultipartBuffer>>>,
}

impl ObjectStorageBackend {
    pub(crate) fn new(scheme: String, io_config: IOConfig) -> Self {
        Self {
            scheme,
            io_config,
            upload_task: None,
            writer_buffer: None,
        }
    }
}

#[async_trait]
impl StorageBackend for ObjectStorageBackend {
    type Writer = SharedMultiPartBuffer;

    async fn create_writer(&mut self, filename: &Path) -> DaftResult<Self::Writer> {
        let filename = filename.to_string_lossy().to_string();

        let io_config = Arc::new(self.io_config.clone());
        let io_client = get_io_client(true, io_config)?;

        let uri = format!("{}://{}", self.scheme, filename);
        let mut writer = match io_client
            .get_source(&uri)
            .await?
            .create_multipart_writer(&uri)
            .await?
        {
            Some(writer) => writer,
            None => {
                return Err(DaftError::InternalError(format!(
                    "The source {} does not support multipart upload",
                    self.scheme
                )));
            }
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let part_size = NonZeroUsize::new(writer.part_size())
            .expect("Object multipart part size must be non-zero");
        let multipart_buffer = Arc::new(Mutex::new(MultipartBuffer::new(part_size, tx)));

        let io_runtime = get_io_runtime(true);
        let background_task = io_runtime.spawn(async move {
            while let Some(part) = rx.recv().await {
                writer.put_part(part).await?;
            }
            writer.complete().await.map_err(|e| e.into())
        });
        self.upload_task = Some(background_task);
        self.writer_buffer = Some(multipart_buffer.clone());

        Ok(SharedMultiPartBuffer {
            inner: multipart_buffer,
        })
    }

    async fn finalize(&mut self) -> DaftResult<()> {
        let write_buffer = self
            .writer_buffer
            .take()
            .expect("Writer buffer must be initialized for multipart upload");

        let upload_task = self
            .upload_task
            .take()
            .expect("Upload thread must be initialized for multipart upload");

        let io_runtime = get_io_runtime(true);

        io_runtime
            .spawn_blocking(move || -> DaftResult<()> {
                // Close the MultiPartBuffer, this flushes any remaining data to object store as the final part.
                write_buffer.lock().shutdown()?;
                Ok(())
            })
            .await??;

        // Wait for the upload task to complete.
        upload_task.await?
    }
}

#[cfg(feature = "python")]
pub(crate) struct GravitinoStorageBackend;

#[cfg(feature = "python")]
impl GravitinoStorageBackend {
    pub(crate) fn parse_gravitino_url_and_config(
        root_dir: &str,
        io_config: IOConfig,
    ) -> DaftResult<(String, Option<IOConfig>)> {
        let root_dir_owned = root_dir.to_string();

        let runtime = get_io_runtime(true);

        runtime.block_within_async_context(async move {
            Self::parse_url_and_config(&root_dir_owned, io_config).await
        })?
    }

    async fn parse_url_and_config(
        root_dir: &str,
        io_config: IOConfig,
    ) -> DaftResult<(String, Option<IOConfig>)> {
        let io_client = get_io_client(true, Arc::new(io_config))?;
        let source = io_client.get_source(root_dir).await?;
        let any = source.as_any_arc();

        if let Ok(gravitino_source) = any.downcast::<daft_io::gravitino::GravitinoSource>() {
            let (source_path, io_config) =
                gravitino_source.resolve_url_and_config(root_dir).await?;
            let (_, target_root_dir) = daft_io::parse_url(&source_path)?;
            let target_url = url::Url::parse(target_root_dir.as_ref()).map_err(|_| {
                DaftError::InternalError(format!(
                    "Invalid target path: {}",
                    target_root_dir.as_ref()
                ))
            })?;
            let scheme = target_url.scheme();

            if scheme == "gvfs" {
                // Prevent circular nesting of gvfs protocol
                Err(DaftError::InternalError(format!(
                    "Resolved target path '{}' still uses the gvfs:// scheme, which would cause circular nesting",
                    target_root_dir.as_ref()
                )))
            } else {
                Ok((target_root_dir.to_string(), Some(io_config)))
            }
        } else {
            Err(DaftError::InternalError(format!(
                "The source {} does not support gvfs",
                root_dir
            )))
        }
    }
}
