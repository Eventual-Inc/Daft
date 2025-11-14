use std::{
    io::{BufWriter, Write},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use common_error::DaftResult;
use common_runtime::{RuntimeTask, get_io_runtime};
use daft_io::{
    IOConfig, MultipartObjectSource, ObjectMultipartWriter, ObjectPartBuffer, get_io_client,
};
use parking_lot::Mutex;

/// A trait for storage backends.
#[async_trait]
pub(crate) trait StorageBackend: Send + Sync + 'static {
    type Writer: Write + Send + Sync;

    /// Create the output buffer (buffered file writer, object buffer, etc).
    async fn create_writer(&mut self, filename: &Path) -> DaftResult<Self::Writer>;

    /// Finalize the write operation (close file, await upload to object, etc).
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

pub(crate) struct ObjectStorageBackend {
    scheme: String,
    io_config: IOConfig,
    object_client: Option<Arc<dyn MultipartObjectSource>>,
    part_buffer: Option<Arc<Mutex<ObjectPartBuffer>>>,
    upload_task: Option<RuntimeTask<DaftResult<()>>>,
}

impl ObjectStorageBackend {
    pub(crate) fn new(scheme: String, io_config: IOConfig) -> Self {
        Self {
            scheme,
            io_config,
            object_client: None,
            part_buffer: None,
            upload_task: None,
        }
    }
}

/// A Send and Sync wrapper around ObjectPartBuffer.
pub(crate) struct SharedPartBuffer {
    inner: Arc<Mutex<ObjectPartBuffer>>,
}

impl Write for SharedPartBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.lock().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.lock().flush()
    }
}

#[async_trait]
impl StorageBackend for ObjectStorageBackend {
    type Writer = SharedPartBuffer;

    async fn create_writer(&mut self, filename: &Path) -> DaftResult<Self::Writer> {
        let filename = filename.to_string_lossy().to_string();

        if self.object_client.is_none() {
            // Initialize object client if needed.
            let io_config = Arc::new(self.io_config.clone());
            let io_client = get_io_client(true, io_config)?;

            let source = io_client
                .get_source(&format!("{}://{}", self.scheme, filename))
                .await?;

            if let Some(multipart_source) = source.as_multipart_object_source() {
                self.object_client = Some(multipart_source);
            } else {
                return Err(common_error::DaftError::InternalError(format!(
                    "Source '{}' does not support multipart uploads.",
                    self.scheme
                )));
            }
        }

        let object_client = self
            .object_client
            .clone()
            .expect("Object client must be initialized");

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        // Create the object part buffer that interfaces with parquet-rs.
        let part_size = NonZeroUsize::new(object_client.part_size())
            .expect("Object multipart part size must be non-zero");
        let part_buffer = Arc::new(Mutex::new(ObjectPartBuffer::new(part_size, tx)));
        self.part_buffer = Some(part_buffer.clone());

        let uri = format!("{}://{}", self.scheme, filename);
        let mut multipart_writer = ObjectMultipartWriter::create(uri, object_client)
            .await
            .expect("Failed to create object multipart writer");

        // Spawn background upload thread.
        let io_runtime = get_io_runtime(true);
        let background_task = io_runtime.spawn(async move {
            while let Some(part) = rx.recv().await {
                multipart_writer.write_part(part).await?;
            }
            multipart_writer.shutdown().await.map_err(|e| e.into())
        });

        self.upload_task = Some(background_task);

        Ok(SharedPartBuffer { inner: part_buffer })
    }

    async fn finalize(&mut self) -> DaftResult<()> {
        let part_buffer = self
            .part_buffer
            .take()
            .expect("Object part buffer must be initialized for multipart upload");
        let upload_task = self
            .upload_task
            .take()
            .expect("Upload thread must be initialized for multipart upload");

        let io_runtime = get_io_runtime(true);

        io_runtime
            .spawn_blocking(move || -> DaftResult<()> {
                // Close the PartBuffer, this flushes any remaining data to object as the final part.
                part_buffer.lock().shutdown()?;
                Ok(())
            })
            .await??;

        // Wait for the upload task to complete.
        upload_task.await?
    }
}
