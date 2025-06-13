use std::{
    io::{BufWriter, Write},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use common_error::DaftResult;
use common_runtime::{get_io_runtime, RuntimeTask};
use daft_io::{get_io_client, IOConfig, S3LikeSource, S3MultipartWriter, S3PartBuffer};
use parking_lot::Mutex;

/// A trait for storage backends. Currently only supports files and S3 as backends.
#[async_trait]
pub(crate) trait StorageBackend: Send + Sync + 'static {
    type Writer: Write + Send + Sync;

    /// Create the output buffer (buffered file writer, S3 buffer, etc).
    async fn create_writer(&mut self, filename: &Path) -> DaftResult<Self::Writer>;

    /// Finalize the write operation (close file, await upload to S3, etc).
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

pub(crate) struct S3StorageBackend {
    scheme: String,
    io_config: IOConfig,
    s3_client: Option<Arc<S3LikeSource>>,
    s3part_buffer: Option<Arc<Mutex<S3PartBuffer>>>,
    upload_task: Option<RuntimeTask<DaftResult<()>>>,
}

impl S3StorageBackend {
    const S3_MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024; // 8 MB
    const S3_MULTIPART_MAX_CONCURRENT_UPLOADS_PER_OBJECT: usize = 100; // 100 uploads per S3 object

    pub(crate) fn new(scheme: String, io_config: IOConfig) -> Self {
        Self {
            scheme,
            io_config,
            s3_client: None,
            s3part_buffer: None,
            upload_task: None,
        }
    }
}

/// A Send and Sync wrapper around S3PartBuffer.
pub(crate) struct SharedS3PartBuffer {
    inner: Arc<Mutex<S3PartBuffer>>,
}

impl Write for SharedS3PartBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.lock().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.lock().flush()
    }
}

#[async_trait]
impl StorageBackend for S3StorageBackend {
    type Writer = SharedS3PartBuffer;

    async fn create_writer(&mut self, filename: &Path) -> DaftResult<Self::Writer> {
        let filename = filename.to_string_lossy().to_string();
        let part_size = NonZeroUsize::new(Self::S3_MULTIPART_PART_SIZE)
            .expect("S3 multipart part size must be non-zero");
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        // Create the S3 part buffer that interfaces with parquet-rs.
        let s3part_buffer = Arc::new(Mutex::new(S3PartBuffer::new(part_size, tx)));
        self.s3part_buffer = Some(s3part_buffer.clone());

        if self.s3_client.is_none() {
            // Initialize S3 client if needed.
            let io_config = Arc::new(self.io_config.clone());

            let io_client = get_io_client(true, io_config)?;
            let s3_client = io_client
                .get_source(&format!("s3://{}", filename))
                .await?
                .as_any_arc()
                .downcast()
                .unwrap();
            self.s3_client = Some(s3_client);
        }

        // Spawn background upload thread.
        let s3_client = self
            .s3_client
            .clone()
            .expect("S3 client must be initialized");
        let uri = format!("{}://{}", self.scheme, filename);

        let io_runtime = get_io_runtime(true);

        let mut s3_multipart_writer = S3MultipartWriter::create(
            uri,
            part_size,
            NonZeroUsize::new(Self::S3_MULTIPART_MAX_CONCURRENT_UPLOADS_PER_OBJECT)
                .expect("S3 multipart concurrent uploads per object must be non-zero"),
            s3_client,
        )
        .await
        .expect("Failed to create S3 multipart writer");

        let background_task = io_runtime.spawn(async move {
            while let Some(part) = rx.recv().await {
                s3_multipart_writer.write_part(part).await?;
            }
            s3_multipart_writer.shutdown().await.map_err(|e| e.into())
        });

        self.upload_task = Some(background_task);

        Ok(SharedS3PartBuffer {
            inner: s3part_buffer,
        })
    }

    async fn finalize(&mut self) -> DaftResult<()> {
        let part_buffer = self
            .s3part_buffer
            .take()
            .expect("S3 part buffer must be initialized for multipart upload");
        let upload_task = self
            .upload_task
            .take()
            .expect("Upload thread must be initialized for multipart upload");

        let io_runtime = get_io_runtime(true);

        io_runtime
            .spawn_blocking(move || -> DaftResult<()> {
                // Close the S3PartBuffer, this flushes any remaining data to S3 as the final part.
                part_buffer.lock().shutdown()?;
                Ok(())
            })
            .await??;

        // Wait for the upload task to complete.
        upload_task.await?
    }
}
