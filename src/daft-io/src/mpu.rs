use std::{
    borrow::Cow,
    io::Write,
    num::{NonZeroI32, NonZeroUsize},
    sync::Arc,
};

use tokio::{sync::mpsc::Sender, task::JoinSet};

use crate::{
    Error::InvalidArgument,
    object_io::{CompletedPart, MultipartObjectSource},
    utils::parse_object_url,
};

/// ObjectMultipartWriter is responsible for managing multipart uploads to object store.
///
/// It handles the creation of the multipart upload, writing individual parts to object store and
/// also completing the multipart upload once all parts have been uploaded.
///
/// It uses a semaphore to limit upload concurrency (and therefore memory utilization associated
/// with the part data).
pub struct ObjectMultipartWriter {
    /// The URI of the object to write to.
    uri: Cow<'static, str>,

    /// The bucket and key of the object to write to.
    bucket: Cow<'static, str>,

    /// The key of the object to write to.
    key: Cow<'static, str>,

    /// The upload ID of the object multipart upload. This is used to identify the multipart upload
    /// to object store.
    upload_id: Cow<'static, str>,

    /// Handles for the parts being uploaded.
    in_progress_uploads: JoinSet<super::Result<CompletedPart>>,

    /// Stores the next part number for multipart upload. See [`generate_part_number`] for a
    /// convenience method to generate the next part number.
    next_part_number: NonZeroI32,

    /// The object client used to perform the multipart upload operations.
    source_client: Arc<dyn MultipartObjectSource>,

    /// Semaphore to limit the number of concurrent in-flight uploads.
    in_flight_upload_permits: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Debug for ObjectMultipartWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectMultipartWriter")
            .field("uri", &self.uri)
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("upload_id", &self.upload_id)
            .field("next_part_number", &self.next_part_number)
            .field("source_client", &self.source_client.source_type())
            .field(
                "in_progress_uploads",
                &format!("JoinSet with {} tasks", self.in_progress_uploads.len()),
            )
            .field("in_flight_upload_permits", &self.in_flight_upload_permits)
            .finish()
    }
}

impl ObjectMultipartWriter {
    const MINIMUM_PART_SIZE: usize = 5 * 1024 * 1024; // 5 Mebibytes
    const MAXIMUM_PART_SIZE: usize = 5 * 1024 * 1024 * 1024; // 5 Gibibytes
    const MAX_PART_COUNT: i32 = 10000; // Max parts in a multipart upload

    /// Ensure that the part size is within the valid range for object multipart uploads.
    /// This function checks that the part size is at least 5 MiB and at most 5 GiB.
    fn validate_part_size(part_size: NonZeroUsize) -> super::Result<()> {
        if part_size.get() > Self::MAXIMUM_PART_SIZE {
            return Err(InvalidArgument {
                msg: format!(
                    "Part size must be less than or equal to {} bytes",
                    Self::MAXIMUM_PART_SIZE
                ),
            });
        }
        if part_size.get() < Self::MINIMUM_PART_SIZE {
            return Err(InvalidArgument {
                msg: format!(
                    "Part size must be greater than or equal to {} bytes",
                    Self::MINIMUM_PART_SIZE
                ),
            });
        }
        Ok(())
    }

    /// Creates a new ObjectMultipartWriter for the specified URI, part size, and maximum concurrent uploads.
    ///
    /// This kicks off the multipart upload process by creating a new multipart upload on object store.
    /// The returned ObjectMultipartWriter can then be used to write parts to the upload. After all parts
    /// are written, `shutdown()` must be called to finalize the upload.
    pub async fn create(
        uri: impl Into<String>,
        mpu_client: Arc<dyn MultipartObjectSource>,
    ) -> super::Result<Self> {
        let uri = uri.into();
        let (_scheme, bucket, key) = parse_object_url(&uri)?;

        if key.is_empty() {
            return Err(crate::Error::NotAFile { path: uri.clone() }.into());
        }

        let part_size = NonZeroUsize::new(mpu_client.part_size())
            .expect("Object multipart part size must be non-zero");
        Self::validate_part_size(part_size)?;
        log::debug!("Object multipart upload requested: {uri}, part_size: {part_size}");

        let upload_info = mpu_client.create_multipart(&bucket, &key).await?;
        log::debug!(
            "A multipart upload has been assigned an upload_id: {uri}, upload_id: {}",
            upload_info.upload_id
        );

        let max_concurrent_uploads = NonZeroUsize::new(mpu_client.max_concurrent_uploads())
            .expect("multipart concurrent uploads per object must be non-zero");
        Ok(Self {
            uri: uri.into(),
            bucket: bucket.into(),
            key: key.into(),
            upload_id: upload_info.upload_id.into(),
            source_client: mpu_client,
            next_part_number: unsafe { NonZeroI32::new_unchecked(1) },
            in_progress_uploads: JoinSet::new(),
            in_flight_upload_permits: Arc::new(tokio::sync::Semaphore::new(
                max_concurrent_uploads.get(),
            )),
        })
    }
}

impl ObjectMultipartWriter {
    /// Generates the next part number for the multipart upload.
    ///
    /// Panics if the next part number exceeds the maximum part count of 10,000.
    fn generate_part_number(&mut self) -> NonZeroI32 {
        let part_number = self.next_part_number;
        self.next_part_number = NonZeroI32::new(part_number.get() + 1).unwrap();
        assert!(
            self.next_part_number.get() <= Self::MAX_PART_COUNT,
            "Maximum part count exceeded"
        );
        part_number
    }

    /// Writes a chunk of data to the object multipart upload.
    ///
    /// The part size is expected to be the same as the one specified during the creation of the
    /// ObjectMultipartWriter. If the part size is different, it will panic.
    ///
    /// A new part number is generated for the part and a new task is spawned to upload the part
    /// in the background.
    pub async fn write_part(&mut self, chunk: bytes::Bytes) -> super::Result<()> {
        // Create an async task to upload the part.
        let data_len = chunk.len();

        let next_part_number = self.generate_part_number();
        let upload_id = self.upload_id.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let mpu_client = self.source_client.clone();

        log::debug!(
            "Object multipart upload part requested: {next_part_number}, size: {data_len} bytes"
        );
        let upload_permit = self.in_flight_upload_permits.clone().acquire_owned().await;
        log::debug!(
            "Object multipart upload part permit acquired: {next_part_number}, size: {data_len} bytes"
        );

        let upload_future = async move {
            let output = mpu_client
                .upload_multipart(&bucket, &key, &upload_id, next_part_number, chunk)
                .await?;

            drop(upload_permit);

            log::debug!(
                "Object multipart upload part has been completed: {next_part_number}, size: {data_len} bytes"
            );
            Ok(output)
        };

        // Spawn the upload task and add it to the in-progress uploads.
        self.in_progress_uploads.spawn(upload_future);
        Ok(())
    }

    pub async fn shutdown(&mut self) -> super::Result<()> {
        // Wait for all in-progress uploads to complete.
        let mut completed_parts = vec![];

        while let Some(upload) = self.in_progress_uploads.join_next().await {
            match upload {
                Ok(Ok(part)) => completed_parts.push(part),
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(super::Error::JoinError { source: err }),
            }
        }

        log::debug!(
            "Finalizing multipart upload with {} parts.",
            completed_parts.len()
        );

        // Ensure that completed parts are sorted by in ascending order by part number - else object store
        // might reject the completion request.
        completed_parts.sort_by_key(|part| part.part_number);

        // Complete the multipart upload with the completed parts.
        self.source_client
            .complete_multipart(
                &self.key.clone(),
                &self.bucket.clone(),
                &self.upload_id.clone(),
                completed_parts,
            )
            .await?;

        log::debug!("Object multipart upload completed: {}", self.uri);
        Ok(())
    }
}

pub struct ObjectPartBuffer {
    buffer: Vec<u8>,
    part_size: NonZeroUsize,
    tx: Option<Sender<bytes::Bytes>>,
}

impl ObjectPartBuffer {
    pub fn new(part_size: NonZeroUsize, tx: Sender<bytes::Bytes>) -> Self {
        Self {
            buffer: Vec::with_capacity(part_size.get()),
            part_size,
            tx: Some(tx),
        }
    }

    pub fn shutdown(&mut self) -> std::io::Result<()> {
        log::debug!("Shutting down object parts buffer.");

        if !self.buffer.is_empty() {
            log::debug!(
                "object parts buffer has {} bytes remaining to send.",
                self.buffer.len()
            );

            // If there is any remaining data in the buffer, send it as a final part
            let old_buffer =
                std::mem::replace(&mut self.buffer, Vec::with_capacity(self.part_size.get()));
            let new_part = bytes::Bytes::from(old_buffer);

            if let Some(tx) = &self.tx {
                // Attempt to send the final part
                tx.blocking_send(new_part)
                    .map_err(|_| std::io::Error::other("Failed to send final part for multi-part upload. Has the receiver been dropped?"))?;
            } else {
                panic!(
                    "It seems that the ObjectPartBuffer has been shutdown already, but we still have data to send. This is a bug in the code."
                );
            }
        }
        self.tx.take();
        Ok(())
    }
}

impl Write for ObjectPartBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut remaining = buf;
        let mut total_written_bytes = 0;

        while !remaining.is_empty() {
            // Write the buffer until its full, or we run out of data
            let available_space = self.part_size.get() - self.buffer.len();
            let writable_bytes = remaining.len().min(available_space);

            self.buffer.extend_from_slice(&remaining[..writable_bytes]);
            total_written_bytes += writable_bytes;
            remaining = &remaining[writable_bytes..];

            if self.buffer.len() == self.part_size.get() {
                log::debug!("Enough data to write a part to object store.");
                // Buffer is full, send it to the channel
                let old_buffer =
                    std::mem::replace(&mut self.buffer, Vec::with_capacity(self.part_size.get()));
                let new_part = bytes::Bytes::from(old_buffer);

                if let Some(tx) = &self.tx {
                    tx.blocking_send(new_part)
                        .map_err(|_| std::io::Error::other("Failed to send part for multi-part upload. Has the receiver been dropped?"))?;
                } else {
                    panic!(
                        "It seems that the ObjectPartBuffer has been shutdown already, but we still have data to send. This is a bug in the code."
                    );
                }
            }
        }

        Ok(total_written_bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // No-Op: Flushing a partial part will make the parts of unequal size. R2, for example,
        // requires all parts (except the last) to be the same size. The last part is always flushed
        // at shutdown().
        Ok(())
    }
}
