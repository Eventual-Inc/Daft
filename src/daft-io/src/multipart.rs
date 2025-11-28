use std::{io::Write, num::NonZeroUsize};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::Sender;

/// A trait for writing data to object store via multipart upload.
/// The object won't be visible until the upload is completed.
#[async_trait]
pub trait MultipartWriter: Sync + Send {
    /// Returns the part size in bytes. Most object stores require all parts excluding the last
    /// one are the part size to be at least 5MB. And most object stores limit the max part number to be 10,000.
    /// So the user can adjust the part size to upload an object more than 50GB.
    fn part_size(&self) -> usize;

    /// Puts a part of the multipart data. The implementations may invoke this method multiple times.
    async fn put_part(&mut self, data: Bytes) -> super::Result<()>;

    /// Completes the multipart upload. The implementations need ensure all put_parts task are completed before complete the multipart upload.
    async fn complete(&mut self) -> super::Result<()>;
}

pub struct MultipartBuffer {
    buffer: Vec<u8>,
    part_size: NonZeroUsize,
    tx: Option<Sender<Bytes>>,
}

impl MultipartBuffer {
    pub fn new(part_size: NonZeroUsize, tx: Sender<Bytes>) -> Self {
        Self {
            buffer: Vec::with_capacity(part_size.get()),
            part_size,
            tx: Some(tx),
        }
    }

    pub fn shutdown(&mut self) -> std::io::Result<()> {
        log::debug!("Shutting down the parts buffer.");

        if !self.buffer.is_empty() {
            log::debug!(
                "Parts buffer has {} bytes remaining to send.",
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
                return Err(std::io::Error::other(
                    "It seems that the MultipartBuffer has been shutdown already, but we still have data to send. This is a bug in the code.",
                ));
            }
        }
        self.tx.take();
        Ok(())
    }
}

impl Write for MultipartBuffer {
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
                        "It seems that the PartBuffer has been shutdown already, but we still have data to send. This is a bug in the code."
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
