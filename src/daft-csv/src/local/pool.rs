use core::mem::ManuallyDrop;
use std::pin::pin;
use futures::Stream;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc::{self, Receiver, Sender};

mod fixed_capacity_vec;

type SlabData = Vec<u8>;

/// A pool of reusable memory slabs for efficient I/O operations.
struct SlabPool {
    available_slabs_sender: Sender<SlabData>,
    available_slabs: Receiver<SlabData>,
}

impl SlabPool {
    /// Creates a new `SlabPool` with a specified number of slabs of a given size.
    fn new(slab_count: usize, slab_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(slab_count);
        for _ in 0..slab_count {
            tx.try_send(Vec::with_capacity(slab_size))
                .expect("Failed to send slab to pool");
        }
        Self {
            available_slabs: rx,
            available_slabs_sender: tx,
        }
    }

    /// Asynchronously retrieves the next available slab from the pool.
    async fn get_next_data(&mut self) -> Slab {
        let mut data = self
            .available_slabs
            .recv()
            .await
            .expect("Slab pool is empty");

        data.clear();

        Slab {
            send_back_to_pool: self.available_slabs_sender.clone(),
            data: ManuallyDrop::new(data),
        }
    }
}

/// Represents a single memory slab that can be returned to the pool when dropped.
#[derive(Debug)]
pub struct Slab {
    send_back_to_pool: Sender<SlabData>,
    data: ManuallyDrop<SlabData>,
}

impl std::ops::Deref for Slab {
    type Target = SlabData;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl std::ops::DerefMut for Slab {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

type SharedSlab = Arc<Slab>;

impl Drop for Slab {
    fn drop(&mut self) {
        let data = unsafe { ManuallyDrop::take(&mut self.data) };
        let _ = self.send_back_to_pool.try_send(data);
    }
}

use tokio_stream::wrappers::ReceiverStream;

/// Asynchronously reads slabs from a file and returns a stream of SharedSlabs.
pub fn read_slabs<R: AsyncRead + Unpin + Send + 'static>(
    mut file: R,
    buffer_size: usize,
    pool_size: usize,
) -> impl Stream<Item=Slab> {
    let (tx, rx) = mpsc::channel::<Slab>(pool_size);
    let pool = SlabPool::new(pool_size, buffer_size);
    tokio::spawn(async move {
        let mut pool = pool;
        loop {
            let mut slab = pool.get_next_data().await;

            // note: might not be exactly 4MiB
            let mut total_read = 0;
            while total_read < slab.capacity() {
                let result = file
                    .read_buf(&mut *slab)
                    .await
                    .expect("Failed to read from file");

                if result == 0 {
                    // End of file reached
                    break;
                }

                total_read += result;

                // If we're close to filling the buffer, stop reading
                if slab.capacity() - total_read < 1024 {
                    break;
                }
            }

            if total_read == 0 {
                // No data read, end of file
                break;
            }

            // Update the length of the slab with the actual number of bytes read
            debug_assert_eq!(total_read, slab.len(), "Slab length should be equal to the number of bytes read");
            tx.send(slab).await.expect("Failed to send slab to stream");
        }
    });

    ReceiverStream::new(rx)
}

use crate::local::pool::fixed_capacity_vec::FixedCapacityVec;
use futures::stream::StreamExt;

pub type WindowedSlab = heapless::Vec<SharedSlab, 2>;

/// Asynchronously reads slabs from a file and returns a stream of WindowedSlabs.
///
/// This function creates a windowed view of the slabs, where each `WindowedSlab`
/// contains two consecutive slabs. The windowing is done in an overlapping manner,
/// so the second slab of the previous window becomes the first slab of the next window.
///
/// # Arguments
///
/// * `file` - The file to read from.
/// * `buffer_size` - The size of each slab's buffer.
/// * `pool_size` - The size of the slab pool.
///
/// # Returns
///
/// A `Stream` of `WindowedSlab`s.
pub fn read_slabs_windowed<R: AsyncRead + Unpin + Send + 'static>(
    file: R,
    buffer_size: usize,
    pool_size: usize,
) -> impl Stream<Item=WindowedSlab> {
    let mut slab_stream = read_slabs(file, buffer_size, pool_size);

    use tokio_stream::StreamExt;
    use tokio::sync::mpsc;

    let (tx, rx) = mpsc::channel(pool_size);

    tokio::spawn(async move {
        let mut slab_stream = pin!(slab_stream);

        let mut windowed_slab = heapless::Vec::<SharedSlab, 2>::new();

        let mut slab_stream = slab_stream.as_mut();
        while let Some(slab) = StreamExt::next(&mut slab_stream).await {
            let slab = SharedSlab::from(slab);

            windowed_slab.push(slab).unwrap();

            if windowed_slab.len() == 2 {
                tx.send(windowed_slab.clone()).await.unwrap();
                windowed_slab.remove(0);
            }
        }

        // Send the last windowed slab
        if !windowed_slab.is_empty() {
            tx.send(windowed_slab).await.unwrap();
        }
    });

    ReceiverStream::new(rx)
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_read_slabs() {
        let data = b"Hello, World!".repeat(1000);
        let data_len = data.len();
        let cursor = Cursor::new(data);
        let buffer_size = 100;
        let pool_size = 5;

        let mut stream = read_slabs(cursor, buffer_size, pool_size).await;
        let mut total_bytes = 0;

        while let Some(slab) = stream.next().await {
            assert!(slab.len() <= buffer_size);
            total_bytes += slab.len();
        }

        assert_eq!(total_bytes, data_len);
    }

    #[tokio::test]
    async fn test_read_slabs_windowed() {
        let data = b"Hello, World!".repeat(1000);
        let data_len = data.len();
        let cursor = Cursor::new(data);
        let buffer_size = 100;
        let pool_size = 5;

        let mut stream = read_slabs_windowed(cursor, buffer_size, pool_size).await;
        let mut total_bytes = 0;
        let mut previous_slab: Option<SharedSlab> = None;

        let left_total = 0;
        let right_total = 0;

        while let Some(windowed_slab) = stream.next().await {
            assert_eq!(windowed_slab.len(), 2);

            if let Some(prev) = previous_slab {
                assert!(Arc::ptr_eq(&prev, &windowed_slab[0]));
            }

            left_total += windowed_slab[0].len();
            right_total += windowed_slab[1].len();
            total_bytes += windowed_slab[1].len();
            previous_slab = Some(windowed_slab[1].clone());
        }

        assert_eq!(total_bytes, data_len);
        assert_eq!(left_total, right_total);
        assert_eq!(left_total, data_len);
    }
}

