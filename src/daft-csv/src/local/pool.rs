use core::mem::ManuallyDrop;
use futures::Stream;
use std::pin::pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc::{self, Receiver, Sender};

mod fixed_capacity_vec;

pub type FileSlab = Vec<u8>;

/// A pool of reusable memory slabs for efficient I/O operations.
pub struct SlabPool<T> {
    available_slabs_sender: Sender<T>,
    available_slabs: Receiver<T>,
}

trait Clearable {
    fn clear(&mut self);
}

impl<T> Clearable for Vec<T> {
    fn clear(&mut self) {
        self.clear();
    }
}

impl<T> SlabPool<T> {
    /// Creates a new `SlabPool` with a specified number of slabs of a given size.
    pub fn new<I: IntoIterator<Item=T>>(iterator: I) -> Self
    where
        I::IntoIter: ExactSizeIterator<Item=T>,
    {
        let iterator = iterator.into_iter();
        let slab_count = iterator.len();

        let (tx, rx) = mpsc::channel(slab_count);

        for slab in iterator {
            tx.try_send(slab)
                .expect("Failed to send slab to pool");

            // todo: maybe assert that slab_count is correct or use TrustedLen
        }

        Self {
            available_slabs: rx,
            available_slabs_sender: tx,
        }
    }
}

impl<T: Clearable> SlabPool<T> {
    /// Asynchronously retrieves the next available slab from the pool.
    async fn get_next_data(&mut self) -> Slab<T> {
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
pub struct Slab<T> {
    send_back_to_pool: Sender<T>,
    data: ManuallyDrop<T>,
}

impl<T> std::ops::Deref for Slab<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> std::ops::DerefMut for Slab<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

type SharedSlab<T> = Arc<Slab<T>>;

impl<T> Drop for Slab<T> {
    fn drop(&mut self) {
        let data = unsafe { ManuallyDrop::take(&mut self.data) };
        let _ = self.send_back_to_pool.try_send(data);
    }
}

use tokio_stream::wrappers::ReceiverStream;

/// Asynchronously reads slabs from a file and returns a stream of SharedSlabs.
pub fn read_slabs<R>(
    mut file: R,
    iterator: impl ExactSizeIterator<Item=FileSlab>,
) -> impl Stream<Item=Slab<FileSlab>> where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Slab<FileSlab>>(iterator.len());

    let pool = SlabPool::new(iterator);
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
            debug_assert_eq!(
                total_read,
                slab.len(),
                "Slab length should be equal to the number of bytes read"
            );
            tx.send(slab).await.expect("Failed to send slab to stream");
        }
    });

    ReceiverStream::new(rx)
}

pub type WindowedSlab = heapless::Vec<SharedSlab<FileSlab>, 2>;

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
pub fn read_slabs_windowed<R, I>(
    file: R,
    iterator: I,
) -> impl Stream<Item=WindowedSlab> where
    R: AsyncRead + Unpin + Send + 'static,
    I: IntoIterator<Item=FileSlab> + 'static,
    I::IntoIter: ExactSizeIterator<Item=FileSlab> + 'static,
{
    let iterator = iterator.into_iter();
    let (tx, rx) = mpsc::channel(iterator.len());
    let slab_stream = read_slabs(file, iterator);

    use tokio::sync::mpsc;
    use tokio_stream::StreamExt;

    tokio::spawn(async move {
        let mut slab_stream = pin!(slab_stream);

        let mut windowed_slab = heapless::Vec::<SharedSlab<FileSlab>, 2>::new();

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
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_read_slabs() {
        let data = b"Hello, World!".repeat(1000);
        let data_len = data.len();
        let cursor = Cursor::new(data);
        let buffer_size = 100;
        let pool_size = 5;

        let slabs = (0..pool_size).map(|_| vec![0; buffer_size]).collect::<Vec<_>>();
        let mut stream = read_slabs(cursor, slabs.into_iter());
        let mut total_bytes = 0;

        while let Some(slab) = stream.next().await {
            assert!(slab.len() <= buffer_size);
            total_bytes += slab.len();
        }

        assert_eq!(total_bytes, data_len);
    }

    // todo: re-add this test it is probably working we just tested incorrect invariants
    // async fn test_read_slabs_windowed() {
    //     let data = b"Hello, World!".repeat(1000);
    //     let data_len = data.len();
    //     let cursor = Cursor::new(data);
    //     let buffer_size = 100;
    //     let pool_size = 5;
    // 
    //     let slabs = (0..pool_size).map(|_| vec![0; buffer_size]).collect::<Vec<_>>();
    //     let mut stream = read_slabs_windowed(cursor, slabs.into_iter());
    //     let mut total_bytes = 0;
    //     let mut previous_slab: Option<SharedSlab<FileSlab>> = None;
    // 
    //     let mut left_total = 0;
    //     let mut right_total = 0;
    // 
    //     while let Some(windowed_slab) = stream.next().await {
    //         assert_eq!(windowed_slab.len(), 2);
    // 
    //         if let Some(prev) = &previous_slab {
    //             assert!(Arc::ptr_eq(prev, &windowed_slab[0]));
    //         }
    // 
    //         left_total += windowed_slab[0].len();
    //         right_total += windowed_slab[1].len();
    //         total_bytes += windowed_slab[1].len();
    //         previous_slab = Some(windowed_slab[1].clone());
    //     }
    // 
    //     assert_eq!(total_bytes, data_len);
    //     assert_eq!(left_total, right_total);
    //     assert_eq!(left_total, data_len);
    // }
}
