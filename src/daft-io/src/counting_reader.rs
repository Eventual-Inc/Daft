use std::{
    io::{Read, Seek},
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::AsyncRead;

use crate::IOStatsRef;

pub struct CountingReader<R> {
    reader: R,
    count: usize,
    io_stats: Option<IOStatsRef>,
}

impl<R> CountingReader<R> {
    pub fn new(reader: R, io_stats: Option<IOStatsRef>) -> Self {
        Self {
            reader,
            count: 0,
            io_stats,
        }
    }

    pub fn update_count(&mut self) {
        if let Some(ios) = &self.io_stats {
            ios.mark_bytes_read(self.count);
            self.count = 0;
        }
    }
}

impl<R> Read for CountingReader<R>
where
    R: Read + Seek,
{
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.reader.read(buf)?;
        self.count += read;
        Ok(read)
    }
    #[inline]
    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> std::io::Result<usize> {
        let read = self.reader.read_vectored(bufs)?;
        self.count += read;
        Ok(read)
    }
    #[inline]
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        let read = self.reader.read_to_end(buf)?;
        self.count += read;
        Ok(read)
    }
}

impl<R> Seek for CountingReader<R>
where
    R: Read + Seek,
{
    #[inline]
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.reader.seek(pos)
    }
}

impl<R> Drop for CountingReader<R> {
    fn drop(&mut self) {
        self.update_count();
    }
}

impl<R> AsyncRead for CountingReader<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before_len = buf.filled().len();
        match Pin::new(&mut self.reader).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let after_len = buf.filled().len();
                self.count += after_len - before_len;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}
