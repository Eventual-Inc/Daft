use std::io::{Seek, SeekFrom, Write};

type IOResult<T = (), E = std::io::Error> = std::result::Result<T, E>;

/// A wrapper of a writer that tracks the number of bytes successfully written.
pub struct CountingWriter<W> {
    inner: W,
    count: u64,
}

impl<W> CountingWriter<W> {
    /// The number of bytes successful written so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Extracts the inner writer, discarding this wrapper.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W> From<W> for CountingWriter<W> {
    fn from(inner: W) -> Self {
        Self { inner, count: 0 }
    }
}

impl<W: Write + std::fmt::Debug> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> IOResult<usize> {
        let written = self.inner.write(buf)?;
        self.count += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> IOResult {
        self.inner.flush()
    }
}

impl<W: Write + Seek> Seek for CountingWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> IOResult<u64> {
        self.inner.seek(pos)
    }
}
