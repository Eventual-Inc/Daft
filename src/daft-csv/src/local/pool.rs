use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
};

use daft_arrow::io::csv::read;
use parking_lot::{Mutex, RwLock};

// The default size of a slab used for reading CSV files in chunks. Currently set to 4 MiB. This can be tuned.
pub const SLABSIZE: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub struct CsvSlab(Vec<read::ByteRecord>);

impl CsvSlab {
    fn new(record_size: usize, num_fields: usize, num_rows: usize) -> Self {
        Self(vec![
            read::ByteRecord::with_capacity(record_size, num_fields);
            num_rows
        ])
    }
}

impl Deref for CsvSlab {
    type Target = Vec<read::ByteRecord>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CsvSlab {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A pool of ByteRecord slabs. Used for deserializing CSV.
#[derive(Debug)]
pub struct CsvBufferPool {
    buffers: Mutex<Vec<CsvSlab>>,
    record_size: usize,
    num_fields: usize,
    num_rows: usize,
}

/// A slab of ByteRecords. Used for deserializing CSV.
pub struct CsvBuffer {
    pub buffer: CsvSlab,
    pool: Weak<CsvBufferPool>,
}

impl CsvBufferPool {
    pub fn new(record_size: usize, num_fields: usize, num_rows: usize) -> Self {
        Self {
            // We start off with an empty pool. Buffers will be allocated on demand.
            buffers: Mutex::new(vec![]),
            record_size,
            num_fields,
            num_rows,
        }
    }

    pub fn get_buffer(self: &Arc<Self>) -> CsvBuffer {
        let buffer = {
            let mut buffers = self.buffers.lock();
            let buffer = buffers.pop();
            match buffer {
                Some(buffer) => buffer,
                None => CsvSlab::new(self.record_size, self.num_fields, self.num_rows),
            }
        };

        CsvBuffer {
            buffer,
            pool: Arc::downgrade(self),
        }
    }

    fn return_buffer(&self, buffer: CsvSlab) {
        let mut buffers = self.buffers.lock();
        buffers.push(buffer);
    }
}

impl Drop for CsvBuffer {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let buffer = std::mem::take(&mut self.buffer);
            pool.return_buffer(buffer);
        }
    }
}

/// A pool of slabs. Used for reading CSV files in SLABSIZE chunks.
#[derive(Debug)]
pub struct FileSlabPool {
    slabs: Mutex<Vec<RwLock<FileSlabState>>>,
}

impl FileSlabPool {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            // We start off with an empty pool. Slabs will be allocated on demand.
            slabs: Mutex::new(vec![]),
        })
    }

    pub fn get_slab(self: &Arc<Self>) -> Arc<FileSlab> {
        let slab = {
            let mut slabs = self.slabs.lock();
            let slab = slabs.pop();
            match slab {
                Some(slab) => slab,
                None => RwLock::new(FileSlabState::new(
                    unsafe { Box::new_uninit_slice(SLABSIZE).assume_init() },
                    0,
                )),
            }
        };

        Arc::new(FileSlab {
            state: slab,
            pool: Arc::downgrade(self),
        })
    }

    fn return_slab(&self, slab: RwLock<FileSlabState>) {
        let mut slabs = self.slabs.lock();
        slabs.push(slab);
    }
}

/// A slab of bytes. Used for reading CSV files in SLABSIZE chunks.
#[derive(Debug)]
pub struct FileSlab {
    state: RwLock<FileSlabState>,
    pool: Weak<FileSlabPool>,
}

impl FileSlab {
    /// Given an offset into a FileSlab, finds the first \n char found in the FileSlabState's buffer,
    /// then the returns the position relative to the given offset.
    pub fn find_first_newline_from(&self, offset: usize) -> Option<usize> {
        let guard = self.state.read();
        guard.find_first_newline_from(offset)
    }
}

impl Deref for FileSlab {
    type Target = RwLock<FileSlabState>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

// Modify the Drop method for FileSlabs so that their states are returned to their parent slab pool.
impl Drop for FileSlab {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let file_slab_state = std::mem::take(&mut self.state);
            pool.return_slab(file_slab_state);
        }
    }
}

#[derive(Debug, Default)]
pub struct FileSlabState {
    pub buffer: Box<[u8]>,
    pub valid_bytes: usize,
}

impl FileSlabState {
    fn new(buffer: Box<[u8]>, valid_bytes: usize) -> Self {
        Self {
            buffer,
            valid_bytes,
        }
    }

    /// Helper function that find the first \n char in the file slab state's buffer starting from `offset.`
    fn find_first_newline_from(&self, offset: usize) -> Option<usize> {
        newline_position(&self.buffer[offset..self.valid_bytes])
    }

    /// Validate the CSV record in the file slab state's buffer starting from `start`. `validator` is a
    /// state machine that might need to process multiple buffers to validate CSV records.
    pub fn validate_record(
        &self,
        validator: &mut super::CsvValidator,
        start: usize,
    ) -> Option<bool> {
        validator.validate_record(&mut self.buffer[start..self.valid_bytes].iter())
    }
}

// Daft does not currently support non-\n record terminators (e.g. carriage return \r, which only
// matters for pre-Mac OS X).
const NEWLINE: u8 = b'\n';

/// Helper function that finds the first new line character (\n) in the given byte slice.
fn newline_position(buffer: &[u8]) -> Option<usize> {
    // Assuming we are searching for the ASCII `\n` character, we don't need to do any special
    // handling for UTF-8, since a `\n` value always corresponds to an ASCII `\n`.
    // For more details, see: https://en.wikipedia.org/wiki/UTF-8#Encoding
    memchr::memchr(NEWLINE, buffer)
}
