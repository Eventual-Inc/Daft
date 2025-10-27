mod file;
mod functions;

use daft_core::{datatypes::FileArray, file::DaftMediaType};
pub use functions::*;

pub use crate::file::DaftFile;

#[cfg(feature = "python")]
pub mod python;

pub struct DaftFileIterator<T>
where
    T: DaftMediaType,
{
    files: FileArray<T>,
    index: usize,
}

impl<T> Iterator for DaftFileIterator<T>
where
    T: DaftMediaType,
{
    type Item = DaftFile;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.files.len() {
            None
        } else {
            let file = self.files.get(self.index).unwrap();
            self.index += 1;
            let file = DaftFile::new_blocking(file).expect("Failed to create DaftFile");

            Some(file)
        }
    }
}

impl<T> DaftFileIterator<T>
where
    T: DaftMediaType,
{
    pub fn new(files: FileArray<T>) -> Self {
        Self { files, index: 0 }
    }
}
