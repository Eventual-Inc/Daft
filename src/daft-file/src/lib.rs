mod file;
mod functions;

use daft_core::{datatypes::FileArray, file::DaftFileFormat};
pub use functions::*;

pub use crate::file::DaftFile;

#[cfg(feature = "python")]
pub mod python;

pub struct DaftFileIterator<T>
where
    T: DaftFileFormat,
{
    files: FileArray<T>,
    index: usize,
}

impl<T> Iterator for DaftFileIterator<T>
where
    T: DaftFileFormat,
{
    type Item = DaftFile;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.files.len() {
            None
        } else {
            let file = self.files.get(self.index).unwrap();
            self.index += 1;
            Some(file.try_into().expect("Failed to convert file"))
        }
    }
}

impl<T> DaftFileIterator<T>
where
    T: DaftFileFormat,
{
    pub fn new(files: FileArray<T>) -> Self {
        Self { files, index: 0 }
    }
}
