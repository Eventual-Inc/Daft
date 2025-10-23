#[cfg(feature = "python")]
pub mod python;

use std::{marker::PhantomData, sync::Arc};

use common_io_config::IOConfig;
pub use daft_schema::file_format::FileFormat;
use serde::{Deserialize, Serialize};

pub trait DaftFileFormat: Sync + Send + Clone + 'static + std::fmt::Debug {
    fn get_type() -> FileFormat
    where
        Self: Sized;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileFormatUnknown;

impl DaftFileFormat for FileFormatUnknown {
    fn get_type() -> FileFormat {
        FileFormat::Unknown
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileFormatVideo;

impl DaftFileFormat for FileFormatVideo {
    fn get_type() -> FileFormat {
        FileFormat::Video
    }
}

#[derive(Clone, Debug)]
pub struct FileType<T>
where
    T: DaftFileFormat,
{
    _phantom: PhantomData<T>,
}

pub type UnknownFileType = FileType<FileFormatUnknown>;
pub type VideoFileType = FileType<FileFormatVideo>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileReference {
    pub file_format: FileFormat,
    pub inner: DataOrReference,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DataOrReference {
    /// A reference to a file.
    Reference(String, Option<Arc<IOConfig>>),
    /// In memory data.
    Data(Arc<Vec<u8>>),
}

impl FileReference {
    pub fn new_from_data(file_format: FileFormat, data: Vec<u8>) -> Self {
        Self {
            file_format,
            inner: DataOrReference::Data(Arc::new(data)),
        }
    }
    pub fn new_from_reference(
        file_format: FileFormat,
        reference: String,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self {
            file_format,
            inner: DataOrReference::Reference(reference, io_config.map(Arc::new)),
        }
    }
}

impl std::fmt::Display for FileReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "File({:?})", self)
    }
}

impl FileReference {
    pub fn get_type(&self) -> FileReferenceType {
        match self.inner {
            DataOrReference::Reference(_, _) => FileReferenceType::Reference,
            DataOrReference::Data(_) => FileReferenceType::Data,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FileReferenceType {
    Reference = 0,
    Data = 1,
}

impl TryFrom<u8> for FileReferenceType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FileReferenceType::Reference),
            1 => Ok(FileReferenceType::Data),
            _ => Err(format!("Invalid FileReferenceType discriminant: {}", value)),
        }
    }
}

impl TryFrom<FileReferenceType> for u8 {
    type Error = String;

    fn try_from(value: FileReferenceType) -> Result<Self, Self::Error> {
        match value {
            FileReferenceType::Reference => Ok(0),
            FileReferenceType::Data => Ok(1),
        }
    }
}
