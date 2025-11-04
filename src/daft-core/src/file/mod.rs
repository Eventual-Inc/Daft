#[cfg(feature = "python")]
pub mod python;

use std::{fmt, marker::PhantomData, sync::Arc};

use common_io_config::IOConfig;
pub use daft_schema::media_type::MediaType;
use serde::{Deserialize, Serialize};

pub trait DaftMediaType: Sync + Send + Clone + 'static + std::fmt::Debug {
    fn get_type() -> MediaType
    where
        Self: Sized;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MediaTypeUnknown;

impl DaftMediaType for MediaTypeUnknown {
    fn get_type() -> MediaType {
        MediaType::Unknown
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MediaTypeVideo;

impl DaftMediaType for MediaTypeVideo {
    fn get_type() -> MediaType {
        MediaType::Video
    }
}

#[derive(Clone, Debug)]
pub struct FileType<T>
where
    T: DaftMediaType,
{
    _phantom: PhantomData<T>,
}

pub type UnknownFileType = FileType<MediaTypeUnknown>;
pub type VideoFileType = FileType<MediaTypeVideo>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileReference {
    pub media_type: MediaType,
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
    pub fn new_from_data(media_type: MediaType, data: Vec<u8>) -> Self {
        Self {
            media_type,
            inner: DataOrReference::Data(Arc::new(data)),
        }
    }
    pub fn new_from_reference(
        media_type: MediaType,
        reference: String,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self {
            media_type,
            inner: DataOrReference::Reference(reference, io_config.map(Arc::new)),
        }
    }
}

impl fmt::Display for FileReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            DataOrReference::Reference(path, config) => {
                write!(
                    f,
                    "{}(path: {}{})",
                    self.media_type,
                    path,
                    if config.is_some() {
                        " [with config]"
                    } else {
                        ""
                    }
                )
            }
            DataOrReference::Data(data) => {
                write!(f, "{}(in-memory: {} bytes)", self.media_type, data.len())
            }
        }
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
            0 => Ok(Self::Reference),
            1 => Ok(Self::Data),
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
