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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MediaTypeAudio;

impl DaftMediaType for MediaTypeAudio {
    fn get_type() -> MediaType {
        MediaType::Audio
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
pub type AudioFileType = FileType<MediaTypeAudio>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileReference {
    pub media_type: MediaType,
    pub url: String,
    pub io_config: Option<Arc<IOConfig>>,
}

impl FileReference {
    pub fn new(media_type: MediaType, url: String, io_config: Option<IOConfig>) -> Self {
        Self {
            media_type,
            url,
            io_config: io_config.map(Arc::new),
        }
    }
}

impl fmt::Display for FileReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.io_config {
            Some(_) => {
                write!(f, "{}(path: {} [with config])", self.media_type, self.url)
            }
            None => {
                write!(f, "{}(path: {})", self.media_type, self.url)
            }
        }
    }
}
