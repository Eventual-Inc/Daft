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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MediaTypeImage;

impl DaftMediaType for MediaTypeImage {
    fn get_type() -> MediaType {
        MediaType::Image
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
pub type ImageFileType = FileType<MediaTypeImage>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileReference {
    pub media_type: MediaType,
    pub url: String,
    pub io_config: Option<Arc<IOConfig>>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub length: Option<u64>,
}

impl FileReference {
    pub fn new(media_type: MediaType, url: String, io_config: Option<IOConfig>) -> Self {
        Self {
            media_type,
            url,
            io_config: io_config.map(Arc::new),
            offset: None,
            length: None,
        }
    }

    pub fn new_with_range(
        media_type: MediaType,
        url: String,
        io_config: Option<IOConfig>,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> Self {
        Self {
            media_type,
            url,
            io_config: io_config.map(Arc::new),
            offset,
            length,
        }
    }
}

impl fmt::Display for FileReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(path: {}", self.media_type, self.url)?;
        if self.io_config.is_some() {
            write!(f, " [with config]")?;
        }
        if let (Some(offset), Some(length)) = (self.offset, self.length) {
            write!(f, " [range: {}..{}]", offset, offset.saturating_add(length))?;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_without_range() {
        let file_ref =
            FileReference::new(MediaType::Unknown, "s3://bucket/file.bin".to_string(), None);
        assert_eq!(file_ref.to_string(), "Unknown(path: s3://bucket/file.bin)");
    }

    #[test]
    fn test_display_with_range() {
        let file_ref = FileReference::new_with_range(
            MediaType::Unknown,
            "s3://bucket/blob".to_string(),
            None,
            Some(10),
            Some(20),
        );
        assert_eq!(
            file_ref.to_string(),
            "Unknown(path: s3://bucket/blob [range: 10..30])"
        );
    }

    #[test]
    fn test_display_with_config_and_range() {
        let file_ref = FileReference::new_with_range(
            MediaType::Video,
            "s3://bucket/video.mp4".to_string(),
            Some(IOConfig::default()),
            Some(0),
            Some(1024),
        );
        let display = file_ref.to_string();
        assert!(display.contains("[with config]"));
        assert!(display.contains("[range: 0..1024]"));
    }
}
