use std::{
    io,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_core::file::FileReference;
use daft_io::{GetRange, IOConfig, IOStatsRef, ObjectSource};
use daft_schema::media_type::MediaType;
use url::Url;

pub struct DaftFile {
    pub media_type: MediaType,
    pub(crate) cursor: Option<FileCursor>,
    pub(crate) position: usize,
}
// TODO(universalmind303): convert all the Read and Seek impls to AsyncRead and AsyncSeek.
// The python wrapper should handle blocking, but the core implementation should be fully async.
impl DaftFile {
    pub async fn load(file_ref: FileReference, download_small_files: bool) -> DaftResult<Self> {
        let media_type = file_ref.media_type;
        let io_client = daft_io::get_io_client(true, file_ref.io_config.unwrap_or_default())?;

        let (source, path) = io_client
            .get_source_and_path(&file_ref.url)
            .await
            .map_err(DaftError::from)?;
        // getting the size is pretty cheap, so we do it upfront
        // we grab the size upfront so we can use it to determine if we are at the end of the file
        let file_size = source
            .get_size(&path, None)
            .await
            .map_err(|e| DaftError::ComputeError(e.to_string()))?;
        let supports_range = source
            .supports_range(&path)
            .await
            .map_err(|e| DaftError::ComputeError(e.to_string()))?;

        // Default to 16MB buffer
        const DEFAULT_BUFFER_SIZE: usize = 16 * 1024 * 1024;

        let reader = ObjectSourceReader::new(source, path, None, file_size);
        if !supports_range || (file_size <= DEFAULT_BUFFER_SIZE && download_small_files) {
            let buf = reader.read_full_content().await?;

            Ok(Self::from_bytes(media_type, buf))
        } else {
            // we wrap it in a BufReader so we are not making so many network requests for each byte read
            let buffered_reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader);

            Ok(Self {
                media_type,
                cursor: Some(FileCursor::ObjectReader(buffered_reader)),
                position: 0,
            })
        }
    }

    /// Create a new file. unlike the async `new`, this will block the current thread until the file is created.
    pub fn load_blocking(file_ref: FileReference, download_small_files: bool) -> DaftResult<Self> {
        let rt = common_runtime::get_io_runtime(true);
        rt.block_within_async_context(Self::load(file_ref, download_small_files))
            .flatten()
    }

    pub fn from_path(
        media_type: MediaType,
        path: String,
        io_conf: Option<IOConfig>,
    ) -> DaftResult<Self> {
        Self::load_blocking(FileReference::new(media_type, path, io_conf), true)
    }

    pub fn from_bytes(media_type: MediaType, bytes: Vec<u8>) -> Self {
        Self {
            media_type,
            cursor: Some(FileCursor::Memory(Cursor::new(bytes))),
            position: 0,
        }
    }

    pub fn size(&self) -> DaftResult<usize> {
        self.cursor
            .as_ref()
            .map(|c| c.size())
            .ok_or(DaftError::IoError(std::io::Error::other("File not open")))
    }
    /// Attempt to guess the MIME type of the file.
    /// If we are unable to determine the MIME type, returns None.
    pub fn guess_mime_type(&mut self) -> Option<String> {
        self.cursor.as_mut().and_then(|c| c.mime_type())
    }
}

// Simple wrapper around ObjectSource
pub(crate) struct ObjectSourceReader {
    pub(crate) source: Arc<dyn ObjectSource>,
    pub(crate) uri: String,
    pub(crate) position: usize,
    pub(crate) io_stats: Option<IOStatsRef>,
    size: usize,
}

impl ObjectSourceReader {
    pub fn new(
        source: Arc<dyn ObjectSource>,
        uri: String,
        io_stats: Option<IOStatsRef>,
        size: usize,
    ) -> Self {
        Self {
            source,
            uri,
            position: 0,
            io_stats,
            size,
        }
    }
    fn read_full_content_blocking(&self) -> io::Result<Vec<u8>> {
        let rt = common_runtime::get_io_runtime(true);

        let source = self.source.clone();
        let uri = self.uri.clone();
        let io_stats = self.io_stats.clone();

        rt.block_within_async_context(async move {
            let result = source
                .get(&uri, None, io_stats)
                .await
                .map_err(map_get_error)?;

            result
                .bytes()
                .await
                .map(|b| b.to_vec())
                .map_err(map_bytes_error)
        })
        .map_err(map_async_error)
        .flatten()
    }

    async fn read_full_content(&self) -> io::Result<Vec<u8>> {
        let result = self
            .source
            .get(&self.uri, None, self.io_stats.clone())
            .await
            .map_err(map_get_error)?;

        result
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(map_bytes_error)
    }
}

// Implement Read for synchronous reading

impl Read for ObjectSourceReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let rt = common_runtime::get_io_runtime(true);
        let start = self.position;
        let end = start + buf.len();

        let range = Some(GetRange::Bounded(start..end));
        let source = self.source.clone();
        let uri = self.uri.clone();
        let io_stats = self.io_stats.clone();
        let bytes = rt
            .block_within_async_context(async move {
                match source.get(&uri, range, io_stats.clone()).await {
                    Ok(result) => {
                        let bytes = result.bytes().await.map_err(map_bytes_error)?;
                        Ok(bytes.to_vec())
                    }
                    Err(e) => {
                        let error_str = e.to_string();
                        // Check if error suggests range requests aren't supported
                        if error_str.contains("Requested Range Not Satisfiable")
                            || error_str.contains("416")
                        {
                            // Fall back to reading the entire file
                            let result = source
                                .get(&uri, None, io_stats)
                                .await
                                .map_err(map_get_error)?;

                            let bytes = result.bytes().await.map_err(map_bytes_error)?;
                            Ok(bytes.to_vec())
                        } else {
                            Err(map_get_error(e))
                        }
                    }
                }
            })
            .map_err(map_async_error)??;

        if bytes.is_empty() {
            return Ok(0);
        }

        let bytes_to_copy = std::cmp::min(buf.len(), bytes.len());
        buf[..bytes_to_copy].copy_from_slice(&bytes[..bytes_to_copy]);

        self.position += bytes_to_copy;
        Ok(bytes_to_copy)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let content = self.read_full_content_blocking()?;

        if self.position >= content.len() {
            return Ok(0);
        }

        let bytes_to_read = content.len() - self.position;
        buf.extend_from_slice(&content[self.position..]);

        self.position = content.len();

        Ok(bytes_to_read)
    }
}

impl Seek for ObjectSourceReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // Calculate the new position
        let new_position = match pos {
            SeekFrom::Start(offset) => offset as usize,
            SeekFrom::End(offset) => {
                let rt = common_runtime::get_io_runtime(true);

                let source = self.source.clone();
                let uri = self.uri.clone();
                let io_stats = self.io_stats.clone();

                let size = rt
                    .block_within_async_context(async move {
                        source.get_size(&uri, io_stats).await.map_err(map_get_error)
                    })
                    .map_err(map_async_error)??;

                if offset < 0 {
                    size.saturating_sub((-offset) as usize)
                } else {
                    size.saturating_add(offset as usize)
                }
            }
            SeekFrom::Current(offset) => {
                if offset < 0 {
                    self.position.saturating_sub((-offset) as usize)
                } else {
                    self.position.saturating_add(offset as usize)
                }
            }
        };

        // Update position
        self.position = new_position;

        Ok(self.position as u64)
    }
}

pub(crate) enum FileCursor {
    ObjectReader(BufReader<ObjectSourceReader>),
    Memory(std::io::Cursor<Vec<u8>>),
}

impl FileCursor {
    pub fn size(&self) -> usize {
        match self {
            Self::ObjectReader(cursor) => cursor.get_ref().size,
            Self::Memory(cursor) => cursor.get_ref().len(),
        }
    }

    pub fn mime_type(&mut self) -> Option<String> {
        match self {
            Self::ObjectReader(buf_reader) => {
                let inner = buf_reader.get_ref();
                let try_from_url = guess_mimetype_from_url(&inner.uri);
                if try_from_url.is_some() {
                    try_from_url
                } else {
                    guess_mimetype_from_content(buf_reader).ok().flatten()
                }
            }
            Self::Memory(cursor) => guess_mimetype_from_content(cursor).ok().flatten(),
        }
    }
}

impl Read for FileCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::ObjectReader(cursor) => cursor.read(buf),
            Self::Memory(cursor) => cursor.read(buf),
        }
    }
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        match self {
            Self::ObjectReader(cursor) => cursor.read_to_end(buf),
            Self::Memory(cursor) => cursor.read_to_end(buf),
        }
    }
}

impl Seek for FileCursor {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            Self::ObjectReader(cursor) => cursor.seek(pos),
            Self::Memory(cursor) => cursor.seek(pos),
        }
    }
}
fn map_get_error(e: daft_io::Error) -> io::Error {
    io::Error::other(format!("Get failed: {}", e))
}
fn map_bytes_error(e: daft_io::Error) -> io::Error {
    io::Error::other(format!("Bytes failed: {}", e))
}
fn map_async_error(e: DaftError) -> io::Error {
    io::Error::other(format!("Async context failed: {}", e))
}

const PNG_MAGIC: &[u8] = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]; // PNG header
const JPEG_MAGIC: &[u8] = &[0xFF, 0xD8]; // JPEG header
const GIF_MAGIC: &[u8] = &[0x47, 0x49, 0x46, 0x38]; // GIF header
const WEBP_RIFF_MAGIC: &[u8] = &[0x52, 0x49, 0x46, 0x46]; // WEBP RIFF header
const WEBP_MAGIC: &[u8] = &[0x57, 0x45, 0x42, 0x50]; // WEBP header
const PDF_MAGIC: &[u8] = &[0x25, 0x50, 0x44, 0x46]; // PDF header
const ZIP_MAGIC: &[u8] = &[0x50, 0x4B, 0x03, 0x04]; // ZIP header
const MP3_ID3_MAGIC: &[u8] = &[0x49, 0x44, 0x33]; // ID3 tag (MP3)
const MP3_MAGIC: &[u8] = &[0xFF, 0xFB]; // MPEG ADTS, layer III
const WAV_RIFF_MAGIC: &[u8] = &[0x52, 0x49, 0x46, 0x46]; // RIFF header
const WAV_MAGIC: &[u8] = &[0x57, 0x41, 0x56, 0x45]; // WAVE header
const MPEG_MAGIC: &[u8] = &[0x00, 0x00, 0x01, 0xBA]; // MPEG transport stream
const MP4_FTYP_MAGIC: &[u8] = &[0x66, 0x74, 0x79, 0x70]; // MP4 ftyp header
const OGG_MAGIC: &[u8] = &[0x4F, 0x67, 0x67, 0x53]; // Ogg Vorbis
const HTML_MAGIC_1: &[u8] = &[0x3C, 0x21, 0x44, 0x4F, 0x43, 0x54, 0x59, 0x50, 0x45]; // <!DOCTYPE
const HTML_MAGIC_2: &[u8] = &[0x3C, 0x68, 0x74, 0x6D, 0x6C]; // <html
const HTML_MAGIC_3: &[u8] = &[0x3C, 0x48, 0x54, 0x4D, 0x4C]; // <HTML

fn guess_mimetype_from_url(url: &str) -> Option<String> {
    let url = Url::parse(url).ok()?;
    let path = url.path();
    let mime = mime_guess::from_path(path).first()?;
    Some(mime.to_string())
}

pub(crate) fn guess_mimetype_from_content<R: Read + Seek>(
    reader: &mut R,
) -> std::io::Result<Option<String>> {
    let mut buffer = [0; 16]; // Extended for more formats
    let original_pos = reader.stream_position()?;

    reader.seek(SeekFrom::Start(0))?;
    let bytes_read = reader.read(&mut buffer)?;
    reader.seek(SeekFrom::Start(original_pos))?;

    if bytes_read == 0 {
        return Ok(None);
    }

    let mime = if starts_with(&buffer, PNG_MAGIC) {
        Some("image/png")
    } else if starts_with(&buffer, JPEG_MAGIC) {
        Some("image/jpeg")
    } else if starts_with(&buffer, GIF_MAGIC) {
        Some("image/gif")
    } else if starts_with(&buffer, WEBP_RIFF_MAGIC)
        && bytes_read >= 12
        && starts_with(&buffer[8..], WEBP_MAGIC)
    {
        Some("image/webp")
    } else if starts_with(&buffer, PDF_MAGIC) {
        Some("application/pdf")
    } else if starts_with(&buffer, ZIP_MAGIC) {
        Some("application/zip")
    } else if starts_with(&buffer, MP3_ID3_MAGIC) || starts_with(&buffer, MP3_MAGIC) {
        Some("audio/mpeg")
    } else if starts_with(&buffer, WAV_RIFF_MAGIC)
        && bytes_read >= 12
        && starts_with(&buffer[8..], WAV_MAGIC)
    {
        Some("audio/wav")
    } else if starts_with(&buffer, OGG_MAGIC) {
        Some("audio/ogg")
    } else if bytes_read >= 8 && starts_with(&buffer[4..], MP4_FTYP_MAGIC) {
        Some("video/mp4")
    } else if starts_with(&buffer, MPEG_MAGIC) {
        Some("video/mpeg")
    } else if starts_with(&buffer, HTML_MAGIC_1)
        || starts_with(&buffer, HTML_MAGIC_2)
        || starts_with(&buffer, HTML_MAGIC_3)
    {
        Some("text/html")
    } else {
        None
    };

    Ok(mime.map(|s| s.to_string()))
}

fn starts_with(buffer: &[u8], pattern: &[u8]) -> bool {
    buffer.len() >= pattern.len() && buffer[..pattern.len()] == pattern[..]
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_guess_mimetype_from_url() {
        let test_cases = vec![
            ("https://example.com/image.jpg", Some("image/jpeg")),
            ("file:///path/to/image.PNG", Some("image/png")),
            ("s3://bucket/folder/document.pdf", Some("application/pdf")),
            ("http://example.com/file.csv?param=value", Some("text/csv")),
            ("https://example.com/audio.mp3#fragment", Some("audio/mpeg")),
            ("https://example.com/music.wav", Some("audio/wav")),
            ("https://example.com/video.mp4", Some("video/mp4")),
            ("https://example.com/audio.ogg", Some("audio/ogg")),
            ("https://example.com/video.webm", Some("video/webm")),
            ("https://example.com/movie.mov", Some("video/quicktime")),
            ("https://example.com/music.flac", Some("audio/flac")),
            ("https://example.com/sound.aac", Some("audio/aac")),
            ("https://example.com/noextension", None),
            ("https://example.com/unknown.abcde", None),
        ];

        for (url, expected) in test_cases {
            let result = guess_mimetype_from_url(url);
            assert_eq!(result.as_deref(), expected, "Failed for URL: {}", url);
        }
    }

    #[test]
    fn test_guess_mimetype_from_reader_png() {
        let data = [
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00,
        ];
        let mut reader = Cursor::new(data);
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result.as_deref(), Some("image/png"));
    }

    #[test]
    fn test_guess_mimetype_from_reader_jpeg() {
        let data = [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46];
        let mut reader = Cursor::new(data);
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result.as_deref(), Some("image/jpeg"));
    }

    #[test]
    fn test_guess_mimetype_from_reader_mp3_id3() {
        let data = [0x49, 0x44, 0x33, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut reader = Cursor::new(data);
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result.as_deref(), Some("audio/mpeg"));
    }

    #[test]
    fn test_guess_mimetype_from_reader_mp3() {
        let data = [0xFF, 0xFB, 0x90, 0x44, 0x00, 0x00, 0x00, 0x00];
        let mut reader = Cursor::new(data);
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result.as_deref(), Some("audio/mpeg"));
    }

    #[test]
    fn test_guess_mimetype_from_reader_wav() {
        let data = [
            0x52, 0x49, 0x46, 0x46, 0x24, 0x00, 0x00, 0x00, 0x57, 0x41, 0x56, 0x45,
        ];
        let mut reader = Cursor::new(data);
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result.as_deref(), Some("audio/wav"));
    }

    #[test]
    fn test_guess_mimetype_from_reader_ogg() {
        let data = [0x4F, 0x67, 0x67, 0x53, 0x00, 0x02, 0x00, 0x00];
        let mut reader = Cursor::new(data);
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result.as_deref(), Some("audio/ogg"));
    }

    #[test]
    fn test_guess_mimetype_from_reader_mp4() {
        let data = [
            0x00, 0x00, 0x00, 0x18, 0x66, 0x74, 0x79, 0x70, 0x6D, 0x70, 0x34, 0x32,
        ];
        let mut reader = Cursor::new(data);
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result.as_deref(), Some("video/mp4"));
    }

    #[test]
    fn test_guess_mimetype_from_reader_mpeg() {
        let data = [0x00, 0x00, 0x01, 0xBA, 0x21, 0x00, 0x01, 0x00];
        let mut reader = Cursor::new(data);
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result.as_deref(), Some("video/mpeg"));
    }

    #[test]
    fn test_guess_mimetype_from_reader_unknown() {
        let data = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        let mut reader = Cursor::new(data);
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_guess_mimetype_from_reader_empty() {
        let data = [];
        let mut reader = Cursor::new(data);
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_guess_mimetype_from_reader_seek_reset() {
        let data = [
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00,
        ];
        let mut reader = Cursor::new(data);

        // Move cursor to position 3
        reader.seek(SeekFrom::Start(3)).unwrap();

        // Check that position is restored after guess
        let result = guess_mimetype_from_content(&mut reader).unwrap();
        assert_eq!(result.as_deref(), Some("image/png"));
        assert_eq!(reader.position(), 3);
    }
}
