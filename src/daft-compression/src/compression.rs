use std::{path::PathBuf, pin::Pin};

use async_compression::tokio::bufread::{
    BrotliDecoder, BzDecoder, DeflateDecoder, GzipDecoder, LzmaDecoder, XzDecoder, ZlibDecoder,
    ZstdDecoder,
};
use tokio::io::{AsyncBufRead, AsyncRead};
use url::Url;

#[derive(Debug)]
pub enum CompressionCodec {
    Brotli,
    Bz,
    Deflate,
    Gzip,
    Lzma,
    Xz,
    Zlib,
    Zstd,
}

impl CompressionCodec {
    #[must_use]
    pub fn from_uri(uri: &str) -> Option<Self> {
        let url = Url::parse(uri);
        let path = if let Some(stripped) = uri.strip_prefix("file://") {
            // Handle file URLs properly by stripping the scheme.
            stripped
        } else {
            match &url {
                Ok(url) => url.path(),
                _ => uri,
            }
        };
        let extension = PathBuf::from(path)
            .extension()?
            .to_string_lossy()
            .to_string();
        Self::from_extension(extension.as_ref())
    }
    #[must_use]
    pub fn from_extension(extension: &str) -> Option<Self> {
        use CompressionCodec::{Brotli, Bz, Deflate, Gzip, Lzma, Xz, Zlib, Zstd};
        match extension {
            "br" => Some(Brotli),
            "bz2" => Some(Bz),
            "deflate" => Some(Deflate),
            "gz" => Some(Gzip),
            "lzma" => Some(Lzma),
            "xz" => Some(Xz),
            "zl" => Some(Zlib),
            "zstd" | "zst" => Some(Zstd),
            "snappy" => todo!("Snappy compression support not yet implemented"),
            _ => None,
        }
    }

    pub fn to_decoder<T: AsyncBufRead + Send + 'static>(
        &self,
        reader: T,
    ) -> Pin<Box<dyn AsyncRead + Send>> {
        use CompressionCodec::{Brotli, Bz, Deflate, Gzip, Lzma, Xz, Zlib, Zstd};
        match self {
            Brotli => Box::pin(BrotliDecoder::new(reader)),
            Bz => Box::pin(BzDecoder::new(reader)),
            Deflate => Box::pin(DeflateDecoder::new(reader)),
            Gzip => {
                // With async-compression, compressed files with multiple concatenated members
                // might be incorrectly read as an early EOF. Setting multiple_members(true)
                // ensures that the reader will continue to read until the end of the file.
                // For more details, see: https://github.com/Nullus157/async-compression/issues/153
                let mut decoder = GzipDecoder::new(reader);
                decoder.multiple_members(true);
                Box::pin(decoder)
            }
            Lzma => Box::pin(LzmaDecoder::new(reader)),
            Xz => Box::pin(XzDecoder::new(reader)),
            Zlib => Box::pin(ZlibDecoder::new(reader)),
            Zstd => Box::pin(ZstdDecoder::new(reader)),
        }
    }
}
