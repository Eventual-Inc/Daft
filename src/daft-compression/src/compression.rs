use async_compression::tokio::bufread::{
    BrotliDecoder, BzDecoder, DeflateDecoder, GzipDecoder, LzmaDecoder, XzDecoder, ZlibDecoder,
    ZstdDecoder,
};
use std::{path::PathBuf, pin::Pin};
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
    pub fn from_uri(uri: &str) -> Option<Self> {
        let url = Url::parse(uri);
        let path = match &url {
            Ok(url) => url.path(),
            _ => uri,
        };
        let extension = PathBuf::from(path)
            .extension()?
            .to_string_lossy()
            .to_string();
        Self::from_extension(extension.as_ref())
    }
    pub fn from_extension(extension: &str) -> Option<Self> {
        use CompressionCodec::*;
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
        use CompressionCodec::*;
        match self {
            Brotli => Box::pin(BrotliDecoder::new(reader)),
            Bz => Box::pin(BzDecoder::new(reader)),
            Deflate => Box::pin(DeflateDecoder::new(reader)),
            Gzip => Box::pin(GzipDecoder::new(reader)),
            Lzma => Box::pin(LzmaDecoder::new(reader)),
            Xz => Box::pin(XzDecoder::new(reader)),
            Zlib => Box::pin(ZlibDecoder::new(reader)),
            Zstd => Box::pin(ZstdDecoder::new(reader)),
        }
    }
}
