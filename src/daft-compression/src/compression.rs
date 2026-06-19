use std::{path::PathBuf, pin::Pin};

use async_compression::tokio::bufread::{
    BrotliDecoder, BzDecoder, DeflateDecoder, GzipDecoder, LzmaDecoder, XzDecoder, ZlibDecoder,
    ZstdDecoder,
};
use futures::StreamExt;
use tokio::io::{AsyncBufRead, AsyncRead};
use url::Url;

#[derive(Debug)]
pub enum CompressionCodec {
    Brotli,
    Bz,
    Deflate,
    Gzip,
    Lzma,
    TarGz,
    Xz,
    Zlib,
    Zstd,
}

impl CompressionCodec {
    #[must_use]
    pub fn from_uri(uri: &str) -> Option<Self> {
        let url = Url::parse(uri);
        let path = if let Some(stripped) = uri.strip_prefix("file://") {
            // Handle file URLs properly by stripping the scheme. We only use
            // `path` for extension detection below, so the leading `/` before
            // a Windows drive letter (`/C:/...`) is harmless here.
            stripped
        } else {
            match &url {
                Ok(url) => url.path(),
                _ => uri,
            }
        };
        let path_buf = PathBuf::from(path);
        // Check for compound extensions (.tar.gz) before falling back to the
        // last extension only, since PathBuf::extension() returns only "gz".
        let file_name = path_buf.file_name()?.to_string_lossy().to_lowercase();
        if file_name.ends_with(".tar.gz") {
            return Some(Self::TarGz);
        }
        let extension = path_buf.extension()?.to_string_lossy().to_string();
        Self::from_extension(extension.as_ref())
    }

    #[must_use]
    pub fn from_extension(extension: &str) -> Option<Self> {
        use CompressionCodec::{Brotli, Bz, Deflate, Gzip, Lzma, TarGz, Xz, Zlib, Zstd};
        match extension {
            "br" => Some(Brotli),
            "bz2" => Some(Bz),
            "deflate" => Some(Deflate),
            "gz" => Some(Gzip),
            "lzma" => Some(Lzma),
            "tgz" => Some(TarGz),
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
        use CompressionCodec::{Brotli, Bz, Deflate, Gzip, Lzma, TarGz, Xz, Zlib, Zstd};
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
            TarGz => {
                let (mut writer, pipe_reader) = tokio::io::duplex(128 * 1024);
                tokio::spawn(async move {
                    let mut gz = GzipDecoder::new(reader);
                    gz.multiple_members(true);
                    // Pin<Box<T>> is Unpin regardless of T, satisfying tokio_tar's bound.
                    let gz_pinned = Box::pin(gz);
                    let mut archive = tokio_tar::Archive::new(gz_pinned);
                    if let Ok(mut entries) = archive.entries() {
                        while let Some(Ok(mut entry)) = entries.next().await {
                            if tokio::io::copy(&mut entry, &mut writer).await.is_err() {
                                break;
                            }
                        }
                    }
                });
                Box::pin(pipe_reader)
            }
            Xz => Box::pin(XzDecoder::new(reader)),
            Zlib => Box::pin(ZlibDecoder::new(reader)),
            Zstd => Box::pin(ZstdDecoder::new(reader)),
        }
    }
}

#[cfg(test)]
mod tests {
    use async_compression::tokio::write::GzipEncoder;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio_tar::Builder;

    use super::*;

    /// Build a tar.gz in memory containing one or more (name, content) entries.
    ///
    /// Uses a duplex pipe + spawned task because `tokio_tar::Builder` requires
    /// `W: 'static`, which rules out `&mut Vec<u8>`.
    async fn make_tar_gz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let (write_half, mut read_half) = tokio::io::duplex(64 * 1024);
        let owned: Vec<(String, Vec<u8>)> = entries
            .iter()
            .map(|(n, d)| ((*n).to_string(), d.to_vec()))
            .collect();
        tokio::spawn(async move {
            let gz_enc = GzipEncoder::new(write_half);
            let mut tar = Builder::new(gz_enc);
            for (name, data) in &owned {
                let mut header = tokio_tar::Header::new_gnu();
                header.set_size(data.len() as u64);
                header.set_mode(0o644);
                header.set_cksum();
                tar.append_data(
                    &mut header,
                    name.as_str(),
                    std::io::Cursor::new(data.as_slice()),
                )
                .await
                .unwrap();
            }
            let mut gz_enc = tar.into_inner().await.unwrap();
            gz_enc.shutdown().await.unwrap();
            // dropping write_half here closes the duplex, signalling EOF
        });
        let mut buf = Vec::new();
        read_half.read_to_end(&mut buf).await.unwrap();
        buf
    }

    #[tokio::test]
    async fn test_from_uri_tar_gz() {
        assert!(matches!(
            CompressionCodec::from_uri("s3://bucket/data.tar.gz"),
            Some(CompressionCodec::TarGz)
        ));
        assert!(matches!(
            CompressionCodec::from_uri("file:///tmp/data.TAR.GZ"),
            Some(CompressionCodec::TarGz)
        ));
    }

    #[tokio::test]
    async fn test_from_uri_tgz() {
        assert!(matches!(
            CompressionCodec::from_uri("s3://bucket/data.tgz"),
            Some(CompressionCodec::TarGz)
        ));
    }

    #[tokio::test]
    async fn test_from_uri_gz_not_tar() {
        // A plain .gz file should still resolve to Gzip, not TarGz.
        assert!(matches!(
            CompressionCodec::from_uri("s3://bucket/data.csv.gz"),
            Some(CompressionCodec::Gzip)
        ));
    }

    #[tokio::test]
    async fn test_tar_gz_single_entry() {
        let content = b"col1,col2\n1,2\n3,4\n";
        let tar_gz = make_tar_gz(&[("data.csv", content)]).await;

        let reader = BufReader::new(std::io::Cursor::new(tar_gz));
        let mut decoder = CompressionCodec::TarGz.to_decoder(reader);

        let mut out = Vec::new();
        decoder.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, content);
    }

    #[tokio::test]
    async fn test_tar_gz_multiple_entries_concatenated() {
        let part1 = b"hello\n";
        let part2 = b"world\n";
        let tar_gz = make_tar_gz(&[("a.txt", part1), ("b.txt", part2)]).await;

        let reader = BufReader::new(std::io::Cursor::new(tar_gz));
        let mut decoder = CompressionCodec::TarGz.to_decoder(reader);

        let mut out = Vec::new();
        decoder.read_to_end(&mut out).await.unwrap();

        let mut expected = part1.to_vec();
        expected.extend_from_slice(part2);
        assert_eq!(out, expected);
    }
}
