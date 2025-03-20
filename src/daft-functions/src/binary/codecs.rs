use common_error::{DaftError, DaftResult};
use serde::{Deserialize, Serialize};

/// Supported codecs for the decode and encode functions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Codec {
    Gzip,
    Zlib,
    Deflate,
}

/// Function type for encoding a string to bytes
pub(crate) type Encoder = fn(input: &[u8]) -> DaftResult<Vec<u8>>;

/// Function type for decoding bytes to a string
pub(crate) type Decoder = fn(input: &[u8]) -> DaftResult<Vec<u8>>;

/// Each codec should have an encode/decode pair.
impl Codec {
    pub(crate) fn encoder(&self) -> Encoder {
        match self {
            Self::Gzip => gzip_encoder,
            Self::Zlib => zlib_encoder,
            Self::Deflate => deflate_encoder,
        }
    }

    pub(crate) fn decoder(&self) -> Decoder {
        match self {
            Self::Deflate => deflate_decoder,
            Self::Gzip => gzip_decoder,
            Self::Zlib => zlib_decoder,
        }
    }
}

impl TryFrom<&str> for Codec {
    type Error = DaftError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "deflate" => Ok(Self::Deflate),
            "gzip" | "gz" => Ok(Self::Gzip),
            "zlib" => Ok(Self::Zlib),
            _ => Err(DaftError::not_implemented(format!(
                "unsupported codec: {}",
                s
            ))),
        }
    }
}

//
// ENCODERS
//

#[inline]
fn deflate_encoder(input: &[u8]) -> DaftResult<Vec<u8>> {
    use std::io::Write;

    use flate2::{write::DeflateEncoder, Compression};
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input)?;
    Ok(encoder.finish()?)
}

#[inline]
fn gzip_encoder(input: &[u8]) -> DaftResult<Vec<u8>> {
    use std::io::Write;

    use flate2::{write::GzEncoder, Compression};
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input)?;
    Ok(encoder.finish()?)
}

#[inline]
fn zlib_encoder(input: &[u8]) -> DaftResult<Vec<u8>> {
    use std::io::Write;

    use flate2::{write::ZlibEncoder, Compression};
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input)?;
    Ok(encoder.finish()?)
}

//
// DECODERS
//

#[inline]
fn deflate_decoder(input: &[u8]) -> DaftResult<Vec<u8>> {
    use std::io::Read;

    use flate2::read::DeflateDecoder;
    let mut decoder = DeflateDecoder::new(input);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded)?;
    Ok(decoded)
}

#[inline]
fn gzip_decoder(input: &[u8]) -> DaftResult<Vec<u8>> {
    use std::io::Read;

    use flate2::read::GzDecoder;
    let mut decoder = GzDecoder::new(input);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded)?;
    Ok(decoded)
}

#[inline]
fn zlib_decoder(input: &[u8]) -> DaftResult<Vec<u8>> {
    use std::io::Read;

    use flate2::read::ZlibDecoder;
    let mut decoder = ZlibDecoder::new(input);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded)?;
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_from_str() {
        assert_eq!(Codec::try_from("DEFLATE").unwrap(), Codec::Deflate);
        assert_eq!(Codec::try_from("deflate").unwrap(), Codec::Deflate);
        assert_eq!(Codec::try_from("gz").unwrap(), Codec::Gzip);
        assert_eq!(Codec::try_from("GZ").unwrap(), Codec::Gzip);
        assert_eq!(Codec::try_from("gzip").unwrap(), Codec::Gzip);
        assert_eq!(Codec::try_from("GZIP").unwrap(), Codec::Gzip);
        assert_eq!(Codec::try_from("GzIp").unwrap(), Codec::Gzip);
        assert_eq!(Codec::try_from("zlib").unwrap(), Codec::Zlib);
        assert_eq!(Codec::try_from("ZLIB").unwrap(), Codec::Zlib);
        assert_eq!(Codec::try_from("ZlIb").unwrap(), Codec::Zlib);
        assert!(Codec::try_from("unknown").is_err());
    }
}
