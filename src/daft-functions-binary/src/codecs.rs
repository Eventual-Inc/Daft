use std::{fmt::Display, str::FromStr};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::DataType;
use daft_dsl::{FromLiteral, Literal, LiteralValue};
use serde::{Deserialize, Serialize};
use simdutf8::basic::from_utf8;

/// Supported codecs for the decode and encode functions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Codec {
    Deflate,
    Gzip,
    Utf8,
    Zlib,
}

impl Display for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Deflate => "deflate",
            Self::Gzip => "gzip",
            Self::Utf8 => "utf8",
            Self::Zlib => "zlib",
        })
    }
}

impl Literal for Codec {
    fn literal_value(self) -> daft_dsl::LiteralValue {
        LiteralValue::Utf8(self.to_string())
    }
}

impl FromLiteral for Codec {
    fn try_from_literal(lit: &LiteralValue) -> DaftResult<Self> {
        if let LiteralValue::Utf8(s) = lit {
            s.parse()
        } else {
            Err(DaftError::ValueError(format!(
                "Expected a string literal, got {:?}",
                lit
            )))
        }
    }
}

/// Determines if the decoded output should be a binary or text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodecKind {
    Binary,
    Text,
}

/// Function type for binary-to-binary transform.
pub(crate) type Transform = fn(input: &[u8]) -> DaftResult<Vec<u8>>;

/// Each codec should have an encode/decode pair.
impl Codec {
    pub(crate) fn encoder(&self) -> Transform {
        match self {
            Self::Deflate => deflate_encoder,
            Self::Gzip => gzip_encoder,
            Self::Utf8 => utf8_encoder,
            Self::Zlib => zlib_encoder,
        }
    }

    pub(crate) fn decoder(&self) -> Transform {
        match self {
            Self::Deflate => deflate_decoder,
            Self::Gzip => gzip_decoder,
            Self::Utf8 => utf8_decoder,
            Self::Zlib => zlib_decoder,
        }
    }

    pub(crate) fn kind(&self) -> CodecKind {
        match self {
            Self::Deflate => CodecKind::Binary,
            Self::Gzip => CodecKind::Binary,
            Self::Utf8 => CodecKind::Text,
            Self::Zlib => CodecKind::Binary,
        }
    }

    pub(crate) fn returns(&self) -> DataType {
        match self.kind() {
            CodecKind::Binary => DataType::Binary,
            CodecKind::Text => DataType::Utf8,
        }
    }
}

impl FromStr for Codec {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "deflate" => Ok(Self::Deflate),
            "gzip" | "gz" => Ok(Self::Gzip),
            "zlib" => Ok(Self::Zlib),
            "utf-8" | "utf8" => Ok(Self::Utf8),
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
fn utf8_encoder(input: &[u8]) -> DaftResult<Vec<u8>> {
    if input.is_ascii() || from_utf8(input).is_ok() {
        Ok(input.to_vec())
    } else {
        Err(DaftError::InternalError(
            "invalid utf-8 sequence".to_string(),
        ))
    }
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
fn utf8_decoder(input: &[u8]) -> DaftResult<Vec<u8>> {
    // zero-copy utf-8 validation using simdutf8
    if input.is_ascii() || from_utf8(input).is_ok() {
        Ok(input.to_vec())
    } else {
        Err(DaftError::InternalError(
            "invalid utf-8 sequence".to_string(),
        ))
    }
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
        assert_eq!("DEFLATE".parse::<Codec>().unwrap(), Codec::Deflate);
        assert_eq!("deflate".parse::<Codec>().unwrap(), Codec::Deflate);
        assert_eq!("gz".parse::<Codec>().unwrap(), Codec::Gzip);
        assert_eq!("GZ".parse::<Codec>().unwrap(), Codec::Gzip);
        assert_eq!("gzip".parse::<Codec>().unwrap(), Codec::Gzip);
        assert_eq!("GZIP".parse::<Codec>().unwrap(), Codec::Gzip);
        assert_eq!("GzIp".parse::<Codec>().unwrap(), Codec::Gzip);
        assert_eq!("utf-8".parse::<Codec>().unwrap(), Codec::Utf8);
        assert_eq!("UTF-8".parse::<Codec>().unwrap(), Codec::Utf8);
        assert_eq!("zlib".parse::<Codec>().unwrap(), Codec::Zlib);
        assert_eq!("ZLIB".parse::<Codec>().unwrap(), Codec::Zlib);
        assert_eq!("ZlIb".parse::<Codec>().unwrap(), Codec::Zlib);
        assert!("unknown".parse::<Codec>().is_err());
    }
}
