use std::{fmt::Display, str::FromStr};

use daft_common::error::{DaftError, DaftResult};
use daft_core::lit::{FromLiteral, Literal};
use serde::{Deserialize, Serialize};

/// Perceptual hash algorithm to use for image deduplication.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum HashMethod {
    /// Average hash — fastest, least robust.
    AHash,
    /// Difference / gradient hash — fast and accurate.
    DHash,
    /// Vertical difference hash — like dHash but compares top/bottom neighbours.
    DHashVertical,
    /// DCT-based perceptual hash — most robust (default).
    PHash,
    /// Simplified DCT perceptual hash — row-wise DCT only, compared to mean.
    PHashSimple,
    /// Haar wavelet hash.
    WHash,
    /// Crop-resistant hash: divides the image into a grid of segments,
    /// computes a sub-hash per segment and concatenates them.
    /// Robust against cropping at the cost of a larger hash.
    CropResistant,
    /// Color hash: encodes the distribution of colors in HSV space.
    /// Uses `binbits` bits per bin across 14 color/intensity bins.
    ColorHash,
}

impl Display for HashMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::AHash => "ahash",
            Self::DHash => "dhash",
            Self::DHashVertical => "dhash_vertical",
            Self::PHash => "phash",
            Self::PHashSimple => "phash_simple",
            Self::WHash => "whash",
            Self::CropResistant => "crop_resistant",
            Self::ColorHash => "colorhash",
        })
    }
}

impl From<HashMethod> for Literal {
    fn from(value: HashMethod) -> Self {
        Self::Utf8(value.to_string())
    }
}

impl FromLiteral for HashMethod {
    fn try_from_literal(lit: &Literal) -> DaftResult<Self> {
        if let Literal::Utf8(s) = lit {
            s.parse()
        } else {
            Err(DaftError::ValueError(format!(
                "Expected a string literal for hash method, got {:?}",
                lit
            )))
        }
    }
}

impl FromStr for HashMethod {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ahash" => Ok(Self::AHash),
            "dhash" => Ok(Self::DHash),
            "dhash_vertical" => Ok(Self::DHashVertical),
            "phash" => Ok(Self::PHash),
            "phash_simple" => Ok(Self::PHashSimple),
            "whash" => Ok(Self::WHash),
            "crop_resistant" => Ok(Self::CropResistant),
            "colorhash" => Ok(Self::ColorHash),
            other => Err(DaftError::ValueError(format!(
                "Unknown image hash method: '{other}'. \
                 Expected one of: 'ahash', 'dhash', 'dhash_vertical', 'phash', 'phash_simple', \
                 'whash', 'crop_resistant', 'colorhash'."
            ))),
        }
    }
}
