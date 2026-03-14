//! Defines [`GeoArrowError`], representing all errors returned by this crate.

use std::{error::Error, fmt::Debug};

use arrow_schema::ArrowError;
use thiserror::Error;

/// Enum with all errors in this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum GeoArrowError {
    /// [ArrowError]
    #[error(transparent)]
    Arrow(#[from] ArrowError),

    /// CRS error
    #[error("CRS related error: {0}")]
    Crs(String),

    /// Wraps an external error.
    #[error("External error: {0}")]
    External(#[from] Box<dyn Error + Send + Sync>),

    /// FlatGeobuf error
    #[error("FlatGeobuf error: {0}")]
    FlatGeobuf(String),

    /// GeoParquet error
    #[error("GeoParquet error: {0}")]
    GeoParquet(String),

    /// [std::io::Error]
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    /// Invalid data not conforming to GeoArrow specification
    #[error("Data not conforming to GeoArrow specification: {0}")]
    InvalidGeoArrow(String),

    /// Incorrect geometry type for operation
    #[error("Incorrect geometry type for operation: {0}")]
    IncorrectGeometryType(String),

    /// Whenever pushing to a container fails because it does not support more entries.
    ///
    /// The solution is usually to use a higher-capacity container-backing type.
    #[error("Overflow: data does not fit in i32 offsets.")]
    Overflow,

    /// WKB Error
    #[error("WKB error: {0}")]
    Wkb(String),

    /// WKT Error
    #[error("WKT error: {0}")]
    Wkt(String),
}

/// Crate-specific result type.
pub type GeoArrowResult<T> = std::result::Result<T, GeoArrowError>;

impl From<GeoArrowError> for ArrowError {
    /// Many APIs where we pass in a callback into the Arrow crate require the returned error type
    /// to be ArrowError, so implementing this `From` makes the conversion less verbose there.
    fn from(err: GeoArrowError) -> Self {
        match err {
            GeoArrowError::Arrow(err) => err,
            _ => ArrowError::ExternalError(Box::new(err)),
        }
    }
}
