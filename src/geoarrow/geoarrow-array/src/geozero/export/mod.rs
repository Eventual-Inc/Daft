//! Implementation to export GeoArrow arrays through the geozero API.

mod array;
mod data_source;
pub(crate) mod scalar;

pub use data_source::{GeozeroRecordBatchReader, GeozeroRecordBatchWriter};
