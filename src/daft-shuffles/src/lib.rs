pub mod client;
pub mod coalescer;
pub mod decode_cache;
pub mod multi_partition_cache;
pub mod oneshot_writer;
pub mod server;
pub mod shuffle_cache;

use common_error::{DaftError, DaftResult};

/// Parse a Flight-shuffle compression config string (`"none"` | `"lz4"` | `"zstd"`)
/// into the arrow-ipc `CompressionType`.
pub fn parse_flight_compression(s: Option<&str>) -> DaftResult<Option<arrow_ipc::CompressionType>> {
    match s {
        None | Some("none") | Some("") => Ok(None),
        Some("lz4") => Ok(Some(arrow_ipc::CompressionType::LZ4_FRAME)),
        Some("zstd") => Ok(Some(arrow_ipc::CompressionType::ZSTD)),
        Some(other) => Err(DaftError::ValueError(format!(
            "flight_shuffle_compression must be 'none', 'lz4', or 'zstd' (got {})",
            other
        ))),
    }
}
