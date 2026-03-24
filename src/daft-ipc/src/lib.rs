//! Arrow IPC file format readers (Apache Arrow IPC file layout with footer).

mod read;
mod schema;

pub use read::{ArrowIpcReadOptions, stream_ipc_file, stream_ipc_stream};
pub use schema::{read_ipc_file_schema, read_ipc_stream_schema};
