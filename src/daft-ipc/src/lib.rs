//! Arrow IPC file format readers (Apache Arrow IPC file layout with footer).

mod read;
mod schema;

pub use read::{ArrowIpcReadOptions, stream_arrow_ipc_file};
pub use schema::read_arrow_ipc_file_schema;
