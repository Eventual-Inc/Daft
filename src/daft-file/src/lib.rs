mod file;
mod functions;

pub(crate) use file::guess_mimetype_from_content;
pub use functions::*;

pub use crate::file::{BUFFER_SIZE_FULL, BUFFER_SIZE_METADATA, BUFFER_SIZE_SNIFF, DaftFile};

#[cfg(feature = "python")]
pub mod python;
