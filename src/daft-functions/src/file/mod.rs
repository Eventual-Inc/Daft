mod file;
mod functions;

pub(crate) use file::guess_mimetype_from_content;
pub use functions::*;

pub use self::file::DaftFile;

#[cfg(feature = "python")]
#[path = "python_mod.rs"]
pub mod python;
