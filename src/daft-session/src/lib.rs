mod error;
mod function;
mod options;
mod session;
mod source;

pub use function::ScalarFunction;
pub use session::*;
pub use source::DataSource;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
pub use python::register_modules;
