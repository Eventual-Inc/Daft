mod error;
mod function;
mod options;
mod session;

pub use function::ScalarFunction;
pub use session::*;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
pub use python::register_modules;
