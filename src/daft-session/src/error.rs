// temporary
pub use daft_catalog::error::*;

#[macro_export]
macro_rules! unsupported_err {
    ($($arg:tt)*) => {
        return Err($crate::errors::Error::unsupported(format!($($arg)*)))
    };
}
