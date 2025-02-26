// temporary
pub use daft_catalog::error::*;

#[macro_export]
macro_rules! unsupported_err {
    ($($arg:tt)*) => {
        return Err($crate::error::Error::unsupported(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! invalid_identifier_err {
    ($($arg:tt)*) => {
        return Err($crate::error::Error::invalid_identifier(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! obj_already_exists_err {
    ($typ_:literal, $name:expr) => {
        return Err($crate::error::Error::obj_already_exists(
            $typ_.to_string(),
            $name,
        ))
    };
}

#[macro_export]
macro_rules! obj_not_found_err {
    ($typ_:literal, $name:expr) => {
        return Err($crate::error::Error::obj_not_found(
            $typ_.to_string(),
            $name,
        ))
    };
}
