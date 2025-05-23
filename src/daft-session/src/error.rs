// temporary
pub use daft_catalog::error::*;

#[macro_export]
macro_rules! unsupported_err {
    ($($arg:tt)*) => {
        return Err($crate::error::CatalogError::unsupported(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! invalid_identifier_err {
    ($($arg:tt)*) => {
        return Err($crate::error::CatalogError::invalid_identifier(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! obj_already_exists_err {
    ($typ_:literal, $name:expr) => {
        return Err($crate::error::CatalogError::obj_already_exists(
            $typ_.to_string(),
            $name,
        ))
    };
}

#[macro_export]
macro_rules! obj_not_found_err {
    ($typ_:literal, $name:expr) => {
        return Err($crate::error::CatalogError::obj_not_found(
            $typ_.to_string(),
            $name,
        ))
    };
}

#[macro_export]
macro_rules! ambiguous_identifier_err {
    ($typ_:literal, $name:expr) => {
        return Err($crate::error::CatalogError::ambiguous_identifier(
            $typ_.to_string(),
            $name,
        ))
    };
}
