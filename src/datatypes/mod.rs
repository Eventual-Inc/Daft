mod dtype;
mod field;
mod time_unit;

pub use dtype::DataType;
pub use field::Field;
pub use time_unit::TimeUnit;

pub trait DaftDataType {
    fn get_dtype() -> DataType
    where
        Self: Sized;
}

macro_rules! impl_daft_datatype {
    ($ca:ident, $variant:ident) => {
        pub struct $ca {}

        impl DaftDataType for $ca {
            #[inline]
            fn get_dtype() -> DataType {
                DataType::$variant
            }
        }
    };
}

impl_daft_datatype!(Utf8Type, Utf8);
impl_daft_datatype!(Int64Type, Int64);
