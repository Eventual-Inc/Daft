pub mod data_array;
pub mod logical_array;
pub mod nested_array;
#[cfg(feature = "python")]
pub mod python_array;

use super::Series;

#[derive(Debug)]
pub struct ArrayWrapper<T>(pub T);

pub trait IntoSeries {
    fn into_series(self) -> Series;
}
