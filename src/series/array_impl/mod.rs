pub mod data_array;
pub mod logical_array;

use super::Series;

pub struct ArrayWrapper<T>(pub T);

pub trait IntoSeries {
    fn into_series(self) -> Series;
}
