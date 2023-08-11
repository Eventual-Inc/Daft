pub mod binary_ops;
pub mod data_array;
pub mod logical_array;

use super::Series;

#[derive(Clone)]
pub struct ArrayWrapper<T>(pub T);

pub trait IntoSeries {
    fn into_series(self) -> Series;
}
