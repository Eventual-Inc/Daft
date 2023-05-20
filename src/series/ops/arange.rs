use crate::{
    datatypes::Int64Array,
    error::DaftResult,
    series::{IntoSeries, Series},
};

impl Series {
    pub fn arange<S: AsRef<str>>(name: S, start: i64, end: i64, step: usize) -> DaftResult<Self> {
        Ok(Int64Array::arange(name, start, end, step)?.into_series())
    }
}
