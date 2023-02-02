use pyo3::prelude::*;

use crate::series;

#[pyclass]
pub struct PySeries {
    pub series: series::Series,
}

impl From<series::Series> for PySeries {
    fn from(value: series::Series) -> Self {
        PySeries { series: value }
    }
}

impl From<PySeries> for series::Series {
    fn from(item: PySeries) -> Self {
        item.series
    }
}
