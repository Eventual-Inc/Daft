use pyo3::{
    pyclass, pymethods,
    types::{PyModule, PyModuleMethods},
    Bound, PyResult,
};
use tracing::{info_span, span::EnteredSpan};

use crate::otel::flush_opentelemetry_providers;

#[pyclass]
struct QuerySpan {}

#[pymethods]
impl QuerySpan {
    #[new]
    pub fn new() -> Self {
        flush_opentelemetry_providers();
        Self {}
    }

    pub fn enter(&mut self) -> EnteredSpanWrapper {
        println!("Entering span (Daft Query)");
        let span = info_span!("Daft Query").entered();
        EnteredSpanWrapper::new(span)
    }
}

impl Drop for QuerySpan {
    fn drop(&mut self) {
        println!("Dropping span (DaftQueryBoys)");
        flush_opentelemetry_providers();
    }
}

#[pyclass(unsendable)]
pub struct EnteredSpanWrapper {
    _entered_span: EnteredSpan,
}

impl EnteredSpanWrapper {
    pub fn new(span: EnteredSpan) -> Self {
        Self {
            _entered_span: span,
        }
    }
}

#[pyclass]
struct Otel {}

#[pymethods]
impl Otel {
    #[new]
    pub fn new() -> Self {
        Self {}
    }

    pub fn flush(&self) {
        flush_opentelemetry_providers();
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<QuerySpan>()?;
    parent.add_class::<EnteredSpanWrapper>()?;
    parent.add_class::<Otel>()?;

    Ok(())
}
