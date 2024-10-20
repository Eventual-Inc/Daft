use std::sync::Arc;

use pyo3::prelude::*;

use crate::AWSGlueCatalog;

/// Registers an AWS Glue catalog instance with Daft
#[pyfunction]
#[pyo3(name = "register_aws_glue_catalog")]
pub fn register_aws_glue_catalog(name: Option<&str>) -> PyResult<()> {
    let catalog = AWSGlueCatalog::new();
    daft_catalog::global_catalog::register_catalog(Arc::new(catalog), name);
    Ok(())
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(register_aws_glue_catalog))?;

    Ok(())
}
