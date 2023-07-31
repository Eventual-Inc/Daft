use pyo3::prelude::*;

use daft_core::python::schema::PySchema;

use crate::builder;

#[pyclass]
#[derive(Clone)]
pub struct PyLogicalPlanBuilder {
    pub logical_plan_builder: crate::LogicalPlanBuilder,
}

#[pyfunction]
pub fn source(filepaths: Vec<String>, schema: &PySchema) -> PyResult<PyLogicalPlanBuilder> {
    let logical_plan_builder = builder::new_plan_from_source(filepaths, schema.schema.clone());
    let pyres = PyLogicalPlanBuilder {
        logical_plan_builder,
    };
    Ok(pyres)
}
