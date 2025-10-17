use std::collections::HashMap;

use common_daft_config::PyDaftPlanningConfig;
use daft_core::python::PyDataType;
use daft_dsl::python::PyExpr;
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use daft_session::python::PySession;
use pyo3::{IntoPyObjectExt, prelude::*};

use crate::{exec::execute_statement, functions::SQL_FUNCTIONS, schema::try_parse_dtype};

#[pyclass]
pub struct SQLFunctionStub {
    name: String,
    docstring: String,
    arg_names: Vec<&'static str>,
}

#[pymethods]
impl SQLFunctionStub {
    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.name.clone())
    }

    #[getter]
    fn docstring(&self) -> PyResult<String> {
        Ok(self.docstring.clone())
    }

    #[getter]
    fn arg_names(&self) -> PyResult<Vec<&'static str>> {
        Ok(self.arg_names.clone())
    }
}

/// This method is called via `Session.sql` returns a PyObject (typically a PyLogicalBuilder)
#[pyfunction]
pub fn sql_exec(
    py: Python<'_>,
    sql: &str,
    session: &PySession,
    ctes: HashMap<String, PyLogicalPlanBuilder>,
    config: PyDaftPlanningConfig,
) -> PyResult<Option<pyo3::Py<pyo3::PyAny>>> {
    // Prepare any externally provided CTEs.
    let ctes = ctes
        .into_iter()
        .map(|(name, builder)| (name, builder.builder))
        .collect::<HashMap<String, LogicalPlanBuilder>>();
    // Execute and convert the result to a PyObject to be handled by the python caller.
    if let Some(plan) = execute_statement(session.session(), sql, ctes)? {
        let builder = LogicalPlanBuilder::new(plan, Some(config.config));
        let builder = PyLogicalPlanBuilder::from(builder);
        let builder = builder.into_py_any(py)?;
        Ok(Some(builder))
    } else {
        Ok(None)
    }
}

#[pyfunction]
pub fn sql_expr(sql: &str) -> PyResult<PyExpr> {
    let expr = crate::planner::sql_expr(sql)?;
    Ok(PyExpr { expr })
}

#[pyfunction]
pub fn sql_datatype(sql: &str) -> PyResult<PyDataType> {
    let dtype = try_parse_dtype(sql)?;
    Ok(PyDataType { dtype })
}

#[pyfunction]
pub fn list_sql_functions() -> Vec<SQLFunctionStub> {
    SQL_FUNCTIONS
        .map
        .keys()
        .cloned()
        .map(|name| {
            let (docstring, args) = SQL_FUNCTIONS.docsmap.get(&name).unwrap();
            SQLFunctionStub {
                name,
                docstring: docstring.clone(),
                arg_names: args.to_vec(),
            }
        })
        .collect()
}
