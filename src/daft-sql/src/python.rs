use std::{collections::HashMap, sync::Arc};

use common_daft_config::PyDaftPlanningConfig;
use daft_core::python::PyDataType;
use daft_dsl::python::PyExpr;
use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder, PyLogicalPlanBuilder};
use daft_session::python::PySession;
use pyo3::{prelude::*, types::PyDict, IntoPyObjectExt};

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
) -> PyResult<Option<PyObject>> {
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
                docstring: docstring.to_string(),
                arg_names: args.to_vec(),
            }
        })
        .collect()
}

/// SQLCatalog is DEPRECATED in 0.5 and will be REMOVED in 0.6.
///
/// TODO remove once session is removed on python side
/// PyCatalog is the Python interface to the Catalog.
#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone)]
pub struct PySqlCatalog {
    tables: HashMap<String, Arc<LogicalPlan>>,
}

#[pymethods]
impl PySqlCatalog {
    /// Construct an empty PyCatalog.
    #[staticmethod]
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a table with the catalog.
    pub fn register_table(
        &mut self,
        name: &str,
        dataframe: &mut PyLogicalPlanBuilder,
    ) -> PyResult<()> {
        // TODO this is being removed, but do not parse python strings as SQL strings.
        let plan = dataframe.builder.build();
        self.tables.insert(name.to_string(), plan);
        Ok(())
    }

    /// Copy from another catalog, using tables from other in case of conflict
    pub fn copy_from(&mut self, other: &Self) {
        for (name, plan) in &other.tables {
            self.tables.insert(name.clone(), plan.clone());
        }
    }

    /// __str__ to print the catalog's tables
    fn __str__(&self) -> String {
        format!("{:?}", self.tables)
    }

    /// Convert the catalog's tables to a Python dictionary
    fn to_pydict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        for (name, plan) in &self.tables {
            let builder = PyLogicalPlanBuilder::from(LogicalPlanBuilder::new(plan.clone(), None));
            dict.set_item(name, builder)?;
        }
        Ok(dict.into())
    }
}

impl Default for PySqlCatalog {
    fn default() -> Self {
        Self::new()
    }
}
