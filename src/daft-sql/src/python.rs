use std::{collections::HashMap, rc::Rc, sync::Arc};

use common_daft_config::PyDaftPlanningConfig;
use daft_catalog::TableSource;
use daft_dsl::python::PyExpr;
use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder, PyLogicalPlanBuilder};
use daft_session::{python::PySession, Session};
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::{
    error::PlannerError, functions::SQL_FUNCTIONS, planner::SQLPlanner, statement::Statement,
};

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
    config: PyDaftPlanningConfig,
) -> PyResult<Option<PyObject>> {
    let sess: Rc<Session> = Rc::new(session.into());
    let stmt = SQLPlanner::new(sess.clone()).plan(sql)?;
    match stmt {
        Statement::Select(select) => {
            let builder = LogicalPlanBuilder::new(select, Some(config.config));
            let builder = PyLogicalPlanBuilder::from(builder);
            let builder = builder.into_py_any(py)?;
            Ok(Some(builder))
        }
        Statement::Set(_) => Err(PlannerError::unsupported_sql(
            "SET statement is not yet supported.".to_string(),
        ))?,
        Statement::Use(use_) => {
            sess.set_catalog(Some(&use_.catalog))?;
            sess.set_namespace(use_.namespace.as_ref())?;
            Ok(None)
        }
    }
}

#[pyfunction]
pub fn sql(
    sql: &str,
    catalog: PyCatalog,
    daft_planning_config: PyDaftPlanningConfig,
) -> PyResult<PyLogicalPlanBuilder> {
    // TODO deprecated catalog APIs #3819
    let session = Session::empty();
    for (name, view) in catalog.tables {
        session.create_temp_table(name, &TableSource::View(view), true)?;
    }
    let mut planner = SQLPlanner::new(session.into());
    let plan = planner.plan_sql(sql)?;
    Ok(LogicalPlanBuilder::new(plan, Some(daft_planning_config.config)).into())
}

#[pyfunction]
pub fn sql_expr(sql: &str) -> PyResult<PyExpr> {
    let expr = crate::planner::sql_expr(sql)?;
    Ok(PyExpr { expr })
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

/// TODO remove once session is merged on python side.
/// PyCatalog is the Python interface to the Catalog.
#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone)]
pub struct PyCatalog {
    tables: HashMap<String, Arc<LogicalPlan>>,
}

#[pymethods]
impl PyCatalog {
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
}

impl Default for PyCatalog {
    fn default() -> Self {
        Self::new()
    }
}
