use std::sync::Arc;

use daft_core::{prelude::SchemaRef, python::PySchema};
use daft_dsl::LiteralValue;
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use indexmap::IndexMap;
use pyo3::{intern, prelude::*, types::PyList};

use super::PyIdentifier;
use crate::{error::CatalogResult, Catalog, Identifier, Table, TableRef};

/// Newtype to implement the Catalog trait for a Python catalog
#[derive(Debug)]
pub struct PyCatalogWrapper(pub(super) PyObject);

impl Catalog for PyCatalogWrapper {
    fn name(&self) -> String {
        Python::with_gil(|py| {
            // catalog = 'python catalog object'
            let catalog = self.0.bind(py);
            // name = catalog.name
            let name = catalog
                .getattr(intern!(py, "name"))
                .expect("Catalog.name should never fail");
            let name: String = name.extract().expect("name must be a string");
            name
        })
    }

    fn to_py(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.0.clone_ref(py))
    }

    fn create_namespace(&self, ident: &Identifier) -> CatalogResult<()> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            catalog.call_method1(intern!(py, "_create_namespace"), (ident,))?;
            Ok(())
        })
    }

    fn has_namespace(&self, ident: &Identifier) -> CatalogResult<bool> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            Ok(catalog
                .call_method1(intern!(py, "_has_namespace"), (ident,))?
                .extract()?)
        })
    }

    fn drop_namespace(&self, ident: &Identifier) -> CatalogResult<()> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            catalog.call_method1(intern!(py, "_drop_namespace"), (ident,))?;
            Ok(())
        })
    }

    fn list_namespaces(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let namespaces_py =
                catalog.call_method1(intern!(py, "_list_namespaces"), (pattern,))?;
            Ok(namespaces_py
                .downcast::<PyList>()
                .expect("Catalog._list_namespaces must return a list")
                .into_iter()
                .map(|ident| {
                    Ok(ident
                        .getattr(intern!(py, "_ident"))?
                        .extract::<PyIdentifier>()?
                        .0)
                })
                .collect::<PyResult<Vec<_>>>()?)
        })
    }

    fn create_table(&self, ident: &Identifier, schema: SchemaRef) -> CatalogResult<TableRef> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            let schema = PySchema { schema };
            let schema_py = py
                .import(intern!(py, "daft.schema"))?
                .getattr(intern!(py, "Schema"))?
                .call_method1(intern!(py, "_from_pyschema"), (schema,))?;

            let table = catalog.call_method1(intern!(py, "_create_table"), (ident, schema_py))?;
            Ok(Arc::new(PyTableWrapper(table.unbind())) as Arc<dyn Table>)
        })
    }

    fn has_table(&self, ident: &Identifier) -> CatalogResult<bool> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            Ok(catalog
                .call_method1(intern!(py, "_has_table"), (ident,))?
                .extract()?)
        })
    }

    fn drop_table(&self, ident: &Identifier) -> CatalogResult<()> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            catalog.call_method1(intern!(py, "_drop_table"), (ident,))?;
            Ok(())
        })
    }

    fn list_tables(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let namespaces_py = catalog.call_method1(intern!(py, "_list_tables"), (pattern,))?;
            Ok(namespaces_py
                .downcast::<PyList>()
                .expect("Catalog._list_tables must return a list")
                .into_iter()
                .map(|ident| {
                    Ok(ident
                        .getattr(intern!(py, "_ident"))?
                        .extract::<PyIdentifier>()?
                        .0)
                })
                .collect::<PyResult<Vec<_>>>()?)
        })
    }

    fn get_table(&self, ident: &Identifier) -> CatalogResult<TableRef> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            let table = catalog.call_method1(intern!(py, "_get_table"), (ident,))?;
            Ok(Arc::new(PyTableWrapper(table.unbind())) as Arc<dyn Table>)
        })
    }
}

/// Newtype to implement the Table trait for a Python table
#[derive(Debug)]
pub struct PyTableWrapper(pub(super) PyObject);

impl Table for PyTableWrapper {
    fn name(&self) -> String {
        Python::with_gil(|py| {
            let table = self.0.bind(py);
            table
                .getattr(intern!(py, "name"))
                .expect("Table.name() should never fail")
                .extract()
                .expect("Table.name() must return a string")
        })
    }

    fn schema(&self) -> CatalogResult<SchemaRef> {
        Python::with_gil(|py| {
            let table = self.0.bind(py);
            let schema_py = table.call_method0(intern!(py, "schema"))?;
            let schema = schema_py
                .getattr(intern!(py, "_schema"))?
                .downcast::<PySchema>()
                .expect("downcast to PySchema failed")
                .borrow();
            Ok(schema.schema.clone())
        })
    }

    fn to_logical_plan(&self) -> CatalogResult<LogicalPlanBuilder> {
        Python::with_gil(|py| {
            // table = 'python table object'
            let table = self.0.bind(py);
            // df = table.read()
            let df = table.call_method0(intern!(py, "read"))?;
            // builder = df._builder()
            let builder_py = df
                .getattr(intern!(py, "_builder"))?
                .getattr(intern!(py, "_builder"))?;
            // builder as PyLogicalPlanBuilder
            let builder = builder_py
                .downcast::<PyLogicalPlanBuilder>()
                .expect("downcast to PyLogicalPlanBuilder failed")
                .borrow();
            Ok(builder.builder.clone())
        })
    }

    fn append(
        &self,
        plan: LogicalPlanBuilder,
        options: IndexMap<String, LiteralValue>,
    ) -> CatalogResult<()> {
        Python::with_gil(|py| {
            let table = self.0.bind(py);
            let plan_py = PyLogicalPlanBuilder { builder: plan };
            let df = py
                .import(intern!(py, "daft.dataframe"))?
                .getattr(intern!(py, "DataFrame"))?
                .call1((plan_py,))?;
            table.call_method1(intern!(py, "append"), (df, options))?;
            Ok(())
        })
    }

    fn overwrite(
        &self,
        plan: LogicalPlanBuilder,
        options: IndexMap<String, LiteralValue>,
    ) -> CatalogResult<()> {
        Python::with_gil(|py| {
            let table = self.0.bind(py);
            let plan_py = PyLogicalPlanBuilder { builder: plan };
            let df = py
                .import(intern!(py, "daft.dataframe"))?
                .getattr(intern!(py, "DataFrame"))?
                .call1((plan_py,))?;
            table.call_method1(intern!(py, "overwrite"), (df, options))?;
            Ok(())
        })
    }

    fn to_py(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.0.clone_ref(py))
    }
}
