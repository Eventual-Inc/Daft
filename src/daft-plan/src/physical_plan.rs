#[cfg(feature = "python")]
use {
    crate::source_info::PyFileFormatConfig,
    daft_core::python::schema::PySchema,
    daft_dsl::python::PyExpr,
    daft_dsl::Expr,
    daft_table::python::PyTable,
    pyo3::{pyclass, pymethods, PyAny, PyObject, PyRef, PyRefMut, PyResult, Python, ToPyObject},
};

use crate::physical_ops::*;

#[derive(Debug)]
pub enum PhysicalPlan {
    TabularScanParquet(TabularScanParquet),
    Filter(Filter),
    Limit(Limit),
    Aggregate(Aggregate),
}

#[cfg(feature = "python")]
#[pyclass]
struct PartitionIterator {
    parts: Vec<PyObject>,
    index: usize,
}

#[cfg(feature = "python")]
#[pymethods]
impl PartitionIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        let index = slf.index;
        slf.index += 1;
        slf.parts.get(index).map(|part| part.clone_ref(slf.py()))
    }
}

#[cfg(feature = "python")]
type PyScanArgs<'a> = (
    &'a PyAny,
    Option<usize>,
    &'a PyAny,
    Option<&'a PyAny>,
    Option<Vec<String>>,
    PyFileFormatConfig,
    String,
);

#[cfg(feature = "python")]
impl PhysicalPlan {
    pub fn to_partition_tasks(&self, py: Python<'_>) -> PyResult<PyObject> {
        match self {
            PhysicalPlan::TabularScanParquet(TabularScanParquet {
                schema,
                source_info,
                limit,
                ..
            }) => {
                let file_info_table: PyTable = source_info.file_info.to_table()?.into();
                let py_from_pytable_func = py
                    .import(pyo3::intern!(py, "daft.table"))?
                    .getattr(pyo3::intern!(py, "Table"))?
                    .getattr(pyo3::intern!(py, "_from_pytable"))?;
                let py_file_info_table = py_from_pytable_func.call1((file_info_table,))?;
                let py_file_info_partition_iter = PartitionIterator {
                    parts: vec![py_file_info_table.to_object(py)],
                    index: 0usize,
                };
                let py_physical_plan =
                    py.import(pyo3::intern!(py, "daft.execution.physical_plan"))?;
                let in_memory_scan_iter = py_physical_plan
                    .getattr(pyo3::intern!(py, "partition_read"))?
                    .call1((py_file_info_partition_iter,))?;
                let py_schema = py
                    .import(pyo3::intern!(py, "daft.logical.schema"))?
                    .getattr(pyo3::intern!(py, "Schema"))?
                    .getattr(pyo3::intern!(py, "_from_pyschema"))?
                    .call1((PySchema::from(schema.clone()),))?;
                let args: PyScanArgs = (
                    in_memory_scan_iter,
                    *limit,
                    py_schema,
                    None,
                    None,
                    PyFileFormatConfig::from(source_info.file_format_config.clone()),
                    "path".to_string(),
                );
                let py_iter = py_physical_plan
                    .getattr(pyo3::intern!(py, "file_read"))?
                    .call1(args)?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Filter(Filter { input, predicate }) => {
                let upstream_iter = input.to_partition_tasks(py)?;
                let expressions_mod =
                    py.import(pyo3::intern!(py, "daft.expressions.expressions"))?;
                let py_predicate = expressions_mod
                    .getattr(pyo3::intern!(py, "Expression"))?
                    .getattr(pyo3::intern!(py, "_from_pyexpr"))?
                    .call1((PyExpr::from(predicate.clone()),))?;
                let expressions_projection = expressions_mod
                    .getattr(pyo3::intern!(py, "ExpressionsProjection"))?
                    .call1((vec![py_predicate],))?;
                let execution_step_mod =
                    py.import(pyo3::intern!(py, "daft.execution.execution_step"))?;
                let filter_step = execution_step_mod
                    .getattr(pyo3::intern!(py, "Filter"))?
                    .call1((expressions_projection,))?;
                let resource_request = execution_step_mod
                    .getattr(pyo3::intern!(py, "ResourceRequest"))?
                    .call0()?;
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                    .getattr(pyo3::intern!(py, "pipeline_instruction"))?
                    .call1((upstream_iter, filter_step, resource_request))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Limit(Limit {
                input,
                limit,
                num_partitions,
            }) => {
                let upstream_iter = input.to_partition_tasks(py)?;
                let py_physical_plan =
                    py.import(pyo3::intern!(py, "daft.execution.physical_plan"))?;
                let local_limit_iter = py_physical_plan
                    .getattr(pyo3::intern!(py, "local_limit"))?
                    .call1((upstream_iter, *limit))?;
                let global_limit_iter = py_physical_plan
                    .getattr(pyo3::intern!(py, "global_limit"))?
                    .call1((local_limit_iter, *limit, *num_partitions as i64))?;
                Ok(global_limit_iter.into())
            }
            PhysicalPlan::Aggregate(Aggregate {
                aggregations,
                group_by,
                input,
                ..
            }) => {
                let upstream_iter = input.to_partition_tasks(py)?;
                let aggs_as_pyexprs: Vec<PyExpr> = aggregations
                    .iter()
                    .map(|agg_expr| PyExpr::from(Expr::Agg(agg_expr.clone())))
                    .collect();
                let groupbys_as_pyexprs: Vec<PyExpr> = group_by
                    .iter()
                    .map(|expr| PyExpr::from(expr.clone()))
                    .collect();
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "local_aggregate"))?
                    .call1((upstream_iter, aggs_as_pyexprs, groupbys_as_pyexprs))?;
                Ok(py_iter.into())
            }
        }
    }
}
