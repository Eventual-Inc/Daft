#[cfg(feature = "python")]
use {
    crate::source_info::{ExternalInfo, FileInfo, InMemoryInfo, PyFileFormatConfig},
    daft_core::python::schema::PySchema,
    daft_core::schema::SchemaRef,
    daft_dsl::python::PyExpr,
    daft_dsl::Expr,
    daft_table::python::PyTable,
    pyo3::{pyclass, pymethods, PyObject, PyRef, PyRefMut, PyResult, Python},
    std::collections::HashMap,
    std::sync::Arc,
};

use crate::{physical_ops::*, source_info::FileFormatConfig};

#[derive(Debug)]
pub enum PhysicalPlan {
    #[cfg(feature = "python")]
    InMemoryScan(InMemoryScan),
    TabularScanParquet(TabularScanParquet),
    TabularScanCsv(TabularScanCsv),
    TabularScanJson(TabularScanJson),
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
fn tabular_scan(
    py: Python<'_>,
    schema: &SchemaRef,
    file_info: &Arc<FileInfo>,
    file_format_config: &Arc<FileFormatConfig>,
    limit: &Option<usize>,
) -> PyResult<PyObject> {
    let file_info_table: PyTable = file_info.to_table()?.into();
    let py_iter = py
        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
        .getattr(pyo3::intern!(py, "tabular_scan"))?
        .call1((
            PySchema::from(schema.clone()),
            file_info_table,
            PyFileFormatConfig::from(file_format_config.clone()),
            *limit,
        ))?;
    Ok(py_iter.into())
}

#[cfg(feature = "python")]
impl PhysicalPlan {
    pub fn to_partition_tasks(
        &self,
        py: Python<'_>,
        psets: &HashMap<String, Vec<PyObject>>,
    ) -> PyResult<PyObject> {
        match self {
            PhysicalPlan::InMemoryScan(InMemoryScan {
                in_memory_info: InMemoryInfo { cache_key, .. },
                ..
            }) => {
                let partition_iter = PartitionIterator {
                    parts: psets[cache_key].clone(),
                    index: 0usize,
                };
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                    .getattr(pyo3::intern!(py, "partition_read"))?
                    .call1((partition_iter,))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::TabularScanParquet(TabularScanParquet {
                schema,
                external_info:
                    ExternalInfo {
                        file_info,
                        file_format_config,
                        ..
                    },
                limit,
                ..
            }) => tabular_scan(py, schema, file_info, file_format_config, limit),
            PhysicalPlan::TabularScanCsv(TabularScanCsv {
                schema,
                external_info:
                    ExternalInfo {
                        file_info,
                        file_format_config,
                        ..
                    },
                limit,
                ..
            }) => tabular_scan(py, schema, file_info, file_format_config, limit),
            PhysicalPlan::TabularScanJson(TabularScanJson {
                schema,
                external_info:
                    ExternalInfo {
                        file_info,
                        file_format_config,
                        ..
                    },
                limit,
                ..
            }) => tabular_scan(py, schema, file_info, file_format_config, limit),
            PhysicalPlan::Filter(Filter { input, predicate }) => {
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
