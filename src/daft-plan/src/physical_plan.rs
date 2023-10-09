#[cfg(feature = "python")]
use {
    crate::{
        sink_info::OutputFileInfo,
        source_info::{
            ExternalInfo, FileFormat, FileFormatConfig, FileInfos, InMemoryInfo,
            PyFileFormatConfig, PyStorageConfig, StorageConfig,
        },
        PartitionSpec,
    },
    daft_core::python::schema::PySchema,
    daft_core::schema::SchemaRef,
    daft_dsl::python::PyExpr,
    daft_dsl::Expr,
    pyo3::{
        pyclass, pymethods, types::PyBytes, PyObject, PyRef, PyRefMut, PyResult, PyTypeInfo,
        Python, ToPyObject,
    },
    std::collections::HashMap,
};

use daft_core::impl_bincode_py_state_serialization;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::physical_ops::*;

/// Physical plan for a Daft query.
#[derive(Debug, Serialize, Deserialize)]
pub enum PhysicalPlan {
    #[cfg(feature = "python")]
    InMemoryScan(InMemoryScan),
    TabularScanParquet(TabularScanParquet),
    TabularScanCsv(TabularScanCsv),
    TabularScanJson(TabularScanJson),
    Project(Project),
    Filter(Filter),
    Limit(Limit),
    Explode(Explode),
    Sort(Sort),
    Split(Split),
    Flatten(Flatten),
    FanoutRandom(FanoutRandom),
    FanoutByHash(FanoutByHash),
    #[allow(dead_code)]
    FanoutByRange(FanoutByRange),
    ReduceMerge(ReduceMerge),
    Aggregate(Aggregate),
    Coalesce(Coalesce),
    Concat(Concat),
    Join(Join),
    TabularWriteParquet(TabularWriteParquet),
    TabularWriteJson(TabularWriteJson),
    TabularWriteCsv(TabularWriteCsv),
}

/// A work scheduler for physical plans.
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct PhysicalPlanScheduler {
    plan: Arc<PhysicalPlan>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PhysicalPlanScheduler {
    /// Converts the contained physical plan into an iterator of executable partition tasks.
    pub fn to_partition_tasks(
        &self,
        psets: HashMap<String, Vec<PyObject>>,
        is_ray_runner: bool,
    ) -> PyResult<PyObject> {
        Python::with_gil(|py| self.plan.to_partition_tasks(py, &psets, is_ray_runner))
    }
}

impl_bincode_py_state_serialization!(PhysicalPlanScheduler);

impl From<Arc<PhysicalPlan>> for PhysicalPlanScheduler {
    fn from(plan: Arc<PhysicalPlan>) -> Self {
        Self { plan }
    }
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
#[allow(clippy::too_many_arguments)]
fn tabular_scan(
    py: Python<'_>,
    source_schema: &SchemaRef,
    projection_schema: &SchemaRef,
    file_infos: &Arc<FileInfos>,
    file_format_config: &Arc<FileFormatConfig>,
    storage_config: &Arc<StorageConfig>,
    partition_spec: &PartitionSpec,
    limit: &Option<usize>,
    is_ray_runner: bool,
) -> PyResult<PyObject> {
    let columns_to_read = if projection_schema.names() != source_schema.names() {
        Some(
            projection_schema
                .fields
                .iter()
                .map(|(name, _)| name)
                .cloned()
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };
    let py_iter = py
        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
        .getattr(pyo3::intern!(py, "tabular_scan"))?
        .call1((
            PySchema::from(source_schema.clone()),
            columns_to_read,
            file_infos.to_table()?,
            PyFileFormatConfig::from(file_format_config.clone()),
            PyStorageConfig::from(storage_config.clone()),
            *limit,
            is_ray_runner,
        ))?;

    if let Some(limit) = limit {
        apply_limit(
            py,
            py_iter.into(),
            *limit as i64,
            partition_spec.num_partitions,
        )
    } else {
        Ok(py_iter.into())
    }
}

#[cfg(feature = "python")]
fn tabular_write(
    py: Python<'_>,
    upstream_iter: PyObject,
    file_format: &FileFormat,
    schema: &SchemaRef,
    root_dir: &String,
    compression: &Option<String>,
    partition_cols: &Option<Vec<Expr>>,
) -> PyResult<PyObject> {
    let part_cols = partition_cols.as_ref().map(|cols| {
        cols.iter()
            .map(|e| e.clone().into())
            .collect::<Vec<PyExpr>>()
    });
    let py_iter = py
        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
        .getattr(pyo3::intern!(py, "write_file"))?
        .call1((
            upstream_iter,
            file_format.clone(),
            PySchema::from(schema.clone()),
            root_dir,
            compression.clone(),
            part_cols,
        ))?;
    Ok(py_iter.into())
}

#[cfg(feature = "python")]
fn apply_limit(
    py: Python<'_>,
    upstream_iter: PyObject,
    limit: i64,
    num_partitions: usize,
) -> PyResult<PyObject> {
    let py_physical_plan = py.import(pyo3::intern!(py, "daft.execution.physical_plan"))?;
    let local_limit_iter = py_physical_plan
        .getattr(pyo3::intern!(py, "local_limit"))?
        .call1((upstream_iter, limit))?;
    let global_limit_iter = py_physical_plan
        .getattr(pyo3::intern!(py, "global_limit"))?
        .call1((local_limit_iter, limit, num_partitions))?;
    Ok(global_limit_iter.into())
}

#[cfg(feature = "python")]
impl PhysicalPlan {
    pub fn to_partition_tasks(
        &self,
        py: Python<'_>,
        psets: &HashMap<String, Vec<PyObject>>,
        is_ray_runner: bool,
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
                projection_schema,
                external_info:
                    ExternalInfo {
                        source_schema,
                        file_infos,
                        file_format_config,
                        storage_config,
                        ..
                    },
                partition_spec,
                limit,
                ..
            }) => tabular_scan(
                py,
                source_schema,
                projection_schema,
                file_infos,
                file_format_config,
                storage_config,
                partition_spec,
                limit,
                is_ray_runner,
            ),
            PhysicalPlan::TabularScanCsv(TabularScanCsv {
                projection_schema,
                external_info:
                    ExternalInfo {
                        source_schema,
                        file_infos,
                        file_format_config,
                        storage_config,
                        ..
                    },
                partition_spec,
                limit,
                ..
            }) => tabular_scan(
                py,
                source_schema,
                projection_schema,
                file_infos,
                file_format_config,
                storage_config,
                partition_spec,
                limit,
                is_ray_runner,
            ),
            PhysicalPlan::TabularScanJson(TabularScanJson {
                projection_schema,
                external_info:
                    ExternalInfo {
                        source_schema,
                        file_infos,
                        file_format_config,
                        storage_config,
                        ..
                    },
                partition_spec,
                limit,
                ..
            }) => tabular_scan(
                py,
                source_schema,
                projection_schema,
                file_infos,
                file_format_config,
                storage_config,
                partition_spec,
                limit,
                is_ray_runner,
            ),
            PhysicalPlan::Project(Project {
                input,
                projection,
                resource_request,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let projection_pyexprs: Vec<PyExpr> = projection
                    .iter()
                    .map(|expr| PyExpr::from(expr.clone()))
                    .collect();
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "project"))?
                    .call1((upstream_iter, projection_pyexprs, resource_request.clone()))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Filter(Filter { input, predicate }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                apply_limit(py, upstream_iter, *limit, *num_partitions)
            }
            PhysicalPlan::Explode(Explode { input, to_explode }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let explode_pyexprs: Vec<PyExpr> = to_explode
                    .iter()
                    .map(|expr| PyExpr::from(expr.clone()))
                    .collect();
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "explode"))?
                    .call1((upstream_iter, explode_pyexprs))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Sort(Sort {
                input,
                sort_by,
                descending,
                num_partitions,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let sort_by_pyexprs: Vec<PyExpr> = sort_by
                    .iter()
                    .map(|expr| PyExpr::from(expr.clone()))
                    .collect();
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "sort"))?
                    .call1((
                        upstream_iter,
                        sort_by_pyexprs,
                        descending.clone(),
                        *num_partitions,
                    ))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Split(Split {
                input,
                input_num_partitions,
                output_num_partitions,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                    .getattr(pyo3::intern!(py, "split"))?
                    .call1((upstream_iter, *input_num_partitions, *output_num_partitions))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Flatten(Flatten { input }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                    .getattr(pyo3::intern!(py, "flatten_plan"))?
                    .call1((upstream_iter,))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::FanoutRandom(FanoutRandom {
                input,
                num_partitions,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                    .getattr(pyo3::intern!(py, "fanout_random"))?
                    .call1((upstream_iter, *num_partitions))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::FanoutByHash(FanoutByHash {
                input,
                num_partitions,
                partition_by,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let partition_by_pyexprs: Vec<PyExpr> = partition_by
                    .iter()
                    .map(|expr| PyExpr::from(expr.clone()))
                    .collect();
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "split_by_hash"))?
                    .call1((upstream_iter, *num_partitions, partition_by_pyexprs))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::FanoutByRange(_) => unimplemented!(
                "FanoutByRange not implemented, since only use case (sorting) doesn't need it yet."
            ),
            PhysicalPlan::ReduceMerge(ReduceMerge { input }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "reduce_merge"))?
                    .call1((upstream_iter,))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Aggregate(Aggregate {
                aggregations,
                groupby,
                input,
                ..
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let aggs_as_pyexprs: Vec<PyExpr> = aggregations
                    .iter()
                    .map(|agg_expr| PyExpr::from(Expr::Agg(agg_expr.clone())))
                    .collect();
                let groupbys_as_pyexprs: Vec<PyExpr> = groupby
                    .iter()
                    .map(|expr| PyExpr::from(expr.clone()))
                    .collect();
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "local_aggregate"))?
                    .call1((upstream_iter, aggs_as_pyexprs, groupbys_as_pyexprs))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Coalesce(Coalesce {
                input,
                num_from,
                num_to,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                    .getattr(pyo3::intern!(py, "coalesce"))?
                    .call1((upstream_iter, *num_from, *num_to))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Concat(Concat { other, input }) => {
                let upstream_input_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let upstream_other_iter = other.to_partition_tasks(py, psets, is_ray_runner)?;
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                    .getattr(pyo3::intern!(py, "concat"))?
                    .call1((upstream_input_iter, upstream_other_iter))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Join(Join {
                left,
                right,
                left_on,
                right_on,
                join_type,
                ..
            }) => {
                let upstream_left_iter = left.to_partition_tasks(py, psets, is_ray_runner)?;
                let upstream_right_iter = right.to_partition_tasks(py, psets, is_ray_runner)?;
                let left_on_pyexprs: Vec<PyExpr> = left_on
                    .iter()
                    .map(|expr| PyExpr::from(expr.clone()))
                    .collect();
                let right_on_pyexprs: Vec<PyExpr> = right_on
                    .iter()
                    .map(|expr| PyExpr::from(expr.clone()))
                    .collect();
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "join"))?
                    .call1((
                        upstream_left_iter,
                        upstream_right_iter,
                        left_on_pyexprs,
                        right_on_pyexprs,
                        *join_type,
                    ))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::TabularWriteParquet(TabularWriteParquet {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                    },
                input,
            }) => tabular_write(
                py,
                input.to_partition_tasks(py, psets, is_ray_runner)?,
                file_format,
                schema,
                root_dir,
                compression,
                partition_cols,
            ),
            PhysicalPlan::TabularWriteCsv(TabularWriteCsv {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                    },
                input,
            }) => tabular_write(
                py,
                input.to_partition_tasks(py, psets, is_ray_runner)?,
                file_format,
                schema,
                root_dir,
                compression,
                partition_cols,
            ),
            PhysicalPlan::TabularWriteJson(TabularWriteJson {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                    },
                input,
            }) => tabular_write(
                py,
                input.to_partition_tasks(py, psets, is_ray_runner)?,
                file_format,
                schema,
                root_dir,
                compression,
                partition_cols,
            ),
        }
    }
}
