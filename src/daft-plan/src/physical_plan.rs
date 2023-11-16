#[cfg(feature = "python")]
use {
    crate::{
        sink_info::OutputFileInfo,
        source_info::{FileInfos, InMemoryInfo, LegacyExternalInfo},
    },
    common_io_config::IOConfig,
    daft_core::python::schema::PySchema,
    daft_core::schema::SchemaRef,
    daft_dsl::python::PyExpr,
    daft_dsl::Expr,
    daft_scan::{
        file_format::{FileFormat, FileFormatConfig, PyFileFormatConfig},
        python::pylib::PyScanTask,
        storage_config::{PyStorageConfig, StorageConfig},
        Pushdowns,
    },
    pyo3::{
        pyclass, pymethods, types::PyBytes, PyObject, PyRef, PyRefMut, PyResult, PyTypeInfo,
        Python, ToPyObject,
    },
    std::collections::HashMap,
};

use daft_core::impl_bincode_py_state_serialization;
use serde::{Deserialize, Serialize};
use std::{cmp::max, sync::Arc};

use crate::{physical_ops::*, PartitionScheme, PartitionSpec};

/// Physical plan for a Daft query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PhysicalPlan {
    #[cfg(feature = "python")]
    InMemoryScan(InMemoryScan),
    TabularScanParquet(TabularScanParquet),
    TabularScanCsv(TabularScanCsv),
    TabularScanJson(TabularScanJson),
    TabularScan(TabularScan),
    Project(Project),
    Filter(Filter),
    Limit(Limit),
    Explode(Explode),
    Sort(Sort),
    Split(Split),
    Coalesce(Coalesce),
    Flatten(Flatten),
    FanoutRandom(FanoutRandom),
    FanoutByHash(FanoutByHash),
    #[allow(dead_code)]
    FanoutByRange(FanoutByRange),
    ReduceMerge(ReduceMerge),
    Aggregate(Aggregate),
    Concat(Concat),
    Join(Join),
    TabularWriteParquet(TabularWriteParquet),
    TabularWriteJson(TabularWriteJson),
    TabularWriteCsv(TabularWriteCsv),
}

impl PhysicalPlan {
    pub fn partition_spec(&self) -> Arc<PartitionSpec> {
        match self {
            #[cfg(feature = "python")]
            Self::InMemoryScan(InMemoryScan { partition_spec, .. }) => partition_spec.clone(),
            Self::TabularScan(TabularScan { partition_spec, .. }) => partition_spec.clone(),
            Self::TabularScanParquet(TabularScanParquet { partition_spec, .. }) => {
                partition_spec.clone()
            }
            Self::TabularScanCsv(TabularScanCsv { partition_spec, .. }) => partition_spec.clone(),
            Self::TabularScanJson(TabularScanJson { partition_spec, .. }) => partition_spec.clone(),
            Self::Project(Project { partition_spec, .. }) => partition_spec.clone(),
            Self::Filter(Filter { input, .. }) => input.partition_spec(),
            Self::Limit(Limit { input, .. }) => input.partition_spec(),
            Self::Explode(Explode { input, .. }) => input.partition_spec(),
            Self::Sort(Sort { input, sort_by, .. }) => PartitionSpec::new_internal(
                PartitionScheme::Range,
                input.partition_spec().num_partitions,
                Some(sort_by.clone()),
            )
            .into(),
            Self::Split(Split {
                output_num_partitions,
                ..
            }) => {
                PartitionSpec::new_internal(PartitionScheme::Unknown, *output_num_partitions, None)
                    .into()
            }
            Self::Coalesce(Coalesce { num_to, .. }) => {
                PartitionSpec::new_internal(PartitionScheme::Unknown, *num_to, None).into()
            }
            Self::Flatten(Flatten { input }) => input.partition_spec(),
            Self::FanoutRandom(FanoutRandom { num_partitions, .. }) => {
                PartitionSpec::new_internal(PartitionScheme::Random, *num_partitions, None).into()
            }
            Self::FanoutByHash(FanoutByHash {
                num_partitions,
                partition_by,
                ..
            }) => PartitionSpec::new_internal(
                PartitionScheme::Hash,
                *num_partitions,
                Some(partition_by.clone()),
            )
            .into(),
            Self::FanoutByRange(FanoutByRange {
                num_partitions,
                sort_by,
                ..
            }) => PartitionSpec::new_internal(
                PartitionScheme::Range,
                *num_partitions,
                Some(sort_by.clone()),
            )
            .into(),
            Self::ReduceMerge(ReduceMerge { input }) => input.partition_spec(),
            Self::Aggregate(Aggregate { input, groupby, .. }) => {
                let input_partition_spec = input.partition_spec();
                if input_partition_spec.num_partitions == 1 {
                    input_partition_spec.clone()
                } else if groupby.is_empty() {
                    PartitionSpec::new_internal(PartitionScheme::Unknown, 1, None).into()
                } else {
                    PartitionSpec::new_internal(
                        PartitionScheme::Hash,
                        input.partition_spec().num_partitions,
                        Some(groupby.clone()),
                    )
                    .into()
                }
            }
            Self::Concat(Concat { input, other }) => PartitionSpec::new_internal(
                PartitionScheme::Unknown,
                input.partition_spec().num_partitions + other.partition_spec().num_partitions,
                None,
            )
            .into(),
            Self::Join(Join {
                left,
                right,
                left_on,
                ..
            }) => {
                let input_partition_spec = left.partition_spec();
                match max(
                    input_partition_spec.num_partitions,
                    right.partition_spec().num_partitions,
                ) {
                    // NOTE: This duplicates the repartitioning logic in the planner, where we
                    // conditionally repartition the left and right tables.
                    // TODO(Clark): Consolidate this logic with the planner logic when we push the partition spec
                    // to be an entirely planner-side concept.
                    1 => input_partition_spec,
                    num_partitions => PartitionSpec::new_internal(
                        PartitionScheme::Hash,
                        num_partitions,
                        Some(left_on.clone()),
                    )
                    .into(),
                }
            }
            Self::TabularWriteParquet(TabularWriteParquet { input, .. }) => input.partition_spec(),
            Self::TabularWriteCsv(TabularWriteCsv { input, .. }) => input.partition_spec(),
            Self::TabularWriteJson(TabularWriteJson { input, .. }) => input.partition_spec(),
        }
    }
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
    pub fn num_partitions(&self) -> PyResult<i64> {
        self.plan.partition_spec().get_num_partitions()
    }
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
    pushdowns: &Pushdowns,
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
            pushdowns.limit,
            is_ray_runner,
        ))?;

    Ok(py_iter.into())
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "python")]
fn tabular_write(
    py: Python<'_>,
    upstream_iter: PyObject,
    file_format: &FileFormat,
    schema: &SchemaRef,
    root_dir: &String,
    compression: &Option<String>,
    partition_cols: &Option<Vec<Expr>>,
    io_config: &Option<IOConfig>,
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
            io_config
                .as_ref()
                .map(|cfg| common_io_config::python::IOConfig {
                    config: cfg.clone(),
                }),
        ))?;
    Ok(py_iter.into())
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
            PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "scan_with_tasks"))?
                    .call1((scan_tasks
                        .iter()
                        .map(|scan_task| PyScanTask(scan_task.clone()))
                        .collect::<Vec<PyScanTask>>(),))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::TabularScanParquet(TabularScanParquet {
                projection_schema,
                external_info:
                    LegacyExternalInfo {
                        source_schema,
                        file_infos,
                        file_format_config,
                        storage_config,
                        pushdowns,
                        ..
                    },
                ..
            }) => tabular_scan(
                py,
                source_schema,
                projection_schema,
                file_infos,
                file_format_config,
                storage_config,
                pushdowns,
                is_ray_runner,
            ),
            PhysicalPlan::TabularScanCsv(TabularScanCsv {
                projection_schema,
                external_info:
                    LegacyExternalInfo {
                        source_schema,
                        file_infos,
                        file_format_config,
                        storage_config,
                        pushdowns,
                        ..
                    },
                ..
            }) => tabular_scan(
                py,
                source_schema,
                projection_schema,
                file_infos,
                file_format_config,
                storage_config,
                pushdowns,
                is_ray_runner,
            ),
            PhysicalPlan::TabularScanJson(TabularScanJson {
                projection_schema,
                external_info:
                    LegacyExternalInfo {
                        source_schema,
                        file_infos,
                        file_format_config,
                        storage_config,
                        pushdowns,
                        ..
                    },
                ..
            }) => tabular_scan(
                py,
                source_schema,
                projection_schema,
                file_infos,
                file_format_config,
                storage_config,
                pushdowns,
                is_ray_runner,
            ),
            PhysicalPlan::Project(Project {
                input,
                projection,
                resource_request,
                ..
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
                eager,
                num_partitions,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
                let py_physical_plan =
                    py.import(pyo3::intern!(py, "daft.execution.physical_plan"))?;
                let global_limit_iter = py_physical_plan
                    .getattr(pyo3::intern!(py, "global_limit"))?
                    .call1((upstream_iter, *limit, *eager, *num_partitions))?;
                Ok(global_limit_iter.into())
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
                        io_config,
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
                io_config,
            ),
            PhysicalPlan::TabularWriteCsv(TabularWriteCsv {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                        io_config,
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
                io_config,
            ),
            PhysicalPlan::TabularWriteJson(TabularWriteJson {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                        io_config,
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
                io_config,
            ),
        }
    }
}
