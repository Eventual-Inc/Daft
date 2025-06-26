use std::sync::Arc;

use common_display::mermaid::MermaidDisplayOptions;
use common_error::DaftResult;
use common_file_formats::FileFormat;
#[cfg(feature = "python")]
use common_file_formats::WriteMode;
use common_py_serde::impl_bincode_py_state_serialization;
use daft_dsl::ExprRef;
use daft_logical_plan::InMemoryInfo;
#[cfg(feature = "python")]
use daft_logical_plan::{DataSinkInfo, DeltaLakeCatalogInfo, IcebergCatalogInfo, LanceCatalogInfo};
#[cfg(feature = "python")]
use daft_physical_plan::ops::{DeltaLakeWrite, IcebergWrite, LanceWrite};
use daft_physical_plan::{
    logical_to_physical,
    ops::{
        ActorPoolProject, Aggregate, BroadcastJoin, Concat, Dedup, EmptyScan, Explode, Filter,
        HashJoin, InMemoryScan, Limit, MonotonicallyIncreasingId, Pivot, Project, Sample, Sort,
        SortMergeJoin, TabularScan, TabularWriteCsv, TabularWriteJson, TabularWriteParquet, TopN,
        Unpivot,
    },
    PhysicalPlan, PhysicalPlanRef, QueryStageOutput,
};
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    common_io_config::IOConfig,
    daft_core::prelude::SchemaRef,
    daft_core::python::PySchema,
    daft_dsl::python::PyExpr,
    daft_logical_plan::{OutputFileInfo, PyLogicalPlanBuilder},
    daft_scan::python::pylib::PyScanTask,
    pyo3::{
        pyclass, pymethods,
        types::{PyAnyMethods, PyDict, PyList},
        Bound, Py, PyAny, PyObject, PyRef, PyRefMut, PyResult, Python,
    },
};

/// A work scheduler for physical plans.
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct PhysicalPlanScheduler {
    query_stage: Arc<QueryStageOutput>,
}

impl PhysicalPlanScheduler {
    fn plan(&self) -> PhysicalPlanRef {
        match self.query_stage.as_ref() {
            QueryStageOutput::Partial { physical_plan, .. } => physical_plan.clone(),
            QueryStageOutput::Final { physical_plan } => physical_plan.clone(),
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PhysicalPlanScheduler {
    #[staticmethod]
    pub fn from_logical_plan_builder(
        logical_plan_builder: &PyLogicalPlanBuilder,
        py: Python,
        cfg: PyDaftExecutionConfig,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let logical_plan = logical_plan_builder.builder.build();
            let physical_plan: PhysicalPlanRef =
                logical_to_physical(logical_plan, cfg.config.clone())?;
            Ok(QueryStageOutput::Final { physical_plan }.into())
        })
    }

    pub fn num_partitions(&self) -> PyResult<i64> {
        Ok(self.plan().clustering_spec().num_partitions() as i64)
    }

    pub fn repr_ascii(&self, simple: bool) -> PyResult<String> {
        Ok(self.plan().repr_ascii(simple))
    }

    pub fn repr_mermaid(&self, options: MermaidDisplayOptions) -> PyResult<String> {
        use common_display::mermaid::MermaidDisplay;
        Ok(self.plan().repr_mermaid(options))
    }

    pub fn to_json_string(&self) -> PyResult<String> {
        serde_json::to_string(&self.plan())
            .map_err(|e| pyo3::PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }
    /// Converts the contained physical plan into an iterator of executable partition tasks.
    pub fn to_partition_tasks(
        &self,
        py: Python,
        psets: Bound<PyDict>,
        actor_pool_manager: PyObject,
    ) -> PyResult<PyObject> {
        physical_plan_to_partition_tasks(self.plan().as_ref(), py, &psets, &actor_pool_manager)
    }
}

#[cfg(feature = "python")]
#[pyclass]
struct StreamingPartitionIterator {
    iter: Box<dyn Iterator<Item = DaftResult<PyObject>> + Send + Sync>,
}

#[cfg(feature = "python")]
#[pymethods]
impl StreamingPartitionIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python) -> PyResult<Option<PyObject>> {
        let iter = &mut slf.iter;
        Ok(py.allow_threads(|| iter.next().transpose())?)
    }
}

impl_bincode_py_state_serialization!(PhysicalPlanScheduler);

impl From<QueryStageOutput> for PhysicalPlanScheduler {
    fn from(query_stage: QueryStageOutput) -> Self {
        Self {
            query_stage: Arc::new(query_stage),
        }
    }
}

#[cfg(feature = "python")]
#[pyclass]
struct PartitionIterator {
    parts: Py<PyList>,
    index: usize,
}

#[cfg(feature = "python")]
#[pymethods]
impl PartitionIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<Bound<PyAny>> {
        let index = slf.index;
        slf.index += 1;
        slf.parts.bind(slf.py()).get_item(index).ok()
    }
}

#[cfg(feature = "python")]
fn exprs_to_pyexprs(exprs: &[ExprRef]) -> Vec<PyExpr> {
    exprs.iter().map(|e| e.clone().into()).collect()
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "python")]
fn tabular_write(
    py: Python,
    upstream_iter: PyObject,
    write_mode: &WriteMode,
    file_format: &FileFormat,
    schema: &SchemaRef,
    root_dir: &String,
    compression: Option<&String>,
    partition_cols: Option<&Vec<ExprRef>>,
    io_config: Option<&IOConfig>,
) -> PyResult<PyObject> {
    let py_iter = py
        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
        .getattr(pyo3::intern!(py, "write_file"))?
        .call1((
            upstream_iter,
            *write_mode,
            *file_format,
            PySchema::from(schema.clone()),
            root_dir,
            compression,
            partition_cols.map(|cols| exprs_to_pyexprs(cols)),
            io_config.map(|cfg| common_io_config::python::IOConfig {
                config: cfg.clone(),
            }),
        ))?;
    Ok(py_iter.into())
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "python")]
fn iceberg_write(
    py: Python,
    upstream_iter: PyObject,
    iceberg_info: &IcebergCatalogInfo,
) -> PyResult<PyObject> {
    let py_iter = py
        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
        .getattr(pyo3::intern!(py, "write_iceberg"))?
        .call1((
            upstream_iter,
            &iceberg_info.table_location,
            &iceberg_info.iceberg_schema.clone_ref(py),
            &iceberg_info.iceberg_properties.clone_ref(py),
            iceberg_info.partition_spec_id,
            exprs_to_pyexprs(&iceberg_info.partition_cols),
            iceberg_info
                .io_config
                .as_ref()
                .map(|cfg| common_io_config::python::IOConfig {
                    config: cfg.clone(),
                }),
        ))?;
    Ok(py_iter.into())
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "python")]
fn deltalake_write(
    py: Python,
    upstream_iter: PyObject,
    delta_lake_info: &DeltaLakeCatalogInfo,
) -> PyResult<PyObject> {
    let py_iter = py
        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
        .getattr(pyo3::intern!(py, "write_deltalake"))?
        .call1((
            upstream_iter,
            &delta_lake_info.path,
            delta_lake_info.large_dtypes,
            delta_lake_info.version,
            delta_lake_info.partition_cols.as_ref().map(|cols| {
                cols.iter()
                    .cloned()
                    .map(|col| col.into())
                    .collect::<Vec<PyExpr>>()
            }),
            delta_lake_info
                .io_config
                .as_ref()
                .map(|cfg| common_io_config::python::IOConfig {
                    config: cfg.clone(),
                }),
        ))?;
    Ok(py_iter.into())
}
#[allow(clippy::too_many_arguments)]
#[cfg(feature = "python")]
fn lance_write(
    py: Python,
    upstream_iter: PyObject,
    lance_info: &LanceCatalogInfo,
) -> PyResult<PyObject> {
    let py_iter = py
        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
        .getattr(pyo3::intern!(py, "write_lance"))?
        .call1((
            upstream_iter,
            &lance_info.path,
            lance_info.mode.clone(),
            lance_info
                .io_config
                .as_ref()
                .map(|cfg| common_io_config::python::IOConfig {
                    config: cfg.clone(),
                }),
            lance_info.kwargs.clone_ref(py),
        ))?;
    Ok(py_iter.into())
}

#[cfg(feature = "python")]
fn data_sink_write(
    py: Python,
    upstream_iter: PyObject,
    data_sink_info: &DataSinkInfo,
) -> PyResult<PyObject> {
    let py_iter = py
        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
        .getattr(pyo3::intern!(py, "write_data_sink"))?
        .call1((upstream_iter, &data_sink_info.sink.clone_ref(py)))?;
    Ok(py_iter.into())
}

#[cfg(feature = "python")]
fn physical_plan_to_partition_tasks(
    physical_plan: &PhysicalPlan,
    py: Python,
    psets: &Bound<PyDict>,
    actor_pool_manager: &PyObject,
) -> PyResult<PyObject> {
    use daft_dsl::Expr;
    use daft_physical_plan::ops::{CrossJoin, DataSink, ShuffleExchange, ShuffleExchangeStrategy};
    match physical_plan {
        PhysicalPlan::PreviousStageScan(..) => {
            panic!("PreviousStageScan should be optimized away before reaching the scheduler")
        }
        PhysicalPlan::InMemoryScan(InMemoryScan {
            in_memory_info: InMemoryInfo { cache_key, .. },
            ..
        }) => {
            let partition_iter = PartitionIterator {
                parts: psets.get_item(cache_key)?.extract()?,
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
                    .map(|scan_task| PyScanTask(scan_task.clone().as_any_arc().downcast().unwrap()))
                    .collect::<Vec<PyScanTask>>(),))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => {
            let schema_mod = py.import(pyo3::intern!(py, "daft.logical.schema"))?;
            let python_schema = schema_mod
                .getattr(pyo3::intern!(py, "Schema"))?
                .getattr(pyo3::intern!(py, "_from_pyschema"))?
                .call1((PySchema {
                    schema: schema.clone(),
                },))?;

            let py_iter = py
                .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                .getattr(pyo3::intern!(py, "empty_scan"))?
                .call1((python_schema,))?;
            Ok(py_iter.into())
        }

        PhysicalPlan::Project(
            project @ Project {
                input, projection, ..
            },
        ) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let projection_pyexprs: Vec<PyExpr> = projection
                .iter()
                .map(|expr| PyExpr::from(expr.clone()))
                .collect();
            let py_iter = py
                .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                .getattr(pyo3::intern!(py, "project"))?
                .call1((
                    upstream_iter,
                    projection_pyexprs,
                    project.resource_request(),
                ))?;
            Ok(py_iter.into())
        }

        PhysicalPlan::ActorPoolProject(
            app @ ActorPoolProject {
                input, projection, ..
            },
        ) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let py_iter = py
                .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                .getattr(pyo3::intern!(py, "actor_pool_project"))?
                .call1((
                    upstream_iter,
                    projection
                        .iter()
                        .map(|expr| PyExpr::from(expr.clone()))
                        .collect::<Vec<_>>(),
                    actor_pool_manager,
                    app.resource_request(),
                    app.concurrency(),
                ))?;
            Ok(py_iter.into())
        }

        PhysicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let expressions_mod = py.import(pyo3::intern!(py, "daft.expressions.expressions"))?;
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
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let py_physical_plan = py.import(pyo3::intern!(py, "daft.execution.physical_plan"))?;
            let global_limit_iter = py_physical_plan
                .getattr(pyo3::intern!(py, "global_limit"))?
                .call1((upstream_iter, *limit, *eager, *num_partitions))?;
            Ok(global_limit_iter.into())
        }
        PhysicalPlan::Explode(Explode {
            input, to_explode, ..
        }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
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
        PhysicalPlan::Unpivot(Unpivot {
            input,
            ids,
            values,
            variable_name,
            value_name,
            ..
        }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let ids_pyexprs: Vec<PyExpr> =
                ids.iter().map(|expr| PyExpr::from(expr.clone())).collect();
            let values_pyexprs: Vec<PyExpr> = values
                .iter()
                .map(|expr| PyExpr::from(expr.clone()))
                .collect();
            let py_iter = py
                .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                .getattr(pyo3::intern!(py, "unpivot"))?
                .call1((
                    upstream_iter,
                    ids_pyexprs,
                    values_pyexprs,
                    variable_name,
                    value_name,
                ))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::Sample(Sample {
            input,
            fraction,
            with_replacement,
            seed,
        }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let py_iter = py
                .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                .getattr(pyo3::intern!(py, "sample"))?
                .call1((upstream_iter, *fraction, *with_replacement, *seed))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
            input,
            column_name,
        }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let py_iter = py
                .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                .getattr(pyo3::intern!(py, "monotonically_increasing_id"))?
                .call1((upstream_iter, column_name))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            nulls_first,
            num_partitions,
        }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
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
                    nulls_first.clone(),
                    *num_partitions,
                ))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::TopN(TopN {
            input,
            sort_by,
            descending,
            nulls_first,
            limit,
            num_partitions,
        }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let py_physical_plan =
                py.import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?;

            let sort_by_pyexprs: Vec<PyExpr> = sort_by
                .iter()
                .map(|expr| PyExpr::from(expr.clone()))
                .collect();
            let global_limit_iter = py_physical_plan
                .getattr(pyo3::intern!(py, "top_n"))?
                .call1((
                    upstream_iter,
                    sort_by_pyexprs,
                    descending.clone(),
                    nulls_first.clone(),
                    *limit,
                    *num_partitions,
                ))?;
            Ok(global_limit_iter.into())
        }
        PhysicalPlan::ShuffleExchange(ShuffleExchange { input, strategy }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let input_num_partitions = input.clustering_spec().num_partitions();
            match strategy {
                ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce { target_spec } => {
                    let mapped = match target_spec.as_ref() {
                        daft_logical_plan::ClusteringSpec::Hash(hash_clustering_config) => {
                            let partition_by_pyexprs: Vec<PyExpr> = hash_clustering_config
                                .by
                                .iter()
                                .map(|expr| PyExpr::from(expr.clone()))
                                .collect();
                            py.import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                                .getattr(pyo3::intern!(py, "fanout_by_hash"))?
                                .call1((
                                    upstream_iter,
                                    hash_clustering_config.num_partitions,
                                    partition_by_pyexprs,
                                ))?
                        }
                        daft_logical_plan::ClusteringSpec::Random(random_clustering_config) => py
                            .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                            .getattr(pyo3::intern!(py, "fanout_random"))?
                            .call1((upstream_iter, random_clustering_config.num_partitions()))?,
                        daft_logical_plan::ClusteringSpec::Range(_) => {
                            unimplemented!("FanoutByRange not implemented, since only use case (sorting) doesn't need it yet.");
                        }
                        daft_logical_plan::ClusteringSpec::Unknown(_) => {
                            unreachable!("Cannot use NaiveFullyMaterializingMapReduce ShuffleExchange to map to an Unknown ClusteringSpec");
                        }
                    };
                    let reduced = py
                        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                        .getattr(pyo3::intern!(py, "reduce_merge"))?
                        .call1((mapped,))?;
                    Ok(reduced.into())
                }
                ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge {
                    target_spec,
                    pre_shuffle_merge_threshold,
                } => {
                    let merged = py
                        .import(pyo3::intern!(
                            py,
                            "daft.execution.shuffles.pre_shuffle_merge"
                        ))?
                        .getattr(pyo3::intern!(py, "pre_shuffle_merge"))?
                        .call1((upstream_iter, *pre_shuffle_merge_threshold))?;
                    let mapped = match target_spec.as_ref() {
                        daft_logical_plan::ClusteringSpec::Hash(hash_clustering_config) => {
                            let partition_by_pyexprs: Vec<PyExpr> = hash_clustering_config
                                .by
                                .iter()
                                .map(|expr| PyExpr::from(expr.clone()))
                                .collect();
                            py.import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                                .getattr(pyo3::intern!(py, "fanout_by_hash"))?
                                .call1((
                                    merged,
                                    hash_clustering_config.num_partitions,
                                    partition_by_pyexprs,
                                ))?
                        }
                        daft_logical_plan::ClusteringSpec::Random(random_clustering_config) => py
                            .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                            .getattr(pyo3::intern!(py, "fanout_random"))?
                            .call1((merged, random_clustering_config.num_partitions()))?,
                        daft_logical_plan::ClusteringSpec::Range(_) => {
                            unimplemented!("FanoutByRange not implemented, since only use case (sorting) doesn't need it yet.");
                        }
                        daft_logical_plan::ClusteringSpec::Unknown(_) => {
                            unreachable!("Cannot use NaiveFullyMaterializingMapReduce ShuffleExchange to map to an Unknown ClusteringSpec");
                        }
                    };
                    let reduced = py
                        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                        .getattr(pyo3::intern!(py, "reduce_merge"))?
                        .call1((mapped,))?;
                    Ok(reduced.into())
                }
                ShuffleExchangeStrategy::FlightShuffle {
                    target_spec,
                    shuffle_dirs,
                } => {
                    let shuffled = match target_spec.as_ref() {
                        daft_logical_plan::ClusteringSpec::Hash(hash_clustering_config) => {
                            let partition_by_pyexprs: Vec<PyExpr> = hash_clustering_config
                                .by
                                .iter()
                                .map(|expr| PyExpr::from(expr.clone()))
                                .collect();
                            py.import(pyo3::intern!(
                                py,
                                "daft.execution.shuffles.flight_shuffle.flight_shuffle"
                            ))?
                            .getattr(pyo3::intern!(py, "flight_shuffle"))?
                            .call1((
                                upstream_iter,
                                hash_clustering_config.num_partitions,
                                shuffle_dirs,
                                Some(partition_by_pyexprs),
                            ))?
                        }
                        daft_logical_plan::ClusteringSpec::Random(random_clustering_config) => py
                            .import(pyo3::intern!(
                                py,
                                "daft.execution.shuffles.flight_shuffle.flight_shuffle"
                            ))?
                            .getattr(pyo3::intern!(py, "flight_shuffle"))?
                            .call1((
                                upstream_iter,
                                random_clustering_config.num_partitions(),
                                shuffle_dirs,
                            ))?,
                        daft_logical_plan::ClusteringSpec::Range(_) => {
                            unimplemented!("FanoutByRange not implemented, since only use case (sorting) doesn't need it yet.");
                        }
                        daft_logical_plan::ClusteringSpec::Unknown(_) => {
                            unreachable!("Cannot use NaiveFullyMaterializingMapReduce ShuffleExchange to map to an Unknown ClusteringSpec");
                        }
                    };
                    Ok(shuffled.into())
                }
                ShuffleExchangeStrategy::SplitOrCoalesceToTargetNum {
                    target_num_partitions,
                } => {
                    match target_num_partitions.cmp(&input_num_partitions) {
                        std::cmp::Ordering::Equal => Ok(upstream_iter),
                        std::cmp::Ordering::Greater => {
                            // Split if more outputs than inputs
                            let split = py
                                .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                                .getattr(pyo3::intern!(py, "split"))?
                                .call1((
                                    upstream_iter,
                                    input_num_partitions,
                                    *target_num_partitions,
                                ))?;
                            let flattened = py
                                .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                                .getattr(pyo3::intern!(py, "flatten_plan"))?
                                .call1((split,))?;
                            Ok(flattened.into())
                        }
                        std::cmp::Ordering::Less => {
                            // Coalesce if fewer outputs than inputs
                            let coalesced = py
                                .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                                .getattr(pyo3::intern!(py, "coalesce"))?
                                .call1((
                                    upstream_iter,
                                    input_num_partitions,
                                    *target_num_partitions,
                                ))?;
                            Ok(coalesced.into())
                        }
                    }
                }
            }
        }
        PhysicalPlan::Aggregate(Aggregate {
            aggregations,
            groupby,
            input,
            ..
        }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
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
        PhysicalPlan::Dedup(Dedup { input, columns, .. }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let columns_as_pyexprs: Vec<PyExpr> = columns
                .iter()
                .map(|expr| PyExpr::from(expr.clone()))
                .collect();
            let py_iter = py
                .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                .getattr(pyo3::intern!(py, "local_dedup"))?
                .call1((upstream_iter, columns_as_pyexprs))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::Pivot(Pivot {
            input,
            group_by,
            pivot_column,
            value_column,
            names,
        }) => {
            let upstream_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let groupbys_as_pyexprs: Vec<PyExpr> = group_by
                .iter()
                .map(|expr| PyExpr::from(expr.clone()))
                .collect();
            let pivot_column_pyexpr = PyExpr::from(pivot_column.clone());
            let value_column_pyexpr = PyExpr::from(value_column.clone());
            let py_iter = py
                .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                .getattr(pyo3::intern!(py, "pivot"))?
                .call1((
                    upstream_iter,
                    groupbys_as_pyexprs,
                    pivot_column_pyexpr,
                    value_column_pyexpr,
                    names.clone(),
                ))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::Concat(Concat { other, input }) => {
            let upstream_input_iter =
                physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?;
            let upstream_other_iter =
                physical_plan_to_partition_tasks(other, py, psets, actor_pool_manager)?;
            let py_iter = py
                .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                .getattr(pyo3::intern!(py, "concat"))?
                .call1((upstream_input_iter, upstream_other_iter))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
            null_equals_nulls,
            join_type,
            ..
        }) => {
            let upstream_left_iter =
                physical_plan_to_partition_tasks(left, py, psets, actor_pool_manager)?;
            let upstream_right_iter =
                physical_plan_to_partition_tasks(right, py, psets, actor_pool_manager)?;
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
                .getattr(pyo3::intern!(py, "hash_join"))?
                .call1((
                    upstream_left_iter,
                    upstream_right_iter,
                    left_on_pyexprs,
                    right_on_pyexprs,
                    null_equals_nulls.clone(),
                    *join_type,
                ))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::SortMergeJoin(SortMergeJoin {
            left,
            right,
            left_on,
            right_on,
            join_type,
            num_partitions,
            left_is_larger,
            needs_presort,
        }) => {
            let left_iter = physical_plan_to_partition_tasks(left, py, psets, actor_pool_manager)?;
            let right_iter =
                physical_plan_to_partition_tasks(right, py, psets, actor_pool_manager)?;
            let left_on_pyexprs: Vec<PyExpr> = left_on
                .iter()
                .map(|expr| PyExpr::from(expr.clone()))
                .collect();
            let right_on_pyexprs: Vec<PyExpr> = right_on
                .iter()
                .map(|expr| PyExpr::from(expr.clone()))
                .collect();
            // TODO(Clark): Elide sorting one side of the join if already range-partitioned, where we'd use that side's boundaries to sort the other side.
            let py_iter = if *needs_presort {
                py.import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "sort_merge_join_aligned_boundaries"))?
                    .call1((
                        left_iter,
                        right_iter,
                        left_on_pyexprs,
                        right_on_pyexprs,
                        *join_type,
                        *num_partitions,
                        *left_is_larger,
                    ))?
            } else {
                py.import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "merge_join_sorted"))?
                    .call1((
                        left_iter,
                        right_iter,
                        left_on_pyexprs,
                        right_on_pyexprs,
                        *join_type,
                        *left_is_larger,
                    ))?
            };
            Ok(py_iter.into())
        }
        PhysicalPlan::BroadcastJoin(BroadcastJoin {
            broadcaster: left,
            receiver: right,
            left_on,
            right_on,
            null_equals_nulls,
            join_type,
            is_swapped,
        }) => {
            let upstream_left_iter =
                physical_plan_to_partition_tasks(left, py, psets, actor_pool_manager)?;
            let upstream_right_iter =
                physical_plan_to_partition_tasks(right, py, psets, actor_pool_manager)?;
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
                .getattr(pyo3::intern!(py, "broadcast_join"))?
                .call1((
                    upstream_left_iter,
                    upstream_right_iter,
                    left_on_pyexprs,
                    right_on_pyexprs,
                    null_equals_nulls.clone(),
                    *join_type,
                    *is_swapped,
                ))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::CrossJoin(CrossJoin {
            left,
            right,
            outer_loop_side,
            ..
        }) => {
            let upstream_left_iter =
                physical_plan_to_partition_tasks(left, py, psets, actor_pool_manager)?;
            let upstream_right_iter =
                physical_plan_to_partition_tasks(right, py, psets, actor_pool_manager)?;
            let py_iter = py
                .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                .getattr(pyo3::intern!(py, "cross_join"))?
                .call1((upstream_left_iter, upstream_right_iter, *outer_loop_side))?;
            Ok(py_iter.into())
        }
        PhysicalPlan::TabularWriteParquet(TabularWriteParquet {
            schema,
            file_info:
                OutputFileInfo {
                    root_dir,
                    write_mode,
                    file_format,
                    partition_cols,
                    compression,
                    io_config,
                },
            input,
        }) => tabular_write(
            py,
            physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?,
            write_mode,
            file_format,
            schema,
            root_dir,
            compression.as_ref(),
            partition_cols.as_ref(),
            io_config.as_ref(),
        ),
        PhysicalPlan::TabularWriteCsv(TabularWriteCsv {
            schema,
            file_info:
                OutputFileInfo {
                    root_dir,
                    write_mode,
                    file_format,
                    partition_cols,
                    compression,
                    io_config,
                },
            input,
        }) => tabular_write(
            py,
            physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?,
            write_mode,
            file_format,
            schema,
            root_dir,
            compression.as_ref(),
            partition_cols.as_ref(),
            io_config.as_ref(),
        ),
        PhysicalPlan::TabularWriteJson(TabularWriteJson {
            schema,
            file_info:
                OutputFileInfo {
                    root_dir,
                    write_mode,
                    file_format,
                    partition_cols,
                    compression,
                    io_config,
                },
            input,
        }) => tabular_write(
            py,
            physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?,
            write_mode,
            file_format,
            schema,
            root_dir,
            compression.as_ref(),
            partition_cols.as_ref(),
            io_config.as_ref(),
        ),
        #[cfg(feature = "python")]
        PhysicalPlan::IcebergWrite(IcebergWrite {
            schema: _,
            iceberg_info,
            input,
        }) => iceberg_write(
            py,
            physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?,
            iceberg_info,
        ),
        #[cfg(feature = "python")]
        PhysicalPlan::DeltaLakeWrite(DeltaLakeWrite {
            schema: _,
            delta_lake_info,
            input,
        }) => deltalake_write(
            py,
            physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?,
            delta_lake_info,
        ),
        #[cfg(feature = "python")]
        PhysicalPlan::LanceWrite(LanceWrite {
            schema: _,
            lance_info,
            input,
        }) => lance_write(
            py,
            physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?,
            lance_info,
        ),
        #[cfg(feature = "python")]
        PhysicalPlan::DataSink(DataSink {
            schema: _,
            data_sink_info,
            input,
        }) => data_sink_write(
            py,
            physical_plan_to_partition_tasks(input, py, psets, actor_pool_manager)?,
            data_sink_info,
        ),
    }
}
