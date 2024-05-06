#[cfg(feature = "python")]
use {
    crate::{sink_info::OutputFileInfo, source_info::InMemoryInfo},
    common_io_config::IOConfig,
    daft_core::python::schema::PySchema,
    daft_core::schema::SchemaRef,
    daft_dsl::python::PyExpr,
    daft_dsl::Expr,
    daft_scan::{file_format::FileFormat, python::pylib::PyScanTask},
    pyo3::{
        pyclass, pymethods, types::PyBytes, PyObject, PyRef, PyRefMut, PyResult, PyTypeInfo,
        Python, ToPyObject,
    },
    std::collections::HashMap,
};

use daft_core::impl_bincode_py_state_serialization;
use daft_dsl::ExprRef;
use serde::{Deserialize, Serialize};
use std::{cmp::max, sync::Arc};

use crate::{
    display::TreeDisplay,
    partitioning::{
        ClusteringSpec, HashClusteringConfig, RandomClusteringConfig, RangeClusteringConfig,
        UnknownClusteringConfig,
    },
    physical_ops::*,
};

#[cfg(feature = "python")]
use crate::sink_info::IcebergCatalogInfo;

pub(crate) type PhysicalPlanRef = Arc<PhysicalPlan>;

/// Physical plan for a Daft query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PhysicalPlan {
    #[cfg(feature = "python")]
    InMemoryScan(InMemoryScan),
    TabularScan(TabularScan),
    EmptyScan(EmptyScan),
    Project(Project),
    Filter(Filter),
    Limit(Limit),
    Explode(Explode),
    Unpivot(Unpivot),
    Sort(Sort),
    Split(Split),
    Sample(Sample),
    MonotonicallyIncreasingId(MonotonicallyIncreasingId),
    Coalesce(Coalesce),
    Flatten(Flatten),
    FanoutRandom(FanoutRandom),
    FanoutByHash(FanoutByHash),
    #[allow(dead_code)]
    FanoutByRange(FanoutByRange),
    ReduceMerge(ReduceMerge),
    Aggregate(Aggregate),
    Pivot(Pivot),
    Concat(Concat),
    HashJoin(HashJoin),
    SortMergeJoin(SortMergeJoin),
    BroadcastJoin(BroadcastJoin),
    TabularWriteParquet(TabularWriteParquet),
    TabularWriteJson(TabularWriteJson),
    TabularWriteCsv(TabularWriteCsv),
    #[cfg(feature = "python")]
    IcebergWrite(IcebergWrite),
}

impl PhysicalPlan {
    pub fn arced(self) -> PhysicalPlanRef {
        Arc::new(self)
    }

    pub fn clustering_spec(&self) -> Arc<ClusteringSpec> {
        match self {
            #[cfg(feature = "python")]
            Self::InMemoryScan(InMemoryScan {
                clustering_spec, ..
            }) => clustering_spec.clone(),
            Self::TabularScan(TabularScan {
                clustering_spec, ..
            }) => clustering_spec.clone(),
            Self::EmptyScan(EmptyScan {
                clustering_spec, ..
            }) => clustering_spec.clone(),
            Self::Project(Project {
                clustering_spec, ..
            }) => clustering_spec.clone(),
            Self::Filter(Filter { input, .. }) => input.clustering_spec(),
            Self::Limit(Limit { input, .. }) => input.clustering_spec(),
            Self::Explode(Explode {
                clustering_spec, ..
            }) => clustering_spec.clone(),
            Self::Unpivot(Unpivot {
                clustering_spec, ..
            }) => clustering_spec.clone(),
            Self::Sample(Sample { input, .. }) => input.clustering_spec(),
            Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. }) => {
                input.clustering_spec().clone()
            }

            Self::Sort(Sort {
                input,
                sort_by,
                descending,
                ..
            }) => ClusteringSpec::Range(RangeClusteringConfig::new(
                input.clustering_spec().num_partitions(),
                sort_by.clone(),
                descending.clone(),
            ))
            .into(),
            Self::Split(Split {
                output_num_partitions,
                ..
            }) => {
                ClusteringSpec::Unknown(UnknownClusteringConfig::new(*output_num_partitions)).into()
            }
            Self::Coalesce(Coalesce { num_to, .. }) => {
                ClusteringSpec::Unknown(UnknownClusteringConfig::new(*num_to)).into()
            }
            Self::Flatten(Flatten { input }) => input.clustering_spec(),
            Self::FanoutRandom(FanoutRandom { num_partitions, .. }) => {
                ClusteringSpec::Random(RandomClusteringConfig::new(*num_partitions)).into()
            }
            Self::FanoutByHash(FanoutByHash {
                num_partitions,
                partition_by,
                ..
            }) => ClusteringSpec::Hash(HashClusteringConfig::new(
                *num_partitions,
                partition_by.clone(),
            ))
            .into(),
            Self::FanoutByRange(FanoutByRange {
                num_partitions,
                sort_by,
                descending,
                ..
            }) => ClusteringSpec::Range(RangeClusteringConfig::new(
                *num_partitions,
                sort_by.clone(),
                descending.clone(),
            ))
            .into(),
            Self::ReduceMerge(ReduceMerge { input }) => input.clustering_spec(),
            Self::Aggregate(Aggregate { input, groupby, .. }) => {
                let input_clustering_spec = input.clustering_spec();
                if input_clustering_spec.num_partitions() == 1 {
                    input_clustering_spec
                } else if groupby.is_empty() {
                    ClusteringSpec::Unknown(Default::default()).into()
                } else {
                    ClusteringSpec::Hash(HashClusteringConfig::new(
                        input.clustering_spec().num_partitions(),
                        groupby.clone(),
                    ))
                    .into()
                }
            }
            Self::Pivot(Pivot { input, .. }) => input.clustering_spec(),
            Self::Concat(Concat { input, other }) => {
                ClusteringSpec::Unknown(UnknownClusteringConfig::new(
                    input.clustering_spec().num_partitions()
                        + other.clustering_spec().num_partitions(),
                ))
                .into()
            }
            Self::HashJoin(HashJoin {
                left,
                right,
                left_on,
                ..
            }) => {
                let input_clustering_spec = left.clustering_spec();
                match max(
                    input_clustering_spec.num_partitions(),
                    right.clustering_spec().num_partitions(),
                ) {
                    // NOTE: This duplicates the repartitioning logic in the planner, where we
                    // conditionally repartition the left and right tables.
                    // TODO(Clark): Consolidate this logic with the planner logic when we push the partition spec
                    // to be an entirely planner-side concept.
                    1 => input_clustering_spec,
                    num_partitions => ClusteringSpec::Hash(HashClusteringConfig::new(
                        num_partitions,
                        left_on.clone(),
                    ))
                    .into(),
                }
            }
            Self::BroadcastJoin(BroadcastJoin {
                receiver: right, ..
            }) => right.clustering_spec(),
            Self::SortMergeJoin(SortMergeJoin {
                left,
                right,
                left_on,
                ..
            }) => ClusteringSpec::Range(RangeClusteringConfig::new(
                max(
                    left.clustering_spec().num_partitions(),
                    right.clustering_spec().num_partitions(),
                ),
                left_on.clone(),
                // TODO(Clark): Propagate descending vec once sort-merge join supports descending sort orders.
                std::iter::repeat(false).take(left_on.len()).collect(),
            ))
            .into(),
            Self::TabularWriteParquet(TabularWriteParquet { input, .. }) => input.clustering_spec(),
            Self::TabularWriteCsv(TabularWriteCsv { input, .. }) => input.clustering_spec(),
            Self::TabularWriteJson(TabularWriteJson { input, .. }) => input.clustering_spec(),
            #[cfg(feature = "python")]
            Self::IcebergWrite(..) => {
                ClusteringSpec::Unknown(UnknownClusteringConfig::new(1)).into()
            }
        }
    }

    pub fn approximate_size_bytes(&self) -> Option<usize> {
        match self {
            #[cfg(feature = "python")]
            Self::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
                Some(in_memory_info.size_bytes)
            }
            Self::TabularScan(TabularScan { scan_tasks, .. }) => scan_tasks
                .iter()
                .map(|scan_task| scan_task.size_bytes())
                .sum::<Option<usize>>(),
            Self::EmptyScan(..) => Some(0),
            // Assume no row/column pruning in cardinality-affecting operations.
            // TODO(Clark): Estimate row/column pruning to get a better size approximation.
            Self::Filter(Filter { input, .. })
            | Self::Limit(Limit { input, .. })
            | Self::Project(Project { input, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. })
            | Self::Unpivot(Unpivot { input, .. }) => input.approximate_size_bytes(),
            Self::Sample(Sample {
                input, fraction, ..
            }) => input
                .approximate_size_bytes()
                .map(|size| (size as f64 * fraction) as usize),
            // Assume ~the same size in bytes for explodes.
            // TODO(Clark): Improve this estimate.
            Self::Explode(Explode { input, .. }) => input.approximate_size_bytes(),
            // Propagate child approximation for operations that don't affect cardinality.
            Self::Coalesce(Coalesce { input, .. })
            | Self::FanoutByHash(FanoutByHash { input, .. })
            | Self::FanoutByRange(FanoutByRange { input, .. })
            | Self::FanoutRandom(FanoutRandom { input, .. })
            | Self::Flatten(Flatten { input, .. })
            | Self::ReduceMerge(ReduceMerge { input, .. })
            | Self::Sort(Sort { input, .. })
            | Self::Split(Split { input, .. })
            | Self::Pivot(Pivot { input, .. }) => input.approximate_size_bytes(),
            Self::Concat(Concat { input, other }) => {
                input.approximate_size_bytes().and_then(|input_size| {
                    other
                        .approximate_size_bytes()
                        .map(|other_size| input_size + other_size)
                })
            }
            // Assume a simple sum of the sizes of both sides of the join for the post-join size.
            // TODO(Clark): This will double-count join key columns, we should ensure that these are only counted once.
            Self::BroadcastJoin(BroadcastJoin {
                broadcaster: left,
                receiver: right,
                ..
            })
            | Self::HashJoin(HashJoin { left, right, .. })
            | Self::SortMergeJoin(SortMergeJoin { left, right, .. }) => {
                left.approximate_size_bytes().and_then(|left_size| {
                    right
                        .approximate_size_bytes()
                        .map(|right_size| left_size + right_size)
                })
            }
            // TODO(Clark): Approximate post-aggregation sizes via grouping estimates + aggregation type.
            Self::Aggregate(_) => None,
            // Post-write DataFrame will contain paths to files that were written.
            // TODO(Clark): Estimate output size via root directory and estimates for # of partitions given partitioning column.
            Self::TabularWriteParquet(_) | Self::TabularWriteCsv(_) | Self::TabularWriteJson(_) => {
                None
            }
            #[cfg(feature = "python")]
            Self::IcebergWrite(_) => None,
        }
    }

    pub fn children(&self) -> Vec<PhysicalPlanRef> {
        match self {
            #[cfg(feature = "python")]
            Self::InMemoryScan(..) => vec![],
            Self::TabularScan(..) | Self::EmptyScan(..) => vec![],
            Self::Project(Project { input, .. }) => vec![input.clone()],
            Self::Filter(Filter { input, .. }) => vec![input.clone()],
            Self::Limit(Limit { input, .. }) => vec![input.clone()],
            Self::Explode(Explode { input, .. }) => vec![input.clone()],
            Self::Unpivot(Unpivot { input, .. }) => vec![input.clone()],
            Self::Sample(Sample { input, .. }) => vec![input.clone()],
            Self::Sort(Sort { input, .. }) => vec![input.clone()],
            Self::Split(Split { input, .. }) => vec![input.clone()],
            Self::Coalesce(Coalesce { input, .. }) => vec![input.clone()],
            Self::Flatten(Flatten { input }) => vec![input.clone()],
            Self::FanoutRandom(FanoutRandom { input, .. }) => vec![input.clone()],
            Self::FanoutByHash(FanoutByHash { input, .. }) => vec![input.clone()],
            Self::FanoutByRange(FanoutByRange { input, .. }) => vec![input.clone()],
            Self::ReduceMerge(ReduceMerge { input }) => vec![input.clone()],
            Self::Aggregate(Aggregate { input, .. }) => vec![input.clone()],
            Self::Pivot(Pivot { input, .. }) => vec![input.clone()],
            Self::TabularWriteParquet(TabularWriteParquet { input, .. }) => vec![input.clone()],
            Self::TabularWriteCsv(TabularWriteCsv { input, .. }) => vec![input.clone()],
            Self::TabularWriteJson(TabularWriteJson { input, .. }) => vec![input.clone()],
            #[cfg(feature = "python")]
            Self::IcebergWrite(IcebergWrite { input, .. }) => vec![input.clone()],
            Self::HashJoin(HashJoin { left, right, .. }) => vec![left.clone(), right.clone()],
            Self::BroadcastJoin(BroadcastJoin {
                broadcaster,
                receiver,
                ..
            }) => vec![broadcaster.clone(), receiver.clone()],
            Self::SortMergeJoin(SortMergeJoin { left, right, .. }) => {
                vec![left.clone(), right.clone()]
            }
            Self::Concat(Concat { input, other }) => vec![input.clone(), other.clone()],
            Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. }) => {
                vec![input.clone()]
            }
        }
    }

    pub fn with_new_children(&self, children: &[PhysicalPlanRef]) -> PhysicalPlan {
        match children {
            [input] => match self {
                #[cfg(feature = "python")]
                Self::InMemoryScan(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::TabularScan(..)
                | Self::EmptyScan(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::Project(Project { projection, resource_request, clustering_spec, .. }) => Self::Project(Project::try_new(
                    input.clone(), projection.clone(), resource_request.clone(), clustering_spec.clone(),
                ).unwrap()),
                Self::Filter(Filter { predicate, .. }) => Self::Filter(Filter::new(input.clone(), predicate.clone())),
                Self::Limit(Limit { limit, eager, num_partitions, .. }) => Self::Limit(Limit::new(input.clone(), *limit, *eager, *num_partitions)),
                Self::Explode(Explode { to_explode, .. }) => Self::Explode(Explode::try_new(input.clone(), to_explode.clone()).unwrap()),
                Self::Unpivot(Unpivot { ids, values, variable_name, value_name, .. }) => Self::Unpivot(Unpivot::new(input.clone(), ids.clone(), values.clone(), variable_name, value_name)),
                Self::Sample(Sample { fraction, with_replacement, seed, .. }) => Self::Sample(Sample::new(input.clone(), *fraction, *with_replacement, *seed)),
                Self::Sort(Sort { sort_by, descending, num_partitions, .. }) => Self::Sort(Sort::new(input.clone(), sort_by.clone(), descending.clone(), *num_partitions)),
                Self::Split(Split { input_num_partitions, output_num_partitions, .. }) => Self::Split(Split::new(input.clone(), *input_num_partitions, *output_num_partitions)),
                Self::Coalesce(Coalesce { num_from, num_to, .. }) => Self::Coalesce(Coalesce::new(input.clone(), *num_from, *num_to)),
                Self::Flatten(..) => Self::Flatten(Flatten::new(input.clone())),
                Self::FanoutRandom(FanoutRandom { num_partitions, .. }) => Self::FanoutRandom(FanoutRandom::new(input.clone(), *num_partitions)),
                Self::FanoutByHash(FanoutByHash { num_partitions, partition_by, .. }) => Self::FanoutByHash(FanoutByHash::new(input.clone(), *num_partitions, partition_by.clone())),
                Self::FanoutByRange(FanoutByRange { num_partitions, sort_by, descending, .. }) => Self::FanoutByRange(FanoutByRange::new(input.clone(), *num_partitions, sort_by.clone(), descending.clone())),
                Self::ReduceMerge(..) => Self::ReduceMerge(ReduceMerge::new(input.clone())),
                Self::Aggregate(Aggregate { aggregations, groupby, ..}) => Self::Aggregate(Aggregate::new(input.clone(), aggregations.clone(), groupby.clone())),
                Self::TabularWriteParquet(TabularWriteParquet { schema, file_info, .. }) => Self::TabularWriteParquet(TabularWriteParquet::new(schema.clone(), file_info.clone(), input.clone())),
                Self::TabularWriteCsv(TabularWriteCsv { schema, file_info, .. }) => Self::TabularWriteCsv(TabularWriteCsv::new(schema.clone(), file_info.clone(), input.clone())),
                Self::TabularWriteJson(TabularWriteJson { schema, file_info, .. }) => Self::TabularWriteJson(TabularWriteJson::new(schema.clone(), file_info.clone(), input.clone())),
                #[cfg(feature = "python")]
                Self::IcebergWrite(IcebergWrite { schema, iceberg_info, .. }) => Self::IcebergWrite(IcebergWrite::new(schema.clone(), iceberg_info.clone(), input.clone())),
                _ => panic!("Physical op {:?} has two inputs, but got one", self),
            },
            [input1, input2] => match self {
                #[cfg(feature = "python")]
                Self::InMemoryScan(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::TabularScan(..)
                | Self::EmptyScan(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::HashJoin(HashJoin { left_on, right_on, join_type, .. }) => Self::HashJoin(HashJoin::new(input1.clone(), input2.clone(), left_on.clone(), right_on.clone(), *join_type)),
                Self::BroadcastJoin(BroadcastJoin {
                    left_on,
                    right_on,
                    join_type,
                    is_swapped,
                    ..
                }) => Self::BroadcastJoin(BroadcastJoin::new(input1.clone(), input2.clone(), left_on.clone(), right_on.clone(), *join_type, *is_swapped)),
                Self::SortMergeJoin(SortMergeJoin { left_on, right_on, join_type, num_partitions, left_is_larger, needs_presort, .. }) => Self::SortMergeJoin(SortMergeJoin::new(input1.clone(), input2.clone(), left_on.clone(), right_on.clone(), *join_type, *num_partitions, *left_is_larger, *needs_presort)),
                Self::Concat(_) => Self::Concat(Concat::new(input1.clone(), input2.clone())),
                _ => panic!("Physical op {:?} has one input, but got two", self),
            },
            _ => panic!("Physical ops should never have more than 2 inputs, but got: {}", children.len())
        }
    }

    pub fn name(&self) -> String {
        let name = match self {
            #[cfg(feature = "python")]
            Self::InMemoryScan(..) => "InMemoryScan",
            Self::TabularScan(..) => "TabularScan",
            Self::EmptyScan(..) => "EmptyScan",
            Self::Project(..) => "Project",
            Self::Filter(..) => "Filter",
            Self::Limit(..) => "Limit",
            Self::Explode(..) => "Explode",
            Self::Unpivot(..) => "Unpivot",
            Self::Sample(..) => "Sample",
            Self::Sort(..) => "Sort",
            Self::Split(..) => "Split",
            Self::Coalesce(..) => "Coalesce",
            Self::Flatten(..) => "Flatten",
            Self::FanoutRandom(..) => "FanoutRandom",
            Self::FanoutByHash(..) => "FanoutByHash",
            Self::FanoutByRange(..) => "FanoutByRange",
            Self::ReduceMerge(..) => "ReduceMerge",
            Self::Aggregate(..) => "Aggregate",
            Self::Pivot(..) => "Pivot",
            Self::HashJoin(..) => "HashJoin",
            Self::BroadcastJoin(..) => "BroadcastJoin",
            Self::SortMergeJoin(..) => "SortMergeJoin",
            Self::Concat(..) => "Concat",
            Self::TabularWriteParquet(..) => "TabularWriteParquet",
            Self::TabularWriteCsv(..) => "TabularWriteCsv",
            Self::TabularWriteJson(..) => "TabularWriteJson",
            Self::MonotonicallyIncreasingId(..) => "MonotonicallyIncreasingId",
            #[cfg(feature = "python")]
            Self::IcebergWrite(..) => "IcebergWrite",
        };
        name.to_string()
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            #[cfg(feature = "python")]
            Self::InMemoryScan(in_memory_scan) => in_memory_scan.multiline_display(),
            Self::TabularScan(tabular_scan) => tabular_scan.multiline_display(),
            Self::EmptyScan(empty_scan) => empty_scan.multiline_display(),
            Self::Project(project) => project.multiline_display(),
            Self::Filter(filter) => filter.multiline_display(),
            Self::Limit(limit) => limit.multiline_display(),
            Self::Explode(explode) => explode.multiline_display(),
            Self::Unpivot(unpivot) => unpivot.multiline_display(),
            Self::Sample(sample) => sample.multiline_display(),
            Self::Sort(sort) => sort.multiline_display(),
            Self::Split(split) => split.multiline_display(),
            Self::Coalesce(coalesce) => coalesce.multiline_display(),
            Self::Flatten(flatten) => flatten.multiline_display(),
            Self::FanoutRandom(fanout_random) => fanout_random.multiline_display(),
            Self::FanoutByHash(fanout_by_hash) => fanout_by_hash.multiline_display(),
            Self::FanoutByRange(fanout_by_range) => fanout_by_range.multiline_display(),
            Self::ReduceMerge(reduce_merge) => reduce_merge.multiline_display(),
            Self::Aggregate(aggregate) => aggregate.multiline_display(),
            Self::Pivot(pivot) => pivot.multiline_display(),
            Self::HashJoin(hash_join) => hash_join.multiline_display(),
            Self::BroadcastJoin(broadcast_join) => broadcast_join.multiline_display(),
            Self::SortMergeJoin(sort_merge_join) => sort_merge_join.multiline_display(),
            Self::Concat(concat) => concat.multiline_display(),
            Self::TabularWriteParquet(tabular_write_parquet) => {
                tabular_write_parquet.multiline_display()
            }
            Self::TabularWriteCsv(tabular_write_csv) => tabular_write_csv.multiline_display(),
            Self::TabularWriteJson(tabular_write_json) => tabular_write_json.multiline_display(),
            Self::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                monotonically_increasing_id.multiline_display()
            }
            #[cfg(feature = "python")]
            Self::IcebergWrite(iceberg_info) => iceberg_info.multiline_display(),
        }
    }

    pub fn repr_ascii(&self, simple: bool) -> String {
        let mut s = String::new();
        self.fmt_tree(&mut s, simple).unwrap();
        s
    }

    pub fn repr_indent(&self) -> String {
        let mut s = String::new();
        self.fmt_tree_indent_style(0, &mut s).unwrap();
        s
    }
}

/// A work scheduler for physical plans.
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct PhysicalPlanScheduler {
    plan: PhysicalPlanRef,
}

#[cfg(feature = "python")]
#[pymethods]
impl PhysicalPlanScheduler {
    pub fn num_partitions(&self) -> PyResult<i64> {
        Ok(self.plan.clustering_spec().num_partitions() as i64)
    }

    pub fn repr_ascii(&self, simple: bool) -> PyResult<String> {
        Ok(self.plan.repr_ascii(simple))
    }
    /// Converts the contained physical plan into an iterator of executable partition tasks.
    pub fn to_partition_tasks(&self, psets: HashMap<String, Vec<PyObject>>) -> PyResult<PyObject> {
        Python::with_gil(|py| self.plan.to_partition_tasks(py, &psets))
    }
}

impl_bincode_py_state_serialization!(PhysicalPlanScheduler);

impl From<PhysicalPlanRef> for PhysicalPlanScheduler {
    fn from(plan: PhysicalPlanRef) -> Self {
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

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "python")]
fn tabular_write(
    py: Python<'_>,
    upstream_iter: PyObject,
    file_format: &FileFormat,
    schema: &SchemaRef,
    root_dir: &String,
    compression: &Option<String>,
    partition_cols: &Option<Vec<ExprRef>>,
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

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "python")]
fn iceberg_write(
    py: Python<'_>,
    upstream_iter: PyObject,
    iceberg_info: &IcebergCatalogInfo,
) -> PyResult<PyObject> {
    let py_iter = py
        .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
        .getattr(pyo3::intern!(py, "write_iceberg"))?
        .call1((
            upstream_iter,
            &iceberg_info.table_location,
            &iceberg_info.iceberg_schema,
            &iceberg_info.iceberg_properties,
            iceberg_info.spec_id,
            iceberg_info
                .io_config
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

            PhysicalPlan::Project(Project {
                input,
                projection,
                resource_request,
                ..
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                eager,
                num_partitions,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets)?;
                let py_physical_plan =
                    py.import(pyo3::intern!(py, "daft.execution.physical_plan"))?;
                let global_limit_iter = py_physical_plan
                    .getattr(pyo3::intern!(py, "global_limit"))?
                    .call1((upstream_iter, *limit, *eager, *num_partitions))?;
                Ok(global_limit_iter.into())
            }
            PhysicalPlan::Explode(Explode {
                input, to_explode, ..
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                num_partitions,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets)?;
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                    .getattr(pyo3::intern!(py, "split"))?
                    .call1((upstream_iter, *input_num_partitions, *output_num_partitions))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Flatten(Flatten { input }) => {
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets)?;
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
            PhysicalPlan::Pivot(Pivot {
                input,
                group_by,
                pivot_column,
                value_column,
                names,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets)?;
                let group_by_pyexpr = PyExpr::from(group_by.clone());
                let pivot_column_pyexpr = PyExpr::from(pivot_column.clone());
                let value_column_pyexpr = PyExpr::from(value_column.clone());
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.rust_physical_plan_shim"))?
                    .getattr(pyo3::intern!(py, "pivot"))?
                    .call1((
                        upstream_iter,
                        group_by_pyexpr,
                        pivot_column_pyexpr,
                        value_column_pyexpr,
                        names.clone(),
                    ))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Coalesce(Coalesce {
                input,
                num_from,
                num_to,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets)?;
                let py_iter = py
                    .import(pyo3::intern!(py, "daft.execution.physical_plan"))?
                    .getattr(pyo3::intern!(py, "coalesce"))?
                    .call1((upstream_iter, *num_from, *num_to))?;
                Ok(py_iter.into())
            }
            PhysicalPlan::Concat(Concat { other, input }) => {
                let upstream_input_iter = input.to_partition_tasks(py, psets)?;
                let upstream_other_iter = other.to_partition_tasks(py, psets)?;
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
                join_type,
                ..
            }) => {
                let upstream_left_iter = left.to_partition_tasks(py, psets)?;
                let upstream_right_iter = right.to_partition_tasks(py, psets)?;
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
                let left_iter = left.to_partition_tasks(py, psets)?;
                let right_iter = right.to_partition_tasks(py, psets)?;
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
                join_type,
                is_swapped,
            }) => {
                let upstream_left_iter = left.to_partition_tasks(py, psets)?;
                let upstream_right_iter = right.to_partition_tasks(py, psets)?;
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
                        *join_type,
                        *is_swapped,
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
                input.to_partition_tasks(py, psets)?,
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
                input.to_partition_tasks(py, psets)?,
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
                input.to_partition_tasks(py, psets)?,
                file_format,
                schema,
                root_dir,
                compression,
                partition_cols,
                io_config,
            ),
            #[cfg(feature = "python")]
            PhysicalPlan::IcebergWrite(IcebergWrite {
                schema: _,
                iceberg_info,
                input,
            }) => iceberg_write(py, input.to_partition_tasks(py, psets)?, iceberg_info),
        }
    }
}
