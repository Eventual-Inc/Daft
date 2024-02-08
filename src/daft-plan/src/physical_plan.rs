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

use crate::{display::TreeDisplay, physical_ops::*, PartitionScheme, PartitionSpec};

pub(crate) type PhysicalPlanRef = Arc<PhysicalPlan>;

/// Physical plan for a Daft query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PhysicalPlan {
    #[cfg(feature = "python")]
    InMemoryScan(InMemoryScan),
    TabularScanParquet(TabularScanParquet),
    TabularScanCsv(TabularScanCsv),
    TabularScanJson(TabularScanJson),
    TabularScan(TabularScan),
    EmptyScan(EmptyScan),
    Project(Project),
    Filter(Filter),
    Limit(Limit),
    Explode(Explode),
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
    Concat(Concat),
    HashJoin(HashJoin),
    SortMergeJoin(SortMergeJoin),
    BroadcastJoin(BroadcastJoin),
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
            Self::EmptyScan(EmptyScan { partition_spec, .. }) => partition_spec.clone(),
            Self::TabularScanParquet(TabularScanParquet { partition_spec, .. }) => {
                partition_spec.clone()
            }
            Self::TabularScanCsv(TabularScanCsv { partition_spec, .. }) => partition_spec.clone(),
            Self::TabularScanJson(TabularScanJson { partition_spec, .. }) => partition_spec.clone(),
            Self::Project(Project { partition_spec, .. }) => partition_spec.clone(),
            Self::Filter(Filter { input, .. }) => input.partition_spec(),
            Self::Limit(Limit { input, .. }) => input.partition_spec(),
            Self::Explode(Explode { partition_spec, .. }) => partition_spec.clone(),
            Self::Sample(Sample { input, .. }) => input.partition_spec(),
            Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. }) => {
                input.partition_spec().clone()
            }

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
            Self::HashJoin(HashJoin {
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
            Self::BroadcastJoin(BroadcastJoin {
                receiver: right, ..
            }) => right.partition_spec(),
            Self::SortMergeJoin(SortMergeJoin {
                left,
                right,
                left_on,
                ..
            }) => PartitionSpec::new_internal(
                PartitionScheme::Range,
                max(
                    left.partition_spec().num_partitions,
                    right.partition_spec().num_partitions,
                ),
                Some(left_on.clone()),
            )
            .into(),
            Self::TabularWriteParquet(TabularWriteParquet { input, .. }) => input.partition_spec(),
            Self::TabularWriteCsv(TabularWriteCsv { input, .. }) => input.partition_spec(),
            Self::TabularWriteJson(TabularWriteJson { input, .. }) => input.partition_spec(),
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
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. }) => {
                input.approximate_size_bytes()
            }
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
            | Self::Split(Split { input, .. }) => input.approximate_size_bytes(),
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
            // No size approximation support for legacy I/O.
            Self::TabularScanParquet(_) | Self::TabularScanCsv(_) | Self::TabularScanJson(_) => {
                None
            }
            // Post-write DataFrame will contain paths to files that were written.
            // TODO(Clark): Estimate output size via root directory and estimates for # of partitions given partitioning column.
            Self::TabularWriteParquet(_) | Self::TabularWriteCsv(_) | Self::TabularWriteJson(_) => {
                None
            }
        }
    }

    pub fn children(&self) -> Vec<&PhysicalPlanRef> {
        match self {
            #[cfg(feature = "python")]
            Self::InMemoryScan(..) => vec![],
            Self::TabularScan(..)
            | Self::EmptyScan(..)
            | Self::TabularScanParquet(..)
            | Self::TabularScanCsv(..)
            | Self::TabularScanJson(..) => vec![],
            Self::Project(Project { input, .. }) => vec![input],
            Self::Filter(Filter { input, .. }) => vec![input],
            Self::Limit(Limit { input, .. }) => vec![input],
            Self::Explode(Explode { input, .. }) => vec![input],
            Self::Sample(Sample { input, .. }) => vec![input],
            Self::Sort(Sort { input, .. }) => vec![input],
            Self::Split(Split { input, .. }) => vec![input],
            Self::Coalesce(Coalesce { input, .. }) => vec![input],
            Self::Flatten(Flatten { input }) => vec![input],
            Self::FanoutRandom(FanoutRandom { input, .. }) => vec![input],
            Self::FanoutByHash(FanoutByHash { input, .. }) => vec![input],
            Self::FanoutByRange(FanoutByRange { input, .. }) => vec![input],
            Self::ReduceMerge(ReduceMerge { input }) => vec![input],
            Self::Aggregate(Aggregate { input, .. }) => vec![input],
            Self::TabularWriteParquet(TabularWriteParquet { input, .. }) => vec![input],
            Self::TabularWriteCsv(TabularWriteCsv { input, .. }) => vec![input],
            Self::TabularWriteJson(TabularWriteJson { input, .. }) => vec![input],
            Self::HashJoin(HashJoin { left, right, .. }) => vec![left, right],
            Self::BroadcastJoin(BroadcastJoin {
                broadcaster,
                receiver,
                ..
            }) => vec![broadcaster, receiver],
            Self::SortMergeJoin(SortMergeJoin { left, right, .. }) => vec![left, right],
            Self::Concat(Concat { input, other }) => vec![input, other],
            Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. }) => vec![input],
        }
    }

    pub fn with_new_children(&self, children: &[PhysicalPlanRef]) -> PhysicalPlan {
        match children {
            [input] => match self {
                #[cfg(feature = "python")]
                Self::InMemoryScan(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::TabularScan(..)
                | Self::EmptyScan(..)
                | Self::TabularScanParquet(..)
                | Self::TabularScanCsv(..)
                | Self::TabularScanJson(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::Project(Project { projection, resource_request, partition_spec, .. }) => Self::Project(Project::try_new(
                    input.clone(), projection.clone(), resource_request.clone(), partition_spec.clone(),
                ).unwrap()),
                Self::Filter(Filter { predicate, .. }) => Self::Filter(Filter::new(input.clone(), predicate.clone())),
                Self::Limit(Limit { limit, eager, num_partitions, .. }) => Self::Limit(Limit::new(input.clone(), *limit, *eager, *num_partitions)),
                Self::Explode(Explode { to_explode, .. }) => Self::Explode(Explode::try_new(input.clone(), to_explode.clone()).unwrap()),
                Self::Sample(Sample { fraction, with_replacement, seed, .. }) => Self::Sample(Sample::new(input.clone(), *fraction, *with_replacement, *seed)),
                Self::Sort(Sort { sort_by, descending, num_partitions, .. }) => Self::Sort(Sort::new(input.clone(), sort_by.clone(), descending.clone(), *num_partitions)),
                Self::Split(Split { input_num_partitions, output_num_partitions, .. }) => Self::Split(Split::new(input.clone(), *input_num_partitions, *output_num_partitions)),
                Self::Coalesce(Coalesce { num_from, num_to, .. }) => Self::Coalesce(Coalesce::new(input.clone(), *num_from, *num_to)),
                Self::Flatten(..) => Self::Flatten(Flatten::new(input.clone())),
                Self::FanoutRandom(FanoutRandom { num_partitions, .. }) => Self::FanoutRandom(FanoutRandom::new(input.clone(), *num_partitions)),
                Self::FanoutByHash(FanoutByHash { num_partitions, partition_by, .. }) => Self::FanoutByHash(FanoutByHash::new(input.clone(), *num_partitions, partition_by.clone())),
                Self::FanoutByRange(FanoutByRange { num_partitions, sort_by, .. }) => Self::FanoutByRange(FanoutByRange::new(input.clone(), *num_partitions, sort_by.clone())),
                Self::ReduceMerge(..) => Self::ReduceMerge(ReduceMerge::new(input.clone())),
                Self::Aggregate(Aggregate { aggregations, groupby, ..}) => Self::Aggregate(Aggregate::new(input.clone(), aggregations.clone(), groupby.clone())),
                Self::TabularWriteParquet(TabularWriteParquet { schema, file_info, .. }) => Self::TabularWriteParquet(TabularWriteParquet::new(schema.clone(), file_info.clone(), input.clone())),
                Self::TabularWriteCsv(TabularWriteCsv { schema, file_info, .. }) => Self::TabularWriteCsv(TabularWriteCsv::new(schema.clone(), file_info.clone(), input.clone())),
                Self::TabularWriteJson(TabularWriteJson { schema, file_info, .. }) => Self::TabularWriteJson(TabularWriteJson::new(schema.clone(), file_info.clone(), input.clone())),
                _ => panic!("Physical op {:?} has two inputs, but got one", self),
            },
            [input1, input2] => match self {
                #[cfg(feature = "python")]
                Self::InMemoryScan(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::TabularScan(..)
                | Self::EmptyScan(..)
                | Self::TabularScanParquet(..)
                | Self::TabularScanCsv(..)
                | Self::TabularScanJson(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
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
            Self::TabularScanParquet(..) => "TabularScanParquet",
            Self::TabularScanCsv(..) => "TabularScanCsv",
            Self::TabularScanJson(..) => "TabularScanJson",
            Self::Project(..) => "Project",
            Self::Filter(..) => "Filter",
            Self::Limit(..) => "Limit",
            Self::Explode(..) => "Explode",
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
            Self::HashJoin(..) => "HashJoin",
            Self::BroadcastJoin(..) => "BroadcastJoin",
            Self::SortMergeJoin(..) => "SortMergeJoin",
            Self::Concat(..) => "Concat",
            Self::TabularWriteParquet(..) => "TabularWriteParquet",
            Self::TabularWriteCsv(..) => "TabularWriteCsv",
            Self::TabularWriteJson(..) => "TabularWriteJson",
            Self::MonotonicallyIncreasingId(..) => "MonotonicallyIncreasingId",
        };
        name.to_string()
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            #[cfg(feature = "python")]
            Self::InMemoryScan(in_memory_scan) => in_memory_scan.multiline_display(),
            Self::TabularScan(tabular_scan) => tabular_scan.multiline_display(),
            Self::EmptyScan(empty_scan) => empty_scan.multiline_display(),
            Self::TabularScanParquet(tabular_scan_parquet) => {
                tabular_scan_parquet.multiline_display()
            }
            Self::TabularScanCsv(tabular_scan_csv) => tabular_scan_csv.multiline_display(),
            Self::TabularScanJson(tabular_scan_json) => tabular_scan_json.multiline_display(),
            Self::Project(project) => project.multiline_display(),
            Self::Filter(filter) => filter.multiline_display(),
            Self::Limit(limit) => limit.multiline_display(),
            Self::Explode(explode) => explode.multiline_display(),
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
        self.plan.partition_spec().get_num_partitions()
    }
    pub fn partition_spec(&self) -> PyResult<PartitionSpec> {
        Ok(self.plan.partition_spec().as_ref().clone())
    }

    pub fn repr_ascii(&self, simple: bool) -> PyResult<String> {
        Ok(self.plan.repr_ascii(simple))
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
            PhysicalPlan::Explode(Explode {
                input, to_explode, ..
            }) => {
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
            PhysicalPlan::Sample(Sample {
                input,
                fraction,
                with_replacement,
                seed,
            }) => {
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
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
                let upstream_iter = input.to_partition_tasks(py, psets, is_ray_runner)?;
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
            PhysicalPlan::HashJoin(HashJoin {
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
                let left_iter = left.to_partition_tasks(py, psets, is_ray_runner)?;
                let right_iter = right.to_partition_tasks(py, psets, is_ray_runner)?;
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
