use serde::{Deserialize, Serialize};
use std::{cmp::max, ops::Add, sync::Arc};

use crate::{
    display::TreeDisplay,
    partitioning::{
        ClusteringSpec, HashClusteringConfig, RandomClusteringConfig, RangeClusteringConfig,
        UnknownClusteringConfig,
    },
    physical_ops::*,
};

pub type PhysicalPlanRef = Arc<PhysicalPlan>;

/// Physical plan for a Daft query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PhysicalPlan {
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
    #[cfg(feature = "python")]
    DeltaLakeWrite(DeltaLakeWrite),
    #[cfg(feature = "python")]
    LanceWrite(LanceWrite),
}

pub struct ApproxStats {
    pub lower_bound_rows: usize,
    pub upper_bound_rows: Option<usize>,
    pub lower_bound_bytes: usize,
    pub upper_bound_bytes: Option<usize>,
}

impl ApproxStats {
    fn empty() -> Self {
        ApproxStats {
            lower_bound_rows: 0,
            upper_bound_rows: None,
            lower_bound_bytes: 0,
            upper_bound_bytes: None,
        }
    }
    fn apply<F: Fn(usize) -> usize>(&self, f: F) -> Self {
        ApproxStats {
            lower_bound_rows: f(self.lower_bound_rows),
            upper_bound_rows: self.upper_bound_rows.map(&f),
            lower_bound_bytes: f(self.lower_bound_rows),
            upper_bound_bytes: self.upper_bound_bytes.map(&f),
        }
    }
}

impl Add for &ApproxStats {
    type Output = ApproxStats;
    fn add(self, rhs: Self) -> Self::Output {
        ApproxStats {
            lower_bound_rows: self.lower_bound_rows + rhs.lower_bound_rows,
            upper_bound_rows: self
                .upper_bound_rows
                .and_then(|l_ub| rhs.upper_bound_rows.map(|v| v + l_ub)),
            lower_bound_bytes: self.lower_bound_bytes + rhs.lower_bound_bytes,
            upper_bound_bytes: self
                .upper_bound_bytes
                .and_then(|l_ub| rhs.upper_bound_bytes.map(|v| v + l_ub)),
        }
    }
}

impl PhysicalPlan {
    pub fn arced(self) -> PhysicalPlanRef {
        Arc::new(self)
    }

    pub fn clustering_spec(&self) -> Arc<ClusteringSpec> {
        match self {
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
            Self::IcebergWrite(_) | Self::DeltaLakeWrite(_) | Self::LanceWrite(_) => {
                ClusteringSpec::Unknown(UnknownClusteringConfig::new(1)).into()
            }
        }
    }

    pub fn approximate_stats(&self) -> ApproxStats {
        match self {
            Self::InMemoryScan(InMemoryScan { in_memory_info, .. }) => ApproxStats {
                lower_bound_rows: in_memory_info.num_rows,
                upper_bound_rows: Some(in_memory_info.num_rows),
                lower_bound_bytes: in_memory_info.size_bytes,
                upper_bound_bytes: Some(in_memory_info.size_bytes),
            },
            Self::TabularScan(TabularScan { scan_tasks, .. }) => {
                let mut stats = ApproxStats::empty();
                for st in scan_tasks.iter() {
                    stats.lower_bound_rows += st.num_rows().unwrap_or(0);
                    let in_memory_size = st.estimate_in_memory_size_bytes(None);
                    stats.lower_bound_bytes += in_memory_size.unwrap_or(0);
                    stats.upper_bound_rows = stats
                        .upper_bound_rows
                        .and_then(|st_ub| st.upper_bound_rows().map(|ub| st_ub + ub));
                    stats.upper_bound_bytes = stats
                        .upper_bound_bytes
                        .and_then(|st_ub| in_memory_size.map(|ub| st_ub + ub));
                }
                stats
            }
            Self::EmptyScan(..) => ApproxStats {
                lower_bound_rows: 0,
                upper_bound_rows: Some(0),
                lower_bound_bytes: 0,
                upper_bound_bytes: Some(0),
            },
            // Assume no row/column pruning in cardinality-affecting operations.
            // TODO(Clark): Estimate row/column pruning to get a better size approximation.
            Self::Filter(Filter { input, .. }) => {
                let input_stats = input.approximate_stats();
                ApproxStats {
                    lower_bound_rows: 0,
                    upper_bound_rows: input_stats.upper_bound_rows,
                    lower_bound_bytes: 0,
                    upper_bound_bytes: input_stats.upper_bound_bytes,
                }
            }
            Self::Limit(Limit { input, limit, .. }) => {
                let limit = *limit as usize;
                let input_stats = input.approximate_stats();
                let est_bytes_per_row_lower =
                    input_stats.lower_bound_bytes / (input_stats.lower_bound_rows.max(1));
                let est_bytes_per_row_upper = input_stats
                    .upper_bound_bytes
                    .and_then(|bytes| input_stats.upper_bound_rows.map(|rows| bytes / rows.max(1)));
                let new_lower_rows = input_stats.lower_bound_rows.min(limit);
                let new_upper_rows = input_stats
                    .upper_bound_rows
                    .map(|ub| ub.min(limit))
                    .unwrap_or(limit);
                ApproxStats {
                    lower_bound_rows: new_lower_rows,
                    upper_bound_rows: Some(new_upper_rows),
                    lower_bound_bytes: new_lower_rows * est_bytes_per_row_lower,
                    upper_bound_bytes: est_bytes_per_row_upper.map(|x| x * new_upper_rows),
                }
            }
            Self::Project(Project { input, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. }) => {
                // TODO(sammy), we need the schema to estimate the new size per row
                input.approximate_stats()
            }
            Self::Sample(Sample {
                input, fraction, ..
            }) => input
                .approximate_stats()
                .apply(|v| ((v as f64) * fraction) as usize),
            Self::Explode(Explode { input, .. }) => {
                let input_stats = input.approximate_stats();
                ApproxStats {
                    lower_bound_rows: input_stats.lower_bound_rows,
                    upper_bound_rows: None,
                    lower_bound_bytes: input_stats.lower_bound_bytes,
                    upper_bound_bytes: None,
                }
            }
            // Propagate child approximation for operations that don't affect cardinality.
            Self::Coalesce(Coalesce { input, .. })
            | Self::FanoutByHash(FanoutByHash { input, .. })
            | Self::FanoutByRange(FanoutByRange { input, .. })
            | Self::FanoutRandom(FanoutRandom { input, .. })
            | Self::Flatten(Flatten { input, .. })
            | Self::ReduceMerge(ReduceMerge { input, .. })
            | Self::Sort(Sort { input, .. })
            | Self::Split(Split { input, .. })
            | Self::Pivot(Pivot { input, .. }) => input.approximate_stats(),
            Self::Concat(Concat { input, other }) => {
                &input.approximate_stats() + &other.approximate_stats()
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
                // assume a Primary-key + Foreign-Key join which would yield the max of the two tables
                let left_stats = left.approximate_stats();
                let right_stats = right.approximate_stats();

                ApproxStats {
                    lower_bound_rows: 0,
                    upper_bound_rows: left_stats
                        .upper_bound_rows
                        .and_then(|l| right_stats.upper_bound_rows.map(|r| l.max(r))),
                    lower_bound_bytes: 0,
                    upper_bound_bytes: left_stats
                        .upper_bound_bytes
                        .and_then(|l| right_stats.upper_bound_bytes.map(|r| l.max(r))),
                }
            }
            // TODO(Clark): Approximate post-aggregation sizes via grouping estimates + aggregation type.
            Self::Aggregate(Aggregate { input, groupby, .. }) => {
                let input_stats = input.approximate_stats();
                // TODO we should use schema inference here
                let est_bytes_per_row_lower =
                    input_stats.lower_bound_bytes / (input_stats.lower_bound_rows.max(1));
                let est_bytes_per_row_upper = input_stats
                    .upper_bound_bytes
                    .and_then(|bytes| input_stats.upper_bound_rows.map(|rows| bytes / rows.max(1)));
                if groupby.is_empty() {
                    ApproxStats {
                        lower_bound_rows: input_stats.lower_bound_rows.min(1),
                        upper_bound_rows: Some(1),
                        lower_bound_bytes: input_stats.lower_bound_bytes.min(1)
                            * est_bytes_per_row_lower,
                        upper_bound_bytes: est_bytes_per_row_upper,
                    }
                } else {
                    // we should use the new schema here
                    ApproxStats {
                        lower_bound_rows: input_stats.lower_bound_rows.min(1),
                        upper_bound_rows: input_stats.upper_bound_rows,
                        lower_bound_bytes: input_stats.lower_bound_bytes.min(1)
                            * est_bytes_per_row_lower,
                        upper_bound_bytes: input_stats.upper_bound_bytes,
                    }
                }
            }
            Self::Unpivot(Unpivot { input, values, .. }) => {
                let input_stats = input.approximate_stats();
                let num_values = values.len();
                // the number of bytes should be the name but nows should be multiplied by num_values
                ApproxStats {
                    lower_bound_rows: input_stats.lower_bound_rows * num_values,
                    upper_bound_rows: input_stats.upper_bound_rows.map(|v| v * num_values),
                    lower_bound_bytes: input_stats.lower_bound_bytes,
                    upper_bound_bytes: input_stats.upper_bound_bytes,
                }
            }
            // Post-write DataFrame will contain paths to files that were written.
            // TODO(Clark): Estimate output size via root directory and estimates for # of partitions given partitioning column.
            Self::TabularWriteParquet(_) | Self::TabularWriteCsv(_) | Self::TabularWriteJson(_) => {
                ApproxStats::empty()
            }
            #[cfg(feature = "python")]
            Self::IcebergWrite(_) | Self::DeltaLakeWrite(_) | Self::LanceWrite(_) => {
                ApproxStats::empty()
            }
        }
    }

    pub fn children(&self) -> Vec<PhysicalPlanRef> {
        match self {
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
            #[cfg(feature = "python")]
            Self::DeltaLakeWrite(DeltaLakeWrite { input, .. }) => vec![input.clone()],
            #[cfg(feature = "python")]
            Self::LanceWrite(LanceWrite { input, .. }) => vec![input.clone()],
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
                #[cfg(feature = "python")]
                Self::DeltaLakeWrite(DeltaLakeWrite {schema, delta_lake_info, .. }) => Self::DeltaLakeWrite(DeltaLakeWrite::new(schema.clone(), delta_lake_info.clone(), input.clone())),
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
            #[cfg(feature = "python")]
            Self::DeltaLakeWrite(..) => "DeltaLakeWrite",
            #[cfg(feature = "python")]
            Self::LanceWrite(..) => "LanceWrite",
        };
        name.to_string()
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
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
            #[cfg(feature = "python")]
            Self::DeltaLakeWrite(delta_lake_info) => delta_lake_info.multiline_display(),
            #[cfg(feature = "python")]
            Self::LanceWrite(lance_info) => lance_info.multiline_display(),
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
