use std::{cmp::max, collections::HashSet, sync::Arc};

use common_display::ascii::AsciiTreeDisplay;
use daft_logical_plan::{
    partitioning::{
        ClusteringSpec, HashClusteringConfig, RangeClusteringConfig, UnknownClusteringConfig,
    },
    stats::ApproxStats,
};
use serde::{Deserialize, Serialize};

use super::ops::*;

pub type PhysicalPlanRef = Arc<PhysicalPlan>;

/// Physical plan for a Daft query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PhysicalPlan {
    InMemoryScan(InMemoryScan),
    TabularScan(TabularScan),
    EmptyScan(EmptyScan),
    PreviousStageScan(PreviousStageScan),
    Project(Project),
    ActorPoolProject(ActorPoolProject),
    Filter(Filter),
    Limit(Limit),
    Explode(Explode),
    Unpivot(Unpivot),
    Sort(Sort),
    TopN(TopN),
    Sample(Sample),
    MonotonicallyIncreasingId(MonotonicallyIncreasingId),
    Aggregate(Aggregate),
    Dedup(Dedup),
    Pivot(Pivot),
    Concat(Concat),
    HashJoin(HashJoin),
    SortMergeJoin(SortMergeJoin),
    BroadcastJoin(BroadcastJoin),
    CrossJoin(CrossJoin),
    TabularWriteParquet(TabularWriteParquet),
    TabularWriteJson(TabularWriteJson),
    TabularWriteCsv(TabularWriteCsv),
    ShuffleExchange(ShuffleExchange),
    #[cfg(feature = "python")]
    IcebergWrite(IcebergWrite),
    #[cfg(feature = "python")]
    DeltaLakeWrite(DeltaLakeWrite),
    #[cfg(feature = "python")]
    LanceWrite(LanceWrite),
    #[cfg(feature = "python")]
    DataSink(DataSink),
}

impl PhysicalPlan {
    pub fn arced(self) -> PhysicalPlanRef {
        Arc::new(self)
    }

    pub fn clustering_spec(&self) -> Arc<ClusteringSpec> {
        // TODO: add cache or something to avoid excessive recalculation
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
            Self::PreviousStageScan(PreviousStageScan {
                clustering_spec, ..
            }) => clustering_spec.clone(),
            Self::Project(Project {
                clustering_spec, ..
            }) => clustering_spec.clone(),
            Self::ActorPoolProject(ActorPoolProject {
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
                input.clustering_spec()
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
            Self::TopN(TopN {
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
            Self::ShuffleExchange(shuffle_exchange) => shuffle_exchange.clustering_spec(),
            Self::Aggregate(Aggregate {
                input,
                aggregations,
                ..
            }) => {
                // PhysicalPlan aggregates are local aggregations
                //
                // If the local aggregation modifies the partition columns (unlikely), the clustering spec is invalidated
                //
                // If the groupby keys are the partition columns (very likely, since we often partition by hash on the groupby keys), the
                // clustering spec is still valid
                let input_partition_by = input.clustering_spec().partition_by();
                let input_partition_col_names: HashSet<&str> =
                    input_partition_by.iter().map(|e| e.name()).collect();
                if aggregations
                    .iter()
                    .map(|e| e.name())
                    .any(|name| input_partition_col_names.contains(name))
                {
                    ClusteringSpec::Unknown(UnknownClusteringConfig::new(
                        input.clustering_spec().num_partitions(),
                    ))
                    .into()
                } else {
                    input.clustering_spec()
                }
            }
            Self::Dedup(Dedup { input, .. }) => input.clustering_spec(),
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
                std::iter::repeat_n(false, left_on.len()).collect(),
            ))
            .into(),
            Self::CrossJoin(CrossJoin {
                clustering_spec, ..
            }) => clustering_spec.clone(),
            Self::TabularWriteParquet(TabularWriteParquet { input, .. }) => input.clustering_spec(),
            Self::TabularWriteCsv(TabularWriteCsv { input, .. }) => input.clustering_spec(),
            Self::TabularWriteJson(TabularWriteJson { input, .. }) => input.clustering_spec(),
            #[cfg(feature = "python")]
            Self::IcebergWrite(_)
            | Self::DeltaLakeWrite(_)
            | Self::LanceWrite(_)
            | Self::DataSink(_) => ClusteringSpec::Unknown(UnknownClusteringConfig::new(1)).into(),
        }
    }

    pub fn approximate_stats(&self) -> ApproxStats {
        match self {
            Self::InMemoryScan(InMemoryScan { in_memory_info, .. }) => ApproxStats {
                num_rows: in_memory_info.num_rows,
                size_bytes: in_memory_info.size_bytes,
                acc_selectivity: 1.0,
            },
            Self::TabularScan(TabularScan { scan_tasks, .. }) => {
                let mut approx_stats = ApproxStats::empty();
                for st in scan_tasks.iter() {
                    if let Some(num_rows) = st.num_rows() {
                        approx_stats.num_rows += num_rows;
                    } else if let Some(approx_num_rows) = st.approx_num_rows(None) {
                        approx_stats.num_rows += approx_num_rows as usize;
                    }
                    approx_stats.size_bytes += st.estimate_in_memory_size_bytes(None).unwrap_or(0);
                }
                approx_stats.acc_selectivity = if scan_tasks.is_empty() {
                    0.0
                } else {
                    let st = scan_tasks.first().unwrap();
                    st.pushdowns().estimated_selectivity(st.schema().as_ref())
                };
                approx_stats
            }
            Self::EmptyScan(..) => ApproxStats {
                num_rows: 0,
                size_bytes: 0,
                acc_selectivity: 0.0,
            },
            Self::PreviousStageScan(..) => ApproxStats {
                num_rows: 0,
                size_bytes: 0,
                acc_selectivity: 0.0,
            },
            // Assume no row/column pruning in cardinality-affecting operations.
            // TODO(Clark): Estimate row/column pruning to get a better size approximation.
            Self::Filter(Filter {
                input,
                estimated_selectivity,
                ..
            }) => {
                let input_stats = input.approximate_stats();
                ApproxStats {
                    num_rows: (input_stats.num_rows as f64 * estimated_selectivity).ceil() as usize,
                    size_bytes: (input_stats.size_bytes as f64 * estimated_selectivity).ceil()
                        as usize,
                    acc_selectivity: input_stats.acc_selectivity * estimated_selectivity,
                }
            }
            Self::Limit(Limit { input, limit, .. }) | Self::TopN(TopN { input, limit, .. }) => {
                let input_stats = input.approximate_stats();
                let limit = *limit as usize;
                let limit_selectivity = if input_stats.num_rows > limit {
                    if input_stats.num_rows == 0 {
                        0.0
                    } else {
                        limit as f64 / input_stats.num_rows as f64
                    }
                } else {
                    1.0
                };
                ApproxStats {
                    num_rows: limit.min(input_stats.num_rows),
                    size_bytes: if input_stats.num_rows > limit {
                        let est_bytes_per_row =
                            input_stats.size_bytes / input_stats.num_rows.max(1);
                        limit * est_bytes_per_row
                    } else {
                        input_stats.size_bytes
                    },
                    acc_selectivity: input_stats.acc_selectivity * limit_selectivity,
                }
            }
            Self::Project(Project { input, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. })
            | Self::ActorPoolProject(ActorPoolProject { input, .. }) => {
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
                const EXPLODE_FACTOR: usize = 4;
                let est_num_exploded_rows = input_stats.num_rows * EXPLODE_FACTOR;
                let acc_selectivity = if input_stats.num_rows == 0 {
                    0.0
                } else {
                    input_stats.acc_selectivity * EXPLODE_FACTOR as f64
                };
                ApproxStats {
                    num_rows: est_num_exploded_rows,
                    size_bytes: input_stats.size_bytes,
                    acc_selectivity,
                }
            }
            // Propagate child approximation for operations that don't affect cardinality.
            Self::Sort(Sort { input, .. })
            | Self::Pivot(Pivot { input, .. })
            | Self::ShuffleExchange(ShuffleExchange { input, .. }) => input.approximate_stats(),
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
            | Self::SortMergeJoin(SortMergeJoin { left, right, .. })
            | Self::CrossJoin(CrossJoin { left, right, .. }) => {
                // assume a Primary-key + Foreign-Key join which would yield the max of the two tables
                let left_stats = left.approximate_stats();
                let right_stats = right.approximate_stats();

                // We assume that if one side of a join had its cardinality reduced by some operations
                // (e.g. filters, limits, aggregations), then assuming a pk-fk join, the total number of
                // rows output from the join will be reduced proportionally. Hence, apply the right side's
                // selectivity to the number of rows/size in bytes on the left and vice versa.
                let left_num_rows = left_stats.num_rows as f64 * right_stats.acc_selectivity;
                let right_num_rows = right_stats.num_rows as f64 * left_stats.acc_selectivity;
                let left_size = left_stats.size_bytes as f64 * right_stats.acc_selectivity;
                let right_size = right_stats.size_bytes as f64 * left_stats.acc_selectivity;
                ApproxStats {
                    num_rows: left_num_rows.max(right_num_rows).ceil() as usize,
                    size_bytes: left_size.max(right_size).ceil() as usize,
                    acc_selectivity: left_stats.acc_selectivity * right_stats.acc_selectivity,
                }
            }
            // TODO(Clark): Approximate post-aggregation sizes via grouping estimates + aggregation type.
            Self::Aggregate(Aggregate { input, groupby, .. }) => {
                let input_stats = input.approximate_stats();
                // TODO we should use schema inference here
                let est_bytes_per_row = input_stats.size_bytes / (input_stats.num_rows.max(1));
                let acc_selectivity = if input_stats.num_rows == 0 {
                    0.0
                } else {
                    input_stats.acc_selectivity / input_stats.num_rows as f64
                };
                if groupby.is_empty() {
                    ApproxStats {
                        num_rows: 1,
                        size_bytes: est_bytes_per_row,
                        acc_selectivity,
                    }
                } else {
                    // Assume high cardinality for group by columns, and 80% of rows are unique.
                    let est_num_groups = input_stats.num_rows * 4 / 5;
                    ApproxStats {
                        num_rows: est_num_groups,
                        size_bytes: est_bytes_per_row * est_num_groups,
                        acc_selectivity: input_stats.acc_selectivity * est_num_groups as f64
                            / input_stats.num_rows as f64,
                    }
                }
            }
            Self::Dedup(Dedup { input, .. }) => {
                let input_stats = input.approximate_stats();
                // TODO we should use schema inference here
                let est_bytes_per_row = input_stats.size_bytes / (input_stats.num_rows.max(1));
                // Assume high cardinality for group by columns, and 80% of rows are unique.
                let est_num_groups = input_stats.num_rows * 4 / 5;
                ApproxStats {
                    num_rows: est_num_groups,
                    size_bytes: est_bytes_per_row * est_num_groups,
                    acc_selectivity: input_stats.acc_selectivity * est_num_groups as f64
                        / input_stats.num_rows as f64,
                }
            }
            Self::Unpivot(Unpivot { input, values, .. }) => {
                let input_stats = input.approximate_stats();
                let num_values = values.len();
                ApproxStats {
                    num_rows: input_stats.num_rows * num_values,
                    size_bytes: input_stats.size_bytes,
                    acc_selectivity: input_stats.acc_selectivity * num_values as f64,
                }
            }
            // Post-write DataFrame will contain paths to files that were written.
            // TODO(Clark): Estimate output size via root directory and estimates for # of partitions given partitioning column.
            Self::TabularWriteParquet(_) | Self::TabularWriteCsv(_) | Self::TabularWriteJson(_) => {
                ApproxStats::empty()
            }
            #[cfg(feature = "python")]
            Self::IcebergWrite(_)
            | Self::DeltaLakeWrite(_)
            | Self::LanceWrite(_)
            | Self::DataSink(_) => ApproxStats::empty(),
        }
    }

    pub fn children(&self) -> Vec<&Self> {
        match self {
            Self::InMemoryScan(..) => vec![],
            Self::TabularScan(..) | Self::EmptyScan(..) | Self::PreviousStageScan(..) => vec![],
            Self::Project(Project { input, .. }) => vec![input],
            Self::ActorPoolProject(ActorPoolProject { input, .. }) => vec![input],
            Self::Filter(Filter { input, .. }) => vec![input],
            Self::Limit(Limit { input, .. }) => vec![input],
            Self::Explode(Explode { input, .. }) => vec![input],
            Self::Unpivot(Unpivot { input, .. }) => vec![input],
            Self::Sample(Sample { input, .. }) => vec![input],
            Self::Sort(Sort { input, .. }) => vec![input],
            Self::TopN(TopN { input, .. }) => vec![input],
            Self::Aggregate(Aggregate { input, .. }) => vec![input],
            Self::Dedup(Dedup { input, .. }) => vec![input],
            Self::Pivot(Pivot { input, .. }) => vec![input],
            Self::TabularWriteParquet(TabularWriteParquet { input, .. }) => vec![input],
            Self::TabularWriteCsv(TabularWriteCsv { input, .. }) => vec![input],
            Self::TabularWriteJson(TabularWriteJson { input, .. }) => vec![input],
            Self::ShuffleExchange(ShuffleExchange { input, .. }) => vec![input],
            #[cfg(feature = "python")]
            Self::IcebergWrite(IcebergWrite { input, .. }) => vec![input],
            #[cfg(feature = "python")]
            Self::DeltaLakeWrite(DeltaLakeWrite { input, .. }) => vec![input],
            #[cfg(feature = "python")]
            Self::LanceWrite(LanceWrite { input, .. }) => vec![input],
            #[cfg(feature = "python")]
            Self::DataSink(DataSink { input, .. }) => vec![input],
            Self::HashJoin(HashJoin { left, right, .. }) => vec![left, right],
            Self::BroadcastJoin(BroadcastJoin {
                broadcaster,
                receiver,
                ..
            }) => vec![broadcaster, receiver],
            Self::SortMergeJoin(SortMergeJoin { left, right, .. }) => {
                vec![left, right]
            }
            Self::CrossJoin(CrossJoin { left, right, .. }) => vec![left, right],
            Self::Concat(Concat { input, other }) => vec![input, other],
            Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. }) => {
                vec![input]
            }
        }
    }

    pub fn with_new_children(&self, children: &[PhysicalPlanRef]) -> Self {
        match children {
            [input] => match self {
                Self::InMemoryScan(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::TabularScan(..)
                | Self::EmptyScan(..)
                | Self::PreviousStageScan(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::Project(Project { projection, clustering_spec, .. }) =>
                    Self::Project(Project::new_with_clustering_spec(
                    input.clone(), projection.clone(), clustering_spec.clone(),
                ).unwrap()),

                Self::ActorPoolProject(ActorPoolProject {projection, ..}) => Self::ActorPoolProject(ActorPoolProject::try_new(input.clone(), projection.clone()).unwrap()),
                Self::Filter(Filter { predicate, estimated_selectivity,.. }) => Self::Filter(Filter::new(input.clone(), predicate.clone(), *estimated_selectivity)),
                Self::Limit(Limit { limit, eager, num_partitions, .. }) => Self::Limit(Limit::new(input.clone(), *limit, *eager, *num_partitions)),
                Self::TopN(TopN { sort_by, descending, nulls_first, limit, num_partitions, .. }) => Self::TopN(TopN::new(input.clone(), sort_by.clone(), descending.clone(), nulls_first.clone(), *limit, *num_partitions)),
                Self::Explode(Explode { to_explode, .. }) => Self::Explode(Explode::try_new(input.clone(), to_explode.clone()).unwrap()),
                Self::Unpivot(Unpivot { ids, values, variable_name, value_name, .. }) => Self::Unpivot(Unpivot::new(input.clone(), ids.clone(), values.clone(), variable_name, value_name)),
                Self::Pivot(Pivot { group_by, pivot_column, value_column, names, .. }) => Self::Pivot(Pivot::new(input.clone(), group_by.clone(), pivot_column.clone(), value_column.clone(), names.clone())),
                Self::Sample(Sample { fraction, with_replacement, seed, .. }) => Self::Sample(Sample::new(input.clone(), *fraction, *with_replacement, *seed)),
                Self::Sort(Sort { sort_by, descending, nulls_first,  num_partitions, .. }) => Self::Sort(Sort::new(input.clone(), sort_by.clone(), descending.clone(),nulls_first.clone(), *num_partitions)),
                Self::ShuffleExchange(ShuffleExchange { strategy, .. }) => Self::ShuffleExchange(ShuffleExchange { input: input.clone(), strategy: strategy.clone() }),
                Self::Aggregate(Aggregate { aggregations, groupby, ..}) => Self::Aggregate(Aggregate::new(input.clone(), aggregations.clone(), groupby.clone())),
                Self::Dedup(Dedup { columns, .. }) => Self::Dedup(Dedup::new(input.clone(), columns.clone())),
                Self::TabularWriteParquet(TabularWriteParquet { schema, file_info, .. }) => Self::TabularWriteParquet(TabularWriteParquet::new(schema.clone(), file_info.clone(), input.clone())),
                Self::TabularWriteCsv(TabularWriteCsv { schema, file_info, .. }) => Self::TabularWriteCsv(TabularWriteCsv::new(schema.clone(), file_info.clone(), input.clone())),
                Self::TabularWriteJson(TabularWriteJson { schema, file_info, .. }) => Self::TabularWriteJson(TabularWriteJson::new(schema.clone(), file_info.clone(), input.clone())),
                Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { column_name, .. }) => Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId::new(input.clone(), column_name)),
                #[cfg(feature = "python")]
                Self::IcebergWrite(IcebergWrite { schema, iceberg_info, .. }) => Self::IcebergWrite(IcebergWrite::new(schema.clone(), iceberg_info.clone(), input.clone())),
                #[cfg(feature = "python")]
                Self::DeltaLakeWrite(DeltaLakeWrite {schema, delta_lake_info, .. }) => Self::DeltaLakeWrite(DeltaLakeWrite::new(schema.clone(), delta_lake_info.clone(), input.clone())),
                #[cfg(feature = "python")]
                Self::LanceWrite(LanceWrite { schema, lance_info, .. }) => Self::LanceWrite(LanceWrite::new(schema.clone(), lance_info.clone(), input.clone())),
                #[cfg(feature = "python")]
                Self::DataSink(DataSink { schema, data_sink_info, .. }) => Self::DataSink(DataSink::new(schema.clone(), data_sink_info.clone(), input.clone())),
                Self::Concat(_) | Self::HashJoin(_) | Self::SortMergeJoin(_) | Self::BroadcastJoin(_) | Self::CrossJoin(_) => panic!("{} requires more than 1 input, but received: {}", self, children.len()),
            },
            [input1, input2] => match self {
                #[cfg(feature = "python")]
                Self::InMemoryScan(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::TabularScan(..)
                | Self::EmptyScan(..) => panic!("Source nodes don't have children, with_new_children() should never be called for source ops"),
                Self::HashJoin(HashJoin { left_on, right_on, null_equals_nulls, join_type, .. }) => Self::HashJoin(HashJoin::new(input1.clone(), input2.clone(), left_on.clone(), right_on.clone(), null_equals_nulls.clone(), *join_type)),
                Self::BroadcastJoin(BroadcastJoin {
                    left_on,
                    right_on,
                    null_equals_nulls,
                    join_type,
                    is_swapped,
                    ..
                }) => Self::BroadcastJoin(BroadcastJoin::new(input1.clone(), input2.clone(), left_on.clone(), right_on.clone(), null_equals_nulls.clone(), *join_type, *is_swapped)),
                Self::SortMergeJoin(SortMergeJoin { left_on, right_on, join_type, num_partitions, left_is_larger, needs_presort, .. }) => Self::SortMergeJoin(SortMergeJoin::new(input1.clone(), input2.clone(), left_on.clone(), right_on.clone(), *join_type, *num_partitions, *left_is_larger, *needs_presort)),
                Self::CrossJoin(CrossJoin { outer_loop_side, .. }) => Self::CrossJoin(CrossJoin::new(input1.clone(), input2.clone(), *outer_loop_side)),
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
            Self::PreviousStageScan(..) => "PreviousStageScan",
            Self::Project(..) => "Project",
            Self::ActorPoolProject(..) => "ActorPoolProject",
            Self::Filter(..) => "Filter",
            Self::Limit(..) => "Limit",
            Self::TopN(..) => "TopN",
            Self::Explode(..) => "Explode",
            Self::Unpivot(..) => "Unpivot",
            Self::Sample(..) => "Sample",
            Self::Sort(..) => "Sort",
            Self::ShuffleExchange(..) => "ShuffleExchange",
            Self::Aggregate(..) => "Aggregate",
            Self::Dedup(..) => "Dedup",
            Self::Pivot(..) => "Pivot",
            Self::HashJoin(..) => "HashJoin",
            Self::BroadcastJoin(..) => "BroadcastJoin",
            Self::SortMergeJoin(..) => "SortMergeJoin",
            Self::CrossJoin(..) => "CrossJoin",
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
            #[cfg(feature = "python")]
            Self::DataSink(..) => "DataSinkWrite",
        };
        name.to_string()
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::InMemoryScan(in_memory_scan) => in_memory_scan.multiline_display(),
            Self::TabularScan(tabular_scan) => tabular_scan.multiline_display(),
            Self::EmptyScan(empty_scan) => empty_scan.multiline_display(),
            Self::PreviousStageScan(previous_stage_scan) => previous_stage_scan.multiline_display(),
            Self::Project(project) => project.multiline_display(),
            Self::ActorPoolProject(ap_project) => ap_project.multiline_display(),
            Self::Filter(filter) => filter.multiline_display(),
            Self::Limit(limit) => limit.multiline_display(),
            Self::TopN(top_n) => top_n.multiline_display(),
            Self::Explode(explode) => explode.multiline_display(),
            Self::Unpivot(unpivot) => unpivot.multiline_display(),
            Self::Sample(sample) => sample.multiline_display(),
            Self::Sort(sort) => sort.multiline_display(),
            Self::ShuffleExchange(shuffle_exchange) => shuffle_exchange.multiline_display(),
            Self::Aggregate(aggregate) => aggregate.multiline_display(),
            Self::Dedup(dedup) => dedup.multiline_display(),
            Self::Pivot(pivot) => pivot.multiline_display(),
            Self::HashJoin(hash_join) => hash_join.multiline_display(),
            Self::BroadcastJoin(broadcast_join) => broadcast_join.multiline_display(),
            Self::SortMergeJoin(sort_merge_join) => sort_merge_join.multiline_display(),
            Self::CrossJoin(cross_join) => cross_join.multiline_display(),
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
            #[cfg(feature = "python")]
            Self::DataSink(data_sink_info) => data_sink_info.multiline_display(),
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
