use std::{ops::Deref, sync::Arc};

use common_error::DaftResult;
#[cfg(feature = "python")]
use common_py_serde::{deserialize_py_object, serialize_py_object};
use common_resource_request::ResourceRequest;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use common_treenode::{DynTreeNode, TreeNode, TreeNodeRecursion};
use daft_core::{prelude::*, utils::stats::Stats};
use daft_dsl::{
    expr::{
        bound_expr::{BoundAggExpr, BoundExpr, BoundWindowExpr},
        BoundColumn,
    },
    functions::python::get_resource_request,
    Column, WindowExpr, WindowFrame, WindowSpec,
};
use daft_logical_plan::{InMemoryInfo, OutputFileInfo};
use daft_stats::plan_stats::{
    calculate::{
        calculate_actor_pool_project_stats, calculate_concat_stats, calculate_cross_join_stats,
        calculate_dedup_stats, calculate_explode_stats, calculate_filter_stats,
        calculate_hash_aggregate_stats, calculate_hash_join_stats, calculate_limit_stats,
        calculate_monotonically_increasing_id_stats, calculate_pivot_stats,
        calculate_project_stats, calculate_repartition_stats, calculate_sample_stats,
        calculate_scan_stats, calculate_sort_stats, calculate_top_n_stats,
        calculate_ungrouped_aggregate_stats, calculate_unpivot_stats, calculate_window_stats,
        calculate_write_stats,
    },
    ApproxStats, PlanStats, StatsState,
};
use serde::{Deserialize, Serialize};

pub type LocalPhysicalPlanRef = Arc<LocalPhysicalPlan>;
#[derive(Debug, strum::IntoStaticStr, Serialize, Deserialize)]
pub enum LocalPhysicalPlan {
    InMemoryScan(InMemoryScan),
    PhysicalScan(PhysicalScan),
    EmptyScan(EmptyScan),
    Project(Project),
    ActorPoolProject(ActorPoolProject),
    Filter(Filter),
    Limit(Limit),
    Explode(Explode),
    Unpivot(Unpivot),
    Sort(Sort),
    TopN(TopN),
    // Split(Split),
    Sample(Sample),
    MonotonicallyIncreasingId(MonotonicallyIncreasingId),
    // Coalesce(Coalesce),
    // Flatten(Flatten),
    // FanoutRandom(FanoutRandom),
    // FanoutByHash(FanoutByHash),
    // FanoutByRange(FanoutByRange),
    // ReduceMerge(ReduceMerge),
    UnGroupedAggregate(UnGroupedAggregate),
    HashAggregate(HashAggregate),
    Dedup(Dedup),
    Pivot(Pivot),
    Concat(Concat),
    HashJoin(HashJoin),
    CrossJoin(CrossJoin),
    // SortMergeJoin(SortMergeJoin),
    // BroadcastJoin(BroadcastJoin),
    PhysicalWrite(PhysicalWrite),
    CommitWrite(CommitWrite),
    // TabularWriteJson(TabularWriteJson),
    // TabularWriteCsv(TabularWriteCsv),
    #[cfg(feature = "python")]
    CatalogWrite(CatalogWrite),
    #[cfg(feature = "python")]
    LanceWrite(LanceWrite),
    #[cfg(feature = "python")]
    DataSink(DataSink),
    WindowPartitionOnly(WindowPartitionOnly),
    WindowPartitionAndOrderBy(WindowPartitionAndOrderBy),
    WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame),
    WindowOrderByOnly(WindowOrderByOnly),

    // Flotilla Only Nodes
    Repartition(Repartition),
    #[cfg(feature = "python")]
    DistributedActorPoolProject(DistributedActorPoolProject),
}

impl LocalPhysicalPlan {
    #[must_use]
    pub fn name(&self) -> &'static str {
        // uses strum::IntoStaticStr
        self.into()
    }

    #[must_use]
    pub fn arced(self) -> LocalPhysicalPlanRef {
        self.into()
    }

    pub fn materialized_stats(&self) -> &PlanStats {
        match self {
            Self::InMemoryScan(InMemoryScan { stats, .. })
            | Self::PhysicalScan(PhysicalScan { stats, .. })
            | Self::EmptyScan(EmptyScan { stats, .. })
            | Self::Project(Project { stats, .. })
            | Self::ActorPoolProject(ActorPoolProject { stats, .. })
            | Self::Filter(Filter { stats, .. })
            | Self::Limit(Limit { stats, .. })
            | Self::Explode(Explode { stats, .. })
            | Self::Unpivot(Unpivot { stats, .. })
            | Self::Sort(Sort { stats, .. })
            | Self::TopN(TopN { stats, .. })
            | Self::Sample(Sample { stats, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { stats, .. })
            | Self::UnGroupedAggregate(UnGroupedAggregate { stats, .. })
            | Self::HashAggregate(HashAggregate { stats, .. })
            | Self::Dedup(Dedup { stats, .. })
            | Self::Pivot(Pivot { stats, .. })
            | Self::Concat(Concat { stats, .. })
            | Self::HashJoin(HashJoin { stats, .. })
            | Self::CrossJoin(CrossJoin { stats, .. })
            | Self::PhysicalWrite(PhysicalWrite { stats, .. })
            | Self::CommitWrite(CommitWrite { stats, .. })
            | Self::Repartition(Repartition { stats, .. })
            | Self::WindowPartitionOnly(WindowPartitionOnly { stats, .. })
            | Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy { stats, .. })
            | Self::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {
                stats, ..
            })
            | Self::WindowOrderByOnly(WindowOrderByOnly { stats, .. }) => stats,
            #[cfg(feature = "python")]
            Self::CatalogWrite(CatalogWrite { stats, .. })
            | Self::LanceWrite(LanceWrite { stats, .. })
            | Self::DistributedActorPoolProject(DistributedActorPoolProject { stats, .. })
            | Self::DataSink(DataSink { stats, .. }) => stats,
        }
    }

    pub fn in_memory_scan(in_memory_info: InMemoryInfo, stats: StatsState) -> LocalPhysicalPlanRef {
        let stats: PlanStats = match stats {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => PlanStats {
                approx_stats: ApproxStats {
                    num_rows: in_memory_info.num_rows,
                    size_bytes: in_memory_info.size_bytes,
                    acc_selectivity: 0.0,
                },
            },
        };
        Self::InMemoryScan(InMemoryScan {
            info: in_memory_info,
            stats,
        })
        .arced()
    }

    pub fn physical_scan(
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats: PlanStats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_scan_stats(&scan_tasks, &pushdowns, &schema),
        };
        Self::PhysicalScan(PhysicalScan {
            scan_tasks,
            pushdowns,
            schema,
            stats,
        })
        .arced()
    }

    pub fn empty_scan(schema: SchemaRef) -> LocalPhysicalPlanRef {
        Self::EmptyScan(EmptyScan {
            schema,
            stats: PlanStats::empty().into(),
        })
        .arced()
    }

    pub fn filter(
        input: LocalPhysicalPlanRef,
        predicate: BoundExpr,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_filter_stats(input.materialized_stats(), predicate.inner(), &schema)
            }
        };
        Self::Filter(Filter {
            input,
            predicate,
            schema,
            stats,
        })
        .arced()
    }

    pub fn limit(
        input: LocalPhysicalPlanRef,
        num_rows: u64,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_limit_stats(input.materialized_stats(), num_rows)
            }
        };
        Self::Limit(Limit {
            input,
            num_rows,
            schema,
            stats,
        })
        .arced()
    }

    pub fn explode(
        input: LocalPhysicalPlanRef,
        to_explode: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_explode_stats(input.materialized_stats()),
        };
        Self::Explode(Explode {
            input,
            to_explode,
            schema,
            stats,
        })
        .arced()
    }

    pub fn project(
        input: LocalPhysicalPlanRef,
        projection: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_project_stats(input.materialized_stats()),
        };
        Self::Project(Project {
            input,
            projection,
            schema,
            stats,
        })
        .arced()
    }

    pub(crate) fn actor_pool_project(
        input: LocalPhysicalPlanRef,
        projection: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_actor_pool_project_stats(input.materialized_stats())
            }
        };
        Self::ActorPoolProject(ActorPoolProject {
            input,
            projection,
            schema,
            stats,
        })
        .arced()
    }

    #[cfg(feature = "python")]
    pub fn distributed_actor_pool_project(
        input: LocalPhysicalPlanRef,
        actor_objects: Vec<daft_dsl::pyobj_serde::PyObjectWrapper>,
        batch_size: Option<usize>,
        memory_request: u64,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_actor_pool_project_stats(input.materialized_stats())
            }
        };
        Self::DistributedActorPoolProject(DistributedActorPoolProject {
            input,
            actor_objects,
            batch_size,
            memory_request,
            schema,
            stats,
        })
        .arced()
    }
    pub fn ungrouped_aggregate(
        input: LocalPhysicalPlanRef,
        aggregations: Vec<BoundAggExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_ungrouped_aggregate_stats(input.materialized_stats())
            }
        };
        Self::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            stats,
        })
        .arced()
    }

    pub fn hash_aggregate(
        input: LocalPhysicalPlanRef,
        aggregations: Vec<BoundAggExpr>,
        group_by: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_hash_aggregate_stats(
                input.materialized_stats(),
                &group_by.iter().map(|e| e.as_ref()).collect::<Vec<_>>(),
            ),
        };
        Self::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            schema,
            stats,
        })
        .arced()
    }

    pub fn dedup(
        input: LocalPhysicalPlanRef,
        columns: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_dedup_stats(input.materialized_stats()),
        };
        Self::Dedup(Dedup {
            input,
            columns,
            schema,
            stats,
        })
        .arced()
    }

    pub fn window_partition_only(
        input: LocalPhysicalPlanRef,
        partition_by: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
        aggregations: Vec<BoundAggExpr>,
        aliases: Vec<String>,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_window_stats(input.materialized_stats()),
        };
        Self::WindowPartitionOnly(WindowPartitionOnly {
            input,
            partition_by,
            schema,
            stats,
            aggregations,
            aliases,
        })
        .arced()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn window_partition_and_order_by(
        input: LocalPhysicalPlanRef,
        partition_by: Vec<BoundExpr>,
        order_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        schema: SchemaRef,
        stats_state: StatsState,
        functions: Vec<BoundWindowExpr>,
        aliases: Vec<String>,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_window_stats(input.materialized_stats()),
        };
        Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy {
            input,
            partition_by,
            order_by,
            descending,
            nulls_first,
            schema,
            stats,
            functions,
            aliases,
        })
        .arced()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn window_partition_and_dynamic_frame(
        input: LocalPhysicalPlanRef,
        partition_by: Vec<BoundExpr>,
        order_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        frame: WindowFrame,
        min_periods: usize,
        schema: SchemaRef,
        stats_state: StatsState,
        aggregations: Vec<BoundAggExpr>,
        aliases: Vec<String>,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_window_stats(input.materialized_stats()),
        };
        Self::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {
            input,
            partition_by,
            order_by,
            descending,
            nulls_first,
            frame,
            min_periods,
            schema,
            stats,
            aggregations,
            aliases,
        })
        .arced()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn window_order_by_only(
        input: LocalPhysicalPlanRef,
        order_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        schema: SchemaRef,
        stats_state: StatsState,
        functions: Vec<BoundWindowExpr>,
        aliases: Vec<String>,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_window_stats(input.materialized_stats()),
        };
        Self::WindowOrderByOnly(WindowOrderByOnly {
            input,
            order_by,
            descending,
            nulls_first,
            schema,
            stats,
            functions,
            aliases,
        })
        .arced()
    }

    pub fn unpivot(
        input: LocalPhysicalPlanRef,
        ids: Vec<BoundExpr>,
        values: Vec<BoundExpr>,
        variable_name: String,
        value_name: String,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_unpivot_stats(input.materialized_stats(), values.len())
            }
        };
        Self::Unpivot(Unpivot {
            input,
            ids,
            values,
            variable_name,
            value_name,
            schema,
            stats,
        })
        .arced()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn pivot(
        input: LocalPhysicalPlanRef,
        group_by: Vec<BoundExpr>,
        pivot_column: BoundExpr,
        value_column: BoundExpr,
        aggregation: BoundAggExpr,
        names: Vec<String>,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_pivot_stats(input.materialized_stats()),
        };
        Self::Pivot(Pivot {
            input,
            group_by,
            pivot_column,
            value_column,
            aggregation,
            names,
            schema,
            stats,
        })
        .arced()
    }

    pub fn sort(
        input: LocalPhysicalPlanRef,
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_sort_stats(input.materialized_stats()),
        };
        Self::Sort(Sort {
            input,
            sort_by,
            descending,
            nulls_first,
            schema,
            stats,
        })
        .arced()
    }

    pub fn top_n(
        input: LocalPhysicalPlanRef,
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        limit: u64,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_top_n_stats(input.materialized_stats(), limit),
        };
        Self::TopN(TopN {
            input,
            sort_by,
            descending,
            nulls_first,
            limit,
            schema,
            stats,
        })
        .arced()
    }

    pub fn sample(
        input: LocalPhysicalPlanRef,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_sample_stats(input.materialized_stats(), fraction)
            }
        };
        Self::Sample(Sample {
            input,
            fraction,
            with_replacement,
            seed,
            schema,
            stats,
        })
        .arced()
    }

    pub fn monotonically_increasing_id(
        input: LocalPhysicalPlanRef,
        column_name: String,
        starting_offset: Option<u64>,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_monotonically_increasing_id_stats(input.materialized_stats())
            }
        };
        Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
            input,
            column_name,
            starting_offset,
            schema,
            stats,
        })
        .arced()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn hash_join(
        left: LocalPhysicalPlanRef,
        right: LocalPhysicalPlanRef,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        null_equals_null: Option<Vec<bool>>,
        join_type: JoinType,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_hash_join_stats(left.materialized_stats(), right.materialized_stats())
            }
        };
        Self::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
            null_equals_null,
            join_type,
            schema,
            stats,
        })
        .arced()
    }

    pub fn cross_join(
        left: LocalPhysicalPlanRef,
        right: LocalPhysicalPlanRef,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_cross_join_stats(left.materialized_stats(), right.materialized_stats())
            }
        };
        Self::CrossJoin(CrossJoin {
            left,
            right,
            schema,
            stats,
        })
        .arced()
    }

    pub(crate) fn concat(
        input: LocalPhysicalPlanRef,
        other: LocalPhysicalPlanRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => {
                calculate_concat_stats(input.materialized_stats(), other.materialized_stats())
            }
        };
        Self::Concat(Concat {
            input,
            other,
            schema,
            stats,
        })
        .arced()
    }

    pub fn physical_write(
        input: LocalPhysicalPlanRef,
        data_schema: SchemaRef,
        file_schema: SchemaRef,
        file_info: OutputFileInfo<BoundExpr>,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_write_stats(input.materialized_stats()),
        };
        Self::PhysicalWrite(PhysicalWrite {
            input,
            data_schema,
            file_schema,
            file_info,
            stats,
        })
        .arced()
    }

    pub fn commit_write(
        input: LocalPhysicalPlanRef,
        file_schema: SchemaRef,
        file_info: OutputFileInfo<BoundExpr>,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_write_stats(input.materialized_stats()),
        };
        Self::CommitWrite(CommitWrite {
            input,
            file_schema,
            file_info,
            stats,
        })
        .arced()
    }

    #[cfg(feature = "python")]
    pub fn catalog_write(
        input: LocalPhysicalPlanRef,
        catalog_type: daft_logical_plan::CatalogType<BoundExpr>,
        data_schema: SchemaRef,
        file_schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_write_stats(input.materialized_stats()),
        };
        Self::CatalogWrite(CatalogWrite {
            input,
            catalog_type,
            data_schema,
            file_schema,
            stats,
        })
        .arced()
    }

    #[cfg(feature = "python")]
    pub fn lance_write(
        input: LocalPhysicalPlanRef,
        lance_info: daft_logical_plan::LanceCatalogInfo,
        data_schema: SchemaRef,
        file_schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_write_stats(input.materialized_stats()),
        };
        Self::LanceWrite(LanceWrite {
            input,
            lance_info,
            data_schema,
            file_schema,
            stats,
        })
        .arced()
    }

    #[cfg(feature = "python")]
    pub fn data_sink(
        input: LocalPhysicalPlanRef,
        data_sink_info: daft_logical_plan::DataSinkInfo,
        file_schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_write_stats(input.materialized_stats()),
        };
        Self::DataSink(DataSink {
            input,
            data_sink_info,
            file_schema,
            stats,
        })
        .arced()
    }

    pub fn repartition(
        input: LocalPhysicalPlanRef,
        columns: Vec<BoundExpr>,
        num_partitions: usize,
        schema: SchemaRef,
        stats_state: StatsState,
    ) -> LocalPhysicalPlanRef {
        let stats = match stats_state {
            StatsState::Materialized(stats) => stats.deref().clone(),
            StatsState::NotMaterialized => calculate_repartition_stats(input.materialized_stats()),
        };
        Self::Repartition(Repartition {
            input,
            columns,
            num_partitions,
            schema,
            stats,
        })
        .arced()
    }

    pub fn schema(&self) -> &SchemaRef {
        match self {
            Self::PhysicalScan(PhysicalScan { schema, .. })
            | Self::EmptyScan(EmptyScan { schema, .. })
            | Self::Filter(Filter { schema, .. })
            | Self::Limit(Limit { schema, .. })
            | Self::Project(Project { schema, .. })
            | Self::ActorPoolProject(ActorPoolProject { schema, .. })
            | Self::UnGroupedAggregate(UnGroupedAggregate { schema, .. })
            | Self::HashAggregate(HashAggregate { schema, .. })
            | Self::Dedup(Dedup { schema, .. })
            | Self::Pivot(Pivot { schema, .. })
            | Self::Sort(Sort { schema, .. })
            | Self::TopN(TopN { schema, .. })
            | Self::Sample(Sample { schema, .. })
            | Self::HashJoin(HashJoin { schema, .. })
            | Self::CrossJoin(CrossJoin { schema, .. })
            | Self::Explode(Explode { schema, .. })
            | Self::Unpivot(Unpivot { schema, .. })
            | Self::Concat(Concat { schema, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { schema, .. })
            | Self::WindowPartitionOnly(WindowPartitionOnly { schema, .. })
            | Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy { schema, .. })
            | Self::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {
                schema, ..
            })
            | Self::WindowOrderByOnly(WindowOrderByOnly { schema, .. }) => schema,
            Self::PhysicalWrite(PhysicalWrite { file_schema, .. }) => file_schema,
            Self::CommitWrite(CommitWrite { file_schema, .. }) => file_schema,
            Self::InMemoryScan(InMemoryScan { info, .. }) => &info.source_schema,
            #[cfg(feature = "python")]
            Self::CatalogWrite(CatalogWrite { file_schema, .. }) => file_schema,
            #[cfg(feature = "python")]
            Self::LanceWrite(LanceWrite { file_schema, .. }) => file_schema,
            #[cfg(feature = "python")]
            Self::DataSink(DataSink { file_schema, .. }) => file_schema,
            #[cfg(feature = "python")]
            Self::DistributedActorPoolProject(DistributedActorPoolProject { schema, .. }) => schema,
            Self::Repartition(Repartition { schema, .. }) => schema,
            Self::WindowPartitionOnly(WindowPartitionOnly { schema, .. }) => schema,
            Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy { schema, .. }) => schema,
        }
    }

    pub fn resource_request(self: &Arc<Self>) -> ResourceRequest {
        let mut base = ResourceRequest::default();
        self.apply(|plan| match plan.as_ref() {
            Self::Project(Project {
                projection, stats, ..
            }) => {
                if let Some(resource_request) = get_resource_request(projection) {
                    base = base.max(&resource_request);
                } else {
                    base = base.max(
                        &ResourceRequest::default_cpu()
                            .or_memory_bytes(Some(stats.approx_stats.size_bytes))?,
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            }
            Self::ActorPoolProject(ActorPoolProject {
                projection, stats, ..
            }) => {
                if let Some(resource_request) = get_resource_request(projection) {
                    base = base.max(&resource_request);
                } else {
                    base = base.max(
                        &ResourceRequest::default_cpu()
                            .or_memory_bytes(Some(stats.approx_stats.size_bytes))?,
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            }
            #[cfg(feature = "python")]
            Self::DistributedActorPoolProject(DistributedActorPoolProject {
                memory_request,
                ..
            }) => {
                base = base.max(
                    &ResourceRequest::default().or_memory_bytes(Some(*memory_request as usize))?,
                );
                Ok(TreeNodeRecursion::Continue)
            }
            _ => {
                let stats = plan.materialized_stats();
                let memory_bytes = stats.approx_stats.size_bytes;
                base = base.max(&ResourceRequest::default().or_memory_bytes(Some(memory_bytes))?);
                Ok(TreeNodeRecursion::Continue)
            }
        });
        base
    }

    fn children(&self) -> Vec<LocalPhysicalPlanRef> {
        match self {
            Self::PhysicalScan(_) | Self::EmptyScan(_) | Self::InMemoryScan(_) => vec![],
            Self::Filter(Filter { input, .. })
            | Self::Limit(Limit { input, .. })
            | Self::Project(Project { input, .. })
            | Self::ActorPoolProject(ActorPoolProject { input, .. })
            | Self::UnGroupedAggregate(UnGroupedAggregate { input, .. })
            | Self::HashAggregate(HashAggregate { input, .. })
            | Self::Dedup(Dedup { input, .. })
            | Self::Pivot(Pivot { input, .. })
            | Self::Sort(Sort { input, .. })
            | Self::Sample(Sample { input, .. })
            | Self::Explode(Explode { input, .. })
            | Self::Unpivot(Unpivot { input, .. })
            | Self::Concat(Concat { input, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. })
            | Self::WindowPartitionOnly(WindowPartitionOnly { input, .. })
            | Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy { input, .. })
            | Self::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {
                input, ..
            })
            | Self::PhysicalWrite(PhysicalWrite { input, .. })
            | Self::CommitWrite(CommitWrite { input, .. }) => vec![input.clone()],

            Self::HashJoin(HashJoin { left, right, .. }) => vec![left.clone(), right.clone()],
            Self::CrossJoin(CrossJoin { left, right, .. }) => vec![left.clone(), right.clone()],
            #[cfg(feature = "python")]
            Self::CatalogWrite(CatalogWrite { input, .. }) => vec![input.clone()],
            #[cfg(feature = "python")]
            Self::LanceWrite(LanceWrite { input, .. }) => vec![input.clone()],
            #[cfg(feature = "python")]
            Self::DataSink(DataSink { input, .. }) => vec![input.clone()],
            #[cfg(feature = "python")]
            Self::DistributedActorPoolProject(DistributedActorPoolProject { input, .. }) => {
                vec![input.clone()]
            }
            Self::Repartition(Repartition { input, .. }) => vec![input.clone()],
            Self::TopN(TopN { input, .. }) => vec![input.clone()],
            Self::WindowOrderByOnly(WindowOrderByOnly { input, .. }) => vec![input.clone()],
        }
    }

    pub fn with_new_children(&self, children: &[Arc<Self>]) -> Arc<Self> {
        match children {
            [new_child] => match self {
                Self::PhysicalScan(_) | Self::EmptyScan(_)
                | Self::InMemoryScan(_) => panic!("LocalPhysicalPlan::with_new_children: PhysicalScan, PlaceholderScan, EmptyScan, and InMemoryScan do not have children"),
                Self::Filter(Filter {  predicate, schema,..  }) => Self::filter(new_child.clone(), predicate.clone(), StatsState::NotMaterialized),
                Self::Limit(Limit {  num_rows, .. }) => Self::limit(new_child.clone(), *num_rows, StatsState::NotMaterialized),
                Self::Project(Project {  projection, schema, .. }) => Self::project(new_child.clone(), projection.clone(), schema.clone(), StatsState::NotMaterialized),
                Self::ActorPoolProject(ActorPoolProject {  projection, schema, .. }) => Self::actor_pool_project(new_child.clone(), projection.clone(), schema.clone(), StatsState::NotMaterialized),
                Self::UnGroupedAggregate(UnGroupedAggregate {  aggregations, schema, .. }) => Self::ungrouped_aggregate(new_child.clone(), aggregations.clone(), schema.clone(), StatsState::NotMaterialized),
                Self::HashAggregate(HashAggregate {  aggregations, group_by, schema, .. }) => Self::hash_aggregate(new_child.clone(), aggregations.clone(), group_by.clone(), schema.clone(), StatsState::NotMaterialized),
                Self::Dedup(Dedup {  columns, schema, .. }) => Self::dedup(new_child.clone(), columns.clone(), schema.clone(), StatsState::NotMaterialized),
                Self::Pivot(Pivot {  group_by, pivot_column, value_column, aggregation, names, schema, .. }) => Self::pivot(new_child.clone(), group_by.clone(), pivot_column.clone(), value_column.clone(), aggregation.clone(), names.clone(), schema.clone(), StatsState::NotMaterialized),
                Self::Sort(Sort {  sort_by, descending, nulls_first, schema, .. }) => Self::sort(new_child.clone(), sort_by.clone(), descending.clone(), nulls_first.clone(), StatsState::NotMaterialized),
                Self::Sample(Sample {  fraction, with_replacement, seed, schema, .. }) => Self::sample(new_child.clone(), *fraction, *with_replacement, *seed, StatsState::NotMaterialized),
                Self::Explode(Explode {  to_explode, schema, .. }) => Self::explode(new_child.clone(), to_explode.clone(), schema.clone(), StatsState::NotMaterialized),
                Self::Unpivot(Unpivot {  ids, values, variable_name, value_name, schema, .. }) => Self::unpivot(new_child.clone(), ids.clone(), values.clone(), variable_name.clone(), value_name.clone(), schema.clone(), StatsState::NotMaterialized),
                Self::Concat(Concat {  other, schema, .. }) => Self::concat(new_child.clone(), other.clone(), StatsState::NotMaterialized),
                Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId {  column_name, starting_offset, schema, .. }) => Self::monotonically_increasing_id(new_child.clone(), column_name.clone(), *starting_offset, schema.clone(), StatsState::NotMaterialized),
                Self::WindowPartitionOnly(WindowPartitionOnly {  partition_by, schema, aggregations, aliases, .. }) => Self::window_partition_only(new_child.clone(), partition_by.clone(), schema.clone(), StatsState::NotMaterialized, aggregations.clone(), aliases.clone()),
                Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy {  partition_by, order_by, descending, nulls_first, schema, functions, aliases, .. }) => Self::window_partition_and_order_by(new_child.clone(), partition_by.clone(), order_by.clone(), descending.clone(), nulls_first.clone(), schema.clone(), StatsState::NotMaterialized, functions.clone(), aliases.clone()),
                Self::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {  partition_by, order_by, descending, nulls_first, frame, min_periods, schema, aggregations, aliases, .. }) => Self::window_partition_and_dynamic_frame(new_child.clone(), partition_by.clone(), order_by.clone(), descending.clone(), nulls_first.clone(), frame.clone(), *min_periods, schema.clone(), StatsState::NotMaterialized, aggregations.clone(), aliases.clone()),
                Self::WindowOrderByOnly(WindowOrderByOnly {  order_by, descending, nulls_first, schema, functions, aliases, .. }) => Self::window_order_by_only(new_child.clone(), order_by.clone(), descending.clone(), nulls_first.clone(), schema.clone(), StatsState::NotMaterialized, functions.clone(), aliases.clone()),
                Self::TopN(TopN {  sort_by, descending, nulls_first, limit, schema, .. }) => Self::top_n(new_child.clone(), sort_by.clone(), descending.clone(), nulls_first.clone(), *limit, StatsState::NotMaterialized),
                Self::PhysicalWrite(PhysicalWrite {  data_schema, file_schema, file_info, .. }) => Self::physical_write(new_child.clone(), data_schema.clone(), file_schema.clone(), file_info.clone(), StatsState::NotMaterialized),
                Self::CommitWrite(CommitWrite {  input, file_schema, file_info, .. }) => Self::commit_write(new_child.clone(), file_schema.clone(), file_info.clone(), StatsState::NotMaterialized),
                #[cfg(feature = "python")]
                Self::DataSink(DataSink {  input, data_sink_info, file_schema, .. }) => Self::data_sink(new_child.clone(), data_sink_info.clone(), file_schema.clone(), StatsState::NotMaterialized),
                #[cfg(feature = "python")]
                Self::CatalogWrite(CatalogWrite {  catalog_type, data_schema, file_schema, .. }) => Self::catalog_write(new_child.clone(), catalog_type.clone(), data_schema.clone(), file_schema.clone(), StatsState::NotMaterialized),
                #[cfg(feature = "python")]
                Self::LanceWrite(LanceWrite {  lance_info, data_schema, file_schema, .. }) => Self::lance_write(new_child.clone(), lance_info.clone(), data_schema.clone(), file_schema.clone(), StatsState::NotMaterialized),
                #[cfg(feature = "python")]
                Self::DistributedActorPoolProject(DistributedActorPoolProject {  actor_objects, schema, batch_size, memory_request, .. }) => Self::distributed_actor_pool_project(new_child.clone(), actor_objects.clone(), *batch_size, *memory_request, schema.clone(), StatsState::NotMaterialized),
                Self::Repartition(Repartition {  columns, num_partitions, schema, .. }) => Self::repartition(new_child.clone(), columns.clone(), *num_partitions, schema.clone(), StatsState::NotMaterialized),
                Self::HashJoin(_) => panic!("LocalPhysicalPlan::with_new_children: HashJoin should have 2 children"),
                Self::CrossJoin(_) => panic!("LocalPhysicalPlan::with_new_children: CrossJoin should have 2 children"),
                Self::Concat(_) => panic!("LocalPhysicalPlan::with_new_children: Concat should have 2 children"),
            },
            [new_left, new_right] => match self {
                Self::HashJoin(HashJoin {  left_on, right_on, null_equals_null, join_type, schema, .. }) => {
                    Self::hash_join(new_left.clone(), new_right.clone(), left_on.clone(), right_on.clone(), null_equals_null.clone(), *join_type, schema.clone(), StatsState::NotMaterialized)
                }
                Self::CrossJoin(CrossJoin { schema, .. }) => {
                    Self::cross_join(new_left.clone(), new_right.clone(), schema.clone(), StatsState::NotMaterialized)
                }
                Self::Concat(Concat {  ..}) => {
                    Self::concat(new_left.clone(), new_right.clone(), StatsState::NotMaterialized)
                }
                _ => panic!("LocalPhysicalPlan::with_new_children: Wrong number of children"),
            },
            _ => panic!("LocalPhysicalPlan::with_new_children: Wrong number of children"),
        }
    }

    pub fn single_line_display(&self) -> String {
        let children = self.children();
        if children.is_empty() {
            self.name().to_string()
        } else if children.len() == 1 {
            format!("{}->{}", children[0].single_line_display(), self.name())
        } else {
            // For multiple children, show them in parentheses
            let child_names: Vec<String> = children
                .iter()
                .map(|child| child.single_line_display())
                .collect();
            format!("({})->{}", child_names.join(", "), self.name())
        }
    }
}

impl DynTreeNode for LocalPhysicalPlan {
    fn arc_children(&self) -> Vec<Arc<Self>> {
        self.children()
    }

    fn with_new_arc_children(self: Arc<Self>, children: Vec<Arc<Self>>) -> DaftResult<Arc<Self>> {
        let old_children = self.arc_children();
        if children.len() != old_children.len() {
            panic!("LocalPhysicalPlan::with_new_arc_children: Wrong number of children")
        } else if children.is_empty()
            || children
                .iter()
                .zip(old_children.iter())
                .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
        {
            Ok(self.with_new_children(&children))
        } else {
            Ok(self)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InMemoryScan {
    pub info: InMemoryInfo,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PhysicalScan {
    pub scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    pub pushdowns: Pushdowns,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmptyScan {
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Project {
    pub input: LocalPhysicalPlanRef,
    pub projection: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActorPoolProject {
    pub input: LocalPhysicalPlanRef,
    pub projection: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[cfg(feature = "python")]
#[derive(Debug, Serialize, Deserialize)]
pub struct DistributedActorPoolProject {
    pub input: LocalPhysicalPlanRef,
    pub actor_objects: Vec<daft_dsl::pyobj_serde::PyObjectWrapper>,
    pub batch_size: Option<usize>,
    pub memory_request: u64,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Filter {
    pub input: LocalPhysicalPlanRef,
    pub predicate: BoundExpr,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Limit {
    pub input: LocalPhysicalPlanRef,
    pub num_rows: u64,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Explode {
    pub input: LocalPhysicalPlanRef,
    pub to_explode: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sort {
    pub input: LocalPhysicalPlanRef,
    pub sort_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopN {
    pub input: LocalPhysicalPlanRef,
    pub sort_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub limit: u64,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sample {
    pub input: LocalPhysicalPlanRef,
    pub fraction: f64,
    pub with_replacement: bool,
    pub seed: Option<u64>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MonotonicallyIncreasingId {
    pub input: LocalPhysicalPlanRef,
    pub column_name: String,
    pub starting_offset: Option<u64>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnGroupedAggregate {
    pub input: LocalPhysicalPlanRef,
    pub aggregations: Vec<BoundAggExpr>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HashAggregate {
    pub input: LocalPhysicalPlanRef,
    pub aggregations: Vec<BoundAggExpr>,
    pub group_by: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Dedup {
    pub input: LocalPhysicalPlanRef,
    pub columns: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Unpivot {
    pub input: LocalPhysicalPlanRef,
    pub ids: Vec<BoundExpr>,
    pub values: Vec<BoundExpr>,
    pub variable_name: String,
    pub value_name: String,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pivot {
    pub input: LocalPhysicalPlanRef,
    pub group_by: Vec<BoundExpr>,
    pub pivot_column: BoundExpr,
    pub value_column: BoundExpr,
    pub aggregation: BoundAggExpr,
    pub names: Vec<String>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HashJoin {
    pub left: LocalPhysicalPlanRef,
    pub right: LocalPhysicalPlanRef,
    pub left_on: Vec<BoundExpr>,
    pub right_on: Vec<BoundExpr>,
    pub null_equals_null: Option<Vec<bool>>,
    pub join_type: JoinType,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrossJoin {
    pub left: LocalPhysicalPlanRef,
    pub right: LocalPhysicalPlanRef,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Concat {
    pub input: LocalPhysicalPlanRef,
    pub other: LocalPhysicalPlanRef,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PhysicalWrite {
    pub input: LocalPhysicalPlanRef,
    pub data_schema: SchemaRef,
    pub file_schema: SchemaRef,
    pub file_info: OutputFileInfo<BoundExpr>,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitWrite {
    pub input: LocalPhysicalPlanRef,
    pub file_schema: SchemaRef,
    pub file_info: OutputFileInfo<BoundExpr>,
    pub stats: PlanStats,
}

#[cfg(feature = "python")]
#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogWrite {
    pub input: LocalPhysicalPlanRef,
    pub catalog_type: daft_logical_plan::CatalogType<BoundExpr>,
    pub data_schema: SchemaRef,
    pub file_schema: SchemaRef,
    pub stats: PlanStats,
}

#[cfg(feature = "python")]
#[derive(Debug, Serialize, Deserialize)]
pub struct LanceWrite {
    pub input: LocalPhysicalPlanRef,
    pub lance_info: daft_logical_plan::LanceCatalogInfo,
    pub data_schema: SchemaRef,
    pub file_schema: SchemaRef,
    pub stats: PlanStats,
}

#[cfg(feature = "python")]
#[derive(Debug, Serialize, Deserialize)]
pub struct DataSink {
    pub input: LocalPhysicalPlanRef,
    pub data_sink_info: daft_logical_plan::DataSinkInfo,
    pub file_schema: SchemaRef,
    pub stats: PlanStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowPartitionOnly {
    pub input: LocalPhysicalPlanRef,
    pub partition_by: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
    pub aggregations: Vec<BoundAggExpr>,
    pub aliases: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowPartitionAndOrderBy {
    pub input: LocalPhysicalPlanRef,
    pub partition_by: Vec<BoundExpr>,
    pub order_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
    pub functions: Vec<BoundWindowExpr>,
    pub aliases: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowPartitionAndDynamicFrame {
    pub input: LocalPhysicalPlanRef,
    pub partition_by: Vec<BoundExpr>,
    pub order_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub frame: WindowFrame,
    pub min_periods: usize,
    pub schema: SchemaRef,
    pub stats: PlanStats,
    pub aggregations: Vec<BoundAggExpr>,
    pub aliases: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowOrderByOnly {
    pub input: LocalPhysicalPlanRef,
    pub order_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub schema: SchemaRef,
    pub stats: PlanStats,
    pub functions: Vec<BoundWindowExpr>,
    pub aliases: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Repartition {
    pub input: LocalPhysicalPlanRef,
    pub columns: Vec<BoundExpr>,
    pub num_partitions: usize,
    pub schema: SchemaRef,
    pub stats: PlanStats,
}
