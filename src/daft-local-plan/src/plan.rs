use std::{
    collections::HashMap,
    sync::{Arc, LockResult},
};

use common_error::{DaftError, DaftResult, ensure};
use common_io_config::IOConfig;
#[cfg(feature = "python")]
use common_py_serde::{PyObjectWrapper, deserialize_py_object, serialize_py_object};
use common_resource_request::ResourceRequest;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use common_treenode::{DynTreeNode, TreeNode, TreeNodeRecursion};
use daft_core::{join::JoinSide, prelude::*};
use daft_dsl::{
    Column, ExprRef, WindowExpr, WindowFrame, WindowSpec,
    expr::{
        BoundColumn, VLLMExpr,
        bound_expr::{BoundAggExpr, BoundExpr, BoundVLLMExpr, BoundWindowExpr},
    },
    functions::python::{RuntimePyObject, UDFProperties, get_resource_request},
};
use daft_logical_plan::{
    InMemoryInfo, OutputFileInfo,
    partitioning::RepartitionSpec,
    stats::{PlanStats, StatsState},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LocalNodeContext {
    pub origin_node_id: Option<usize>,
    pub additional: Option<HashMap<String, String>>,
}

pub type LocalPhysicalPlanRef = Arc<LocalPhysicalPlan>;
#[derive(strum::IntoStaticStr, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum LocalPhysicalPlan {
    InMemoryScan(InMemoryScan),
    PhysicalScan(PhysicalScan),
    GlobScan(GlobScan),
    EmptyScan(EmptyScan),
    PlaceholderScan(PlaceholderScan),
    Project(Project),
    UDFProject(UDFProject),
    Filter(Filter),
    IntoBatches(IntoBatches),
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
    IntoPartitions(IntoPartitions),
    SortMergeJoin(SortMergeJoin),
    #[cfg(feature = "python")]
    DistributedActorPoolProject(DistributedActorPoolProject),
    VLLMProject(VLLMProject),
}
#[cfg(not(debug_assertions))]
impl std::fmt::Debug for LocalPhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
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

    pub fn get_stats_state(&self) -> &StatsState {
        match self {
            Self::InMemoryScan(InMemoryScan { stats_state, .. })
            | Self::PhysicalScan(PhysicalScan { stats_state, .. })
            | Self::GlobScan(GlobScan { stats_state, .. })
            | Self::PlaceholderScan(PlaceholderScan { stats_state, .. })
            | Self::EmptyScan(EmptyScan { stats_state, .. })
            | Self::Project(Project { stats_state, .. })
            | Self::UDFProject(UDFProject { stats_state, .. })
            | Self::Filter(Filter { stats_state, .. })
            | Self::IntoBatches(IntoBatches { stats_state, .. })
            | Self::Limit(Limit { stats_state, .. })
            | Self::Explode(Explode { stats_state, .. })
            | Self::Unpivot(Unpivot { stats_state, .. })
            | Self::Sort(Sort { stats_state, .. })
            | Self::TopN(TopN { stats_state, .. })
            | Self::Sample(Sample { stats_state, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { stats_state, .. })
            | Self::UnGroupedAggregate(UnGroupedAggregate { stats_state, .. })
            | Self::HashAggregate(HashAggregate { stats_state, .. })
            | Self::Dedup(Dedup { stats_state, .. })
            | Self::Pivot(Pivot { stats_state, .. })
            | Self::Concat(Concat { stats_state, .. })
            | Self::HashJoin(HashJoin { stats_state, .. })
            | Self::CrossJoin(CrossJoin { stats_state, .. })
            | Self::SortMergeJoin(SortMergeJoin { stats_state, .. })
            | Self::PhysicalWrite(PhysicalWrite { stats_state, .. })
            | Self::CommitWrite(CommitWrite { stats_state, .. })
            | Self::Repartition(Repartition { stats_state, .. })
            | Self::IntoPartitions(IntoPartitions { stats_state, .. })
            | Self::WindowPartitionOnly(WindowPartitionOnly { stats_state, .. })
            | Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy { stats_state, .. })
            | Self::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {
                stats_state,
                ..
            })
            | Self::WindowOrderByOnly(WindowOrderByOnly { stats_state, .. })
            | Self::VLLMProject(VLLMProject { stats_state, .. }) => stats_state,
            #[cfg(feature = "python")]
            Self::CatalogWrite(CatalogWrite { stats_state, .. })
            | Self::LanceWrite(LanceWrite { stats_state, .. })
            | Self::DistributedActorPoolProject(DistributedActorPoolProject {
                stats_state, ..
            })
            | Self::DataSink(DataSink { stats_state, .. }) => stats_state,
        }
    }

    #[allow(dead_code)]
    pub fn context(&self) -> &LocalNodeContext {
        match self {
            Self::InMemoryScan(InMemoryScan { context, .. })
            | Self::PhysicalScan(PhysicalScan { context, .. })
            | Self::GlobScan(GlobScan { context, .. })
            | Self::PlaceholderScan(PlaceholderScan { context, .. })
            | Self::EmptyScan(EmptyScan { context, .. })
            | Self::Project(Project { context, .. })
            | Self::UDFProject(UDFProject { context, .. })
            | Self::Filter(Filter { context, .. })
            | Self::IntoBatches(IntoBatches { context, .. })
            | Self::Limit(Limit { context, .. })
            | Self::Explode(Explode { context, .. })
            | Self::Unpivot(Unpivot { context, .. })
            | Self::Sort(Sort { context, .. })
            | Self::TopN(TopN { context, .. })
            | Self::Sample(Sample { context, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { context, .. })
            | Self::UnGroupedAggregate(UnGroupedAggregate { context, .. })
            | Self::HashAggregate(HashAggregate { context, .. })
            | Self::Dedup(Dedup { context, .. })
            | Self::Pivot(Pivot { context, .. })
            | Self::Concat(Concat { context, .. })
            | Self::HashJoin(HashJoin { context, .. })
            | Self::CrossJoin(CrossJoin { context, .. })
            | Self::SortMergeJoin(SortMergeJoin { context, .. })
            | Self::PhysicalWrite(PhysicalWrite { context, .. })
            | Self::CommitWrite(CommitWrite { context, .. })
            | Self::Repartition(Repartition { context, .. })
            | Self::IntoPartitions(IntoPartitions { context, .. })
            | Self::WindowPartitionOnly(WindowPartitionOnly { context, .. })
            | Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy { context, .. })
            | Self::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {
                context,
                ..
            })
            | Self::VLLMProject(VLLMProject { context, .. }) => context,
            Self::WindowOrderByOnly(WindowOrderByOnly { context, .. }) => context,
            #[cfg(feature = "python")]
            Self::CatalogWrite(CatalogWrite { context, .. })
            | Self::LanceWrite(LanceWrite { context, .. })
            | Self::DistributedActorPoolProject(DistributedActorPoolProject { context, .. })
            | Self::DataSink(DataSink { context, .. }) => context,
        }
    }

    pub fn in_memory_scan(
        in_memory_info: InMemoryInfo,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::InMemoryScan(InMemoryScan {
            info: in_memory_info,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn physical_scan(
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::PhysicalScan(PhysicalScan {
            scan_tasks,
            pushdowns,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn glob_scan(
        glob_paths: Arc<Vec<String>>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        stats_state: StatsState,
        io_config: Option<IOConfig>,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::GlobScan(GlobScan {
            glob_paths,
            pushdowns,
            schema,
            stats_state,
            io_config,
            context,
        })
        .arced()
    }

    pub fn placeholder_scan(
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::PlaceholderScan(PlaceholderScan {
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn empty_scan(schema: SchemaRef, context: LocalNodeContext) -> LocalPhysicalPlanRef {
        Self::EmptyScan(EmptyScan {
            schema,
            stats_state: StatsState::Materialized(PlanStats::empty().into()),
            context,
        })
        .arced()
    }

    pub fn filter(
        input: LocalPhysicalPlanRef,
        predicate: BoundExpr,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::Filter(Filter {
            input,
            predicate,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn into_batches(
        input: LocalPhysicalPlanRef,
        batch_size: usize,
        strict: bool,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::IntoBatches(IntoBatches {
            input,
            batch_size,
            strict,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn limit(
        input: LocalPhysicalPlanRef,
        limit: u64,
        offset: Option<u64>,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::Limit(Limit {
            input,
            limit,
            offset,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn explode(
        input: LocalPhysicalPlanRef,
        to_explode: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::Explode(Explode {
            input,
            to_explode,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn project(
        input: LocalPhysicalPlanRef,
        projection: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::Project(Project {
            input,
            projection,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn udf_project(
        input: LocalPhysicalPlanRef,
        expr: BoundExpr,
        udf_properties: UDFProperties,
        passthrough_columns: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::UDFProject(UDFProject {
            input,
            expr,
            udf_properties,
            passthrough_columns,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    #[cfg(feature = "python")]
    pub fn distributed_actor_pool_project(
        input: LocalPhysicalPlanRef,
        actor_objects: Vec<PyObjectWrapper>,
        batch_size: Option<usize>,
        memory_request: u64,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::DistributedActorPoolProject(DistributedActorPoolProject {
            input,
            actor_objects,
            batch_size,
            memory_request,
            schema,
            stats_state,
            context,
        })
        .arced()
    }
    pub fn ungrouped_aggregate(
        input: LocalPhysicalPlanRef,
        aggregations: Vec<BoundAggExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn hash_aggregate(
        input: LocalPhysicalPlanRef,
        aggregations: Vec<BoundAggExpr>,
        group_by: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn dedup(
        input: LocalPhysicalPlanRef,
        columns: Vec<BoundExpr>,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::Dedup(Dedup {
            input,
            columns,
            schema,
            stats_state,
            context,
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
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::WindowPartitionOnly(WindowPartitionOnly {
            input,
            partition_by,
            schema,
            stats_state,
            aggregations,
            aliases,
            context,
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
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy {
            input,
            partition_by,
            order_by,
            descending,
            nulls_first,
            schema,
            stats_state,
            functions,
            aliases,
            context,
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
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {
            input,
            partition_by,
            order_by,
            descending,
            nulls_first,
            frame,
            min_periods,
            schema,
            stats_state,
            aggregations,
            aliases,
            context,
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
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::WindowOrderByOnly(WindowOrderByOnly {
            input,
            order_by,
            descending,
            nulls_first,
            schema,
            stats_state,
            functions,
            aliases,
            context,
        })
        .arced()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn unpivot(
        input: LocalPhysicalPlanRef,
        ids: Vec<BoundExpr>,
        values: Vec<BoundExpr>,
        variable_name: String,
        value_name: String,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::Unpivot(Unpivot {
            input,
            ids,
            values,
            variable_name,
            value_name,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn pivot(
        input: LocalPhysicalPlanRef,
        group_by: Vec<BoundExpr>,
        pivot_column: BoundExpr,
        value_column: BoundExpr,
        aggregation: BoundAggExpr,
        names: Vec<String>,
        pre_agg: bool,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::Pivot(Pivot {
            input,
            group_by,
            pivot_column,
            value_column,
            aggregation,
            names,
            pre_agg,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn sort(
        input: LocalPhysicalPlanRef,
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::Sort(Sort {
            input,
            sort_by,
            descending,
            nulls_first,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn top_n(
        input: LocalPhysicalPlanRef,
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        limit: u64,
        offset: Option<u64>,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::TopN(TopN {
            input,
            sort_by,
            descending,
            nulls_first,
            limit,
            offset,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn sample(
        input: LocalPhysicalPlanRef,
        sampling_method: SamplingMethod,
        with_replacement: bool,
        seed: Option<u64>,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::Sample(Sample {
            input,
            sampling_method,
            with_replacement,
            seed,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn monotonically_increasing_id(
        input: LocalPhysicalPlanRef,
        column_name: String,
        starting_offset: Option<u64>,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
            input,
            column_name,
            starting_offset,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn hash_join(
        left: LocalPhysicalPlanRef,
        right: LocalPhysicalPlanRef,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        build_on_left: Option<bool>,
        null_equals_null: Option<Vec<bool>>,
        join_type: JoinType,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
            build_on_left,
            null_equals_null,
            join_type,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn cross_join(
        left: LocalPhysicalPlanRef,
        right: LocalPhysicalPlanRef,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::CrossJoin(CrossJoin {
            left,
            right,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sort_merge_join(
        left: LocalPhysicalPlanRef,
        right: LocalPhysicalPlanRef,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        join_type: JoinType,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::SortMergeJoin(SortMergeJoin {
            left,
            right,
            left_on,
            right_on,
            join_type,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub(crate) fn concat(
        input: LocalPhysicalPlanRef,
        other: LocalPhysicalPlanRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::Concat(Concat {
            input,
            other,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn physical_write(
        input: LocalPhysicalPlanRef,
        data_schema: SchemaRef,
        file_schema: SchemaRef,
        file_info: OutputFileInfo<BoundExpr>,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::PhysicalWrite(PhysicalWrite {
            input,
            data_schema,
            file_schema,
            file_info,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn commit_write(
        input: LocalPhysicalPlanRef,
        data_schema: SchemaRef,
        file_schema: SchemaRef,
        file_info: OutputFileInfo<BoundExpr>,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::CommitWrite(CommitWrite {
            input,
            data_schema,
            file_schema,
            file_info,
            stats_state,
            context,
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
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::CatalogWrite(CatalogWrite {
            input,
            catalog_type,
            data_schema,
            file_schema,
            stats_state,
            context,
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
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::LanceWrite(LanceWrite {
            input,
            lance_info,
            data_schema,
            file_schema,
            stats_state,
            context,
        })
        .arced()
    }

    #[cfg(feature = "python")]
    pub fn data_sink(
        input: LocalPhysicalPlanRef,
        data_sink_info: daft_logical_plan::DataSinkInfo,
        file_schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::DataSink(DataSink {
            input,
            data_sink_info,
            file_schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn repartition(
        input: LocalPhysicalPlanRef,
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::Repartition(Repartition {
            input,
            repartition_spec,
            num_partitions,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn into_partitions(
        input: LocalPhysicalPlanRef,
        num_partitions: usize,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::IntoPartitions(IntoPartitions {
            input,
            num_partitions,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn vllm_project(
        input: LocalPhysicalPlanRef,
        expr: BoundVLLMExpr,
        llm_actors: Option<RuntimePyObject>,
        output_column_name: Arc<str>,
        schema: SchemaRef,
        stats_state: StatsState,
        context: LocalNodeContext,
    ) -> LocalPhysicalPlanRef {
        Self::VLLMProject(VLLMProject {
            input,
            expr,
            llm_actors,
            output_column_name,
            schema,
            stats_state,
            context,
        })
        .arced()
    }

    pub fn schema(&self) -> &SchemaRef {
        match self {
            Self::PhysicalScan(PhysicalScan { schema, .. })
            | Self::GlobScan(GlobScan { schema, .. })
            | Self::PlaceholderScan(PlaceholderScan { schema, .. })
            | Self::EmptyScan(EmptyScan { schema, .. })
            | Self::Filter(Filter { schema, .. })
            | Self::IntoBatches(IntoBatches { schema, .. })
            | Self::Limit(Limit { schema, .. })
            | Self::Project(Project { schema, .. })
            | Self::UDFProject(UDFProject { schema, .. })
            | Self::UnGroupedAggregate(UnGroupedAggregate { schema, .. })
            | Self::HashAggregate(HashAggregate { schema, .. })
            | Self::Dedup(Dedup { schema, .. })
            | Self::Pivot(Pivot { schema, .. })
            | Self::Sort(Sort { schema, .. })
            | Self::TopN(TopN { schema, .. })
            | Self::Sample(Sample { schema, .. })
            | Self::HashJoin(HashJoin { schema, .. })
            | Self::CrossJoin(CrossJoin { schema, .. })
            | Self::SortMergeJoin(SortMergeJoin { schema, .. })
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
            Self::IntoPartitions(IntoPartitions { schema, .. }) => schema,
            Self::WindowPartitionOnly(WindowPartitionOnly { schema, .. }) => schema,
            Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy { schema, .. }) => schema,
            Self::VLLMProject(VLLMProject { schema, .. }) => schema,
        }
    }

    pub fn resource_request(self: &Arc<Self>) -> ResourceRequest {
        let mut base = ResourceRequest::default_cpu();
        self.apply(|plan| match plan.as_ref() {
            Self::UDFProject(UDFProject {
                expr,
                udf_properties,
                ..
            }) => {
                if let Some(resource_request) = &udf_properties.resource_request {
                    base = base.max(resource_request);
                }
                Ok(TreeNodeRecursion::Continue)
            }
            #[cfg(feature = "python")]
            Self::DistributedActorPoolProject(DistributedActorPoolProject {
                memory_request,
                ..
            }) => {
                base = base.max(
                    &ResourceRequest::default_cpu()
                        .with_memory_bytes(Some(*memory_request as usize))?,
                );
                Ok(TreeNodeRecursion::Continue)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        });
        base
    }

    fn children(&self) -> Vec<LocalPhysicalPlanRef> {
        match self {
            Self::PhysicalScan(_)
            | Self::GlobScan(_)
            | Self::PlaceholderScan(_)
            | Self::EmptyScan(_)
            | Self::InMemoryScan(_) => vec![],
            Self::Filter(Filter { input, .. })
            | Self::Limit(Limit { input, .. })
            | Self::IntoBatches(IntoBatches { input, .. })
            | Self::Project(Project { input, .. })
            | Self::UDFProject(UDFProject { input, .. })
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
            Self::SortMergeJoin(SortMergeJoin { left, right, .. }) => {
                vec![left.clone(), right.clone()]
            }
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
            Self::IntoPartitions(IntoPartitions { input, .. }) => vec![input.clone()],
            Self::TopN(TopN { input, .. }) => vec![input.clone()],
            Self::WindowOrderByOnly(WindowOrderByOnly { input, .. }) => vec![input.clone()],
            Self::VLLMProject(VLLMProject { input, .. }) => vec![input.clone()],
        }
    }

    pub fn with_new_children(&self, children: &[Arc<Self>]) -> Arc<Self> {
        match children {
            [new_child] => match self {
                Self::PhysicalScan(_)
                | Self::PlaceholderScan(_)
                | Self::EmptyScan(_)
                | Self::InMemoryScan(_) => panic!(
                    "LocalPhysicalPlan::with_new_children: PhysicalScan, PlaceholderScan, EmptyScan, and InMemoryScan do not have children"
                ),
                Self::Filter(Filter {
                    predicate, context, ..
                }) => Self::filter(
                    new_child.clone(),
                    predicate.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::IntoBatches(IntoBatches {
                    batch_size,
                    strict,
                    context,
                    ..
                }) => Self::into_batches(
                    new_child.clone(),
                    *batch_size,
                    *strict,
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::Limit(Limit {
                    limit,
                    offset,
                    context,
                    ..
                }) => Self::limit(
                    new_child.clone(),
                    *limit,
                    *offset,
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::Project(Project {
                    projection,
                    schema,
                    context,
                    ..
                }) => Self::project(
                    new_child.clone(),
                    projection.clone(),
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::UDFProject(UDFProject {
                    expr,
                    udf_properties,
                    passthrough_columns,
                    schema,
                    context,
                    ..
                }) => Self::udf_project(
                    new_child.clone(),
                    expr.clone(),
                    udf_properties.clone(),
                    passthrough_columns.clone(),
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::UnGroupedAggregate(UnGroupedAggregate {
                    aggregations,
                    schema,
                    context,
                    ..
                }) => Self::ungrouped_aggregate(
                    new_child.clone(),
                    aggregations.clone(),
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::HashAggregate(HashAggregate {
                    aggregations,
                    group_by,
                    schema,
                    context,
                    ..
                }) => Self::hash_aggregate(
                    new_child.clone(),
                    aggregations.clone(),
                    group_by.clone(),
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::Dedup(Dedup {
                    columns,
                    schema,
                    context,
                    ..
                }) => Self::dedup(
                    new_child.clone(),
                    columns.clone(),
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::Pivot(Pivot {
                    group_by,
                    pivot_column,
                    value_column,
                    aggregation,
                    pre_agg,
                    names,
                    schema,
                    context,
                    ..
                }) => Self::pivot(
                    new_child.clone(),
                    group_by.clone(),
                    pivot_column.clone(),
                    value_column.clone(),
                    aggregation.clone(),
                    names.clone(),
                    *pre_agg,
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::Sort(Sort {
                    sort_by,
                    descending,
                    nulls_first,
                    schema,
                    context,
                    ..
                }) => Self::sort(
                    new_child.clone(),
                    sort_by.clone(),
                    descending.clone(),
                    nulls_first.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::Sample(Sample {
                    sampling_method,
                    with_replacement,
                    seed,
                    schema,
                    context,
                    ..
                }) => Self::sample(
                    new_child.clone(),
                    *sampling_method,
                    *with_replacement,
                    *seed,
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::Explode(Explode {
                    to_explode,
                    schema,
                    context,
                    ..
                }) => Self::explode(
                    new_child.clone(),
                    to_explode.clone(),
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::Unpivot(Unpivot {
                    ids,
                    values,
                    variable_name,
                    value_name,
                    schema,
                    context,
                    ..
                }) => Self::unpivot(
                    new_child.clone(),
                    ids.clone(),
                    values.clone(),
                    variable_name.clone(),
                    value_name.clone(),
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::Concat(Concat {
                    other,
                    schema,
                    context,
                    ..
                }) => Self::concat(
                    new_child.clone(),
                    other.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
                    column_name,
                    starting_offset,
                    schema,
                    context,
                    ..
                }) => Self::monotonically_increasing_id(
                    new_child.clone(),
                    column_name.clone(),
                    *starting_offset,
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::WindowPartitionOnly(WindowPartitionOnly {
                    partition_by,
                    schema,
                    aggregations,
                    aliases,
                    context,
                    ..
                }) => Self::window_partition_only(
                    new_child.clone(),
                    partition_by.clone(),
                    schema.clone(),
                    StatsState::NotMaterialized,
                    aggregations.clone(),
                    aliases.clone(),
                    context.clone(),
                ),
                Self::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy {
                    partition_by,
                    order_by,
                    descending,
                    nulls_first,
                    schema,
                    functions,
                    aliases,
                    context,
                    ..
                }) => Self::window_partition_and_order_by(
                    new_child.clone(),
                    partition_by.clone(),
                    order_by.clone(),
                    descending.clone(),
                    nulls_first.clone(),
                    schema.clone(),
                    StatsState::NotMaterialized,
                    functions.clone(),
                    aliases.clone(),
                    context.clone(),
                ),
                Self::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {
                    partition_by,
                    order_by,
                    descending,
                    nulls_first,
                    frame,
                    min_periods,
                    schema,
                    aggregations,
                    aliases,
                    context,
                    ..
                }) => Self::window_partition_and_dynamic_frame(
                    new_child.clone(),
                    partition_by.clone(),
                    order_by.clone(),
                    descending.clone(),
                    nulls_first.clone(),
                    frame.clone(),
                    *min_periods,
                    schema.clone(),
                    StatsState::NotMaterialized,
                    aggregations.clone(),
                    aliases.clone(),
                    context.clone(),
                ),
                Self::WindowOrderByOnly(WindowOrderByOnly {
                    order_by,
                    descending,
                    nulls_first,
                    schema,
                    functions,
                    aliases,
                    context,
                    ..
                }) => Self::window_order_by_only(
                    new_child.clone(),
                    order_by.clone(),
                    descending.clone(),
                    nulls_first.clone(),
                    schema.clone(),
                    StatsState::NotMaterialized,
                    functions.clone(),
                    aliases.clone(),
                    context.clone(),
                ),
                Self::TopN(TopN {
                    sort_by,
                    descending,
                    nulls_first,
                    limit,
                    offset,
                    context,
                    ..
                }) => Self::top_n(
                    new_child.clone(),
                    sort_by.clone(),
                    descending.clone(),
                    nulls_first.clone(),
                    *limit,
                    *offset,
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::PhysicalWrite(PhysicalWrite {
                    data_schema,
                    file_schema,
                    file_info,
                    stats_state,
                    context,
                    ..
                }) => Self::physical_write(
                    new_child.clone(),
                    data_schema.clone(),
                    file_schema.clone(),
                    file_info.clone(),
                    stats_state.clone(),
                    context.clone(),
                ),
                Self::CommitWrite(CommitWrite {
                    input,
                    data_schema,
                    stats_state,
                    file_schema,
                    file_info,
                    context,
                    ..
                }) => Self::commit_write(
                    new_child.clone(),
                    data_schema.clone(),
                    file_schema.clone(),
                    file_info.clone(),
                    stats_state.clone(),
                    context.clone(),
                ),
                #[cfg(feature = "python")]
                Self::DataSink(DataSink {
                    input,
                    data_sink_info,
                    file_schema,
                    stats_state,
                    context,
                    ..
                }) => Self::data_sink(
                    new_child.clone(),
                    data_sink_info.clone(),
                    file_schema.clone(),
                    stats_state.clone(),
                    context.clone(),
                ),
                #[cfg(feature = "python")]
                Self::CatalogWrite(CatalogWrite {
                    catalog_type,
                    data_schema,
                    file_schema,
                    stats_state,
                    context,
                    ..
                }) => Self::catalog_write(
                    new_child.clone(),
                    catalog_type.clone(),
                    data_schema.clone(),
                    file_schema.clone(),
                    stats_state.clone(),
                    context.clone(),
                ),
                #[cfg(feature = "python")]
                Self::LanceWrite(LanceWrite {
                    lance_info,
                    data_schema,
                    file_schema,
                    stats_state,
                    context,
                    ..
                }) => Self::lance_write(
                    new_child.clone(),
                    lance_info.clone(),
                    data_schema.clone(),
                    file_schema.clone(),
                    stats_state.clone(),
                    context.clone(),
                ),
                #[cfg(feature = "python")]
                Self::DistributedActorPoolProject(DistributedActorPoolProject {
                    actor_objects,
                    schema,
                    batch_size,
                    memory_request,
                    context,
                    ..
                }) => Self::distributed_actor_pool_project(
                    new_child.clone(),
                    actor_objects.clone(),
                    *batch_size,
                    *memory_request,
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::Repartition(Repartition {
                    repartition_spec,
                    num_partitions,
                    schema,
                    context,
                    ..
                }) => Self::repartition(
                    new_child.clone(),
                    repartition_spec.clone(),
                    *num_partitions,
                    schema.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::IntoPartitions(IntoPartitions {
                    num_partitions,
                    context,
                    ..
                }) => Self::into_partitions(
                    new_child.clone(),
                    *num_partitions,
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
                Self::VLLMProject(VLLMProject {
                    input,
                    expr,
                    llm_actors,
                    output_column_name,
                    schema,
                    stats_state,
                    context,
                }) => Self::vllm_project(
                    new_child.clone(),
                    expr.clone(),
                    llm_actors.clone(),
                    output_column_name.clone(),
                    schema.clone(),
                    stats_state.clone(),
                    context.clone(),
                ),
                Self::HashJoin(_) => {
                    panic!("LocalPhysicalPlan::with_new_children: HashJoin should have 2 children")
                }
                Self::CrossJoin(_) => {
                    panic!("LocalPhysicalPlan::with_new_children: CrossJoin should have 2 children")
                }
                Self::SortMergeJoin(_) => {
                    panic!(
                        "LocalPhysicalPlan::with_new_children: SortMergeJoin should have 2 children"
                    )
                }
                Self::Concat(_) => {
                    panic!("LocalPhysicalPlan::with_new_children: Concat should have 2 children")
                }
                Self::GlobScan(_) => {
                    panic!("LocalPhysicalPlan::with_new_children: GlobScan does not have children")
                }
            },
            [new_left, new_right] => match self {
                Self::HashJoin(HashJoin {
                    left_on,
                    right_on,
                    null_equals_null,
                    join_type,
                    build_on_left,
                    schema,
                    stats_state,
                    context,
                    ..
                }) => Self::hash_join(
                    new_left.clone(),
                    new_right.clone(),
                    left_on.clone(),
                    right_on.clone(),
                    *build_on_left,
                    null_equals_null.clone(),
                    *join_type,
                    schema.clone(),
                    stats_state.clone(),
                    context.clone(),
                ),
                Self::CrossJoin(CrossJoin {
                    schema,
                    stats_state,
                    context,
                    ..
                }) => Self::cross_join(
                    new_left.clone(),
                    new_right.clone(),
                    schema.clone(),
                    stats_state.clone(),
                    context.clone(),
                ),
                Self::SortMergeJoin(SortMergeJoin {
                    left_on,
                    right_on,
                    join_type,
                    schema,
                    stats_state,
                    context,
                    ..
                }) => Self::sort_merge_join(
                    new_left.clone(),
                    new_right.clone(),
                    left_on.clone(),
                    right_on.clone(),
                    *join_type,
                    schema.clone(),
                    stats_state.clone(),
                    context.clone(),
                ),
                Self::Concat(Concat { context, .. }) => Self::concat(
                    new_left.clone(),
                    new_right.clone(),
                    StatsState::NotMaterialized,
                    context.clone(),
                ),
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

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct InMemoryScan {
    pub info: InMemoryInfo,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct PhysicalScan {
    pub scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    pub pushdowns: Pushdowns,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct GlobScan {
    pub glob_paths: Arc<Vec<String>>,
    pub pushdowns: Pushdowns,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub io_config: Option<common_io_config::IOConfig>,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct PlaceholderScan {
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct EmptyScan {
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Project {
    pub input: LocalPhysicalPlanRef,
    pub projection: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct UDFProject {
    pub input: LocalPhysicalPlanRef,
    pub expr: BoundExpr,
    pub udf_properties: UDFProperties,
    pub passthrough_columns: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[cfg(feature = "python")]
#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct DistributedActorPoolProject {
    pub input: LocalPhysicalPlanRef,
    pub actor_objects: Vec<PyObjectWrapper>,
    pub batch_size: Option<usize>,
    pub memory_request: u64,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Filter {
    pub input: LocalPhysicalPlanRef,
    pub predicate: BoundExpr,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct IntoBatches {
    pub input: LocalPhysicalPlanRef,
    pub batch_size: usize,
    pub strict: bool,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Limit {
    pub input: LocalPhysicalPlanRef,
    pub limit: u64,
    pub offset: Option<u64>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Explode {
    pub input: LocalPhysicalPlanRef,
    pub to_explode: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Sort {
    pub input: LocalPhysicalPlanRef,
    pub sort_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct TopN {
    pub input: LocalPhysicalPlanRef,
    pub sort_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub limit: u64,
    pub offset: Option<u64>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum SamplingMethod {
    Fraction(f64),
    Size(usize),
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Sample {
    pub input: LocalPhysicalPlanRef,
    pub sampling_method: SamplingMethod,
    pub with_replacement: bool,
    pub seed: Option<u64>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct MonotonicallyIncreasingId {
    pub input: LocalPhysicalPlanRef,
    pub column_name: String,
    pub starting_offset: Option<u64>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct UnGroupedAggregate {
    pub input: LocalPhysicalPlanRef,
    pub aggregations: Vec<BoundAggExpr>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct HashAggregate {
    pub input: LocalPhysicalPlanRef,
    pub aggregations: Vec<BoundAggExpr>,
    pub group_by: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Dedup {
    pub input: LocalPhysicalPlanRef,
    pub columns: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Unpivot {
    pub input: LocalPhysicalPlanRef,
    pub ids: Vec<BoundExpr>,
    pub values: Vec<BoundExpr>,
    pub variable_name: String,
    pub value_name: String,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Pivot {
    pub input: LocalPhysicalPlanRef,
    pub group_by: Vec<BoundExpr>,
    pub pivot_column: BoundExpr,
    pub value_column: BoundExpr,
    pub aggregation: BoundAggExpr,
    pub names: Vec<String>,
    pub pre_agg: bool,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct HashJoin {
    pub left: LocalPhysicalPlanRef,
    pub right: LocalPhysicalPlanRef,
    pub left_on: Vec<BoundExpr>,
    pub right_on: Vec<BoundExpr>,
    pub build_on_left: Option<bool>,
    pub null_equals_null: Option<Vec<bool>>,
    pub join_type: JoinType,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct CrossJoin {
    pub left: LocalPhysicalPlanRef,
    pub right: LocalPhysicalPlanRef,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct SortMergeJoin {
    pub left: LocalPhysicalPlanRef,
    pub right: LocalPhysicalPlanRef,
    pub left_on: Vec<BoundExpr>,
    pub right_on: Vec<BoundExpr>,
    pub join_type: JoinType,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Concat {
    pub input: LocalPhysicalPlanRef,
    pub other: LocalPhysicalPlanRef,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct PhysicalWrite {
    pub input: LocalPhysicalPlanRef,
    pub data_schema: SchemaRef,
    pub file_schema: SchemaRef,
    pub file_info: OutputFileInfo<BoundExpr>,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct CommitWrite {
    pub input: LocalPhysicalPlanRef,
    pub data_schema: SchemaRef,
    pub file_schema: SchemaRef,
    pub file_info: OutputFileInfo<BoundExpr>,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[cfg(feature = "python")]
#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct CatalogWrite {
    pub input: LocalPhysicalPlanRef,
    pub catalog_type: daft_logical_plan::CatalogType<BoundExpr>,
    pub data_schema: SchemaRef,
    pub file_schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[cfg(feature = "python")]
#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct LanceWrite {
    pub input: LocalPhysicalPlanRef,
    pub lance_info: daft_logical_plan::LanceCatalogInfo,
    pub data_schema: SchemaRef,
    pub file_schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[cfg(feature = "python")]
#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct DataSink {
    pub input: LocalPhysicalPlanRef,
    pub data_sink_info: daft_logical_plan::DataSinkInfo,
    pub file_schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct WindowPartitionOnly {
    pub input: LocalPhysicalPlanRef,
    pub partition_by: Vec<BoundExpr>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub aggregations: Vec<BoundAggExpr>,
    pub aliases: Vec<String>,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct WindowPartitionAndOrderBy {
    pub input: LocalPhysicalPlanRef,
    pub partition_by: Vec<BoundExpr>,
    pub order_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub functions: Vec<BoundWindowExpr>,
    pub aliases: Vec<String>,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct WindowPartitionAndDynamicFrame {
    pub input: LocalPhysicalPlanRef,
    pub partition_by: Vec<BoundExpr>,
    pub order_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub frame: WindowFrame,
    pub min_periods: usize,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub aggregations: Vec<BoundAggExpr>,
    pub aliases: Vec<String>,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct WindowOrderByOnly {
    pub input: LocalPhysicalPlanRef,
    pub order_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub functions: Vec<BoundWindowExpr>,
    pub aliases: Vec<String>,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Repartition {
    pub input: LocalPhysicalPlanRef,
    pub repartition_spec: RepartitionSpec,
    pub num_partitions: usize,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct IntoPartitions {
    pub input: LocalPhysicalPlanRef,
    pub num_partitions: usize,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct VLLMProject {
    pub input: LocalPhysicalPlanRef,
    pub expr: BoundVLLMExpr,
    pub llm_actors: Option<RuntimePyObject>,
    pub output_column_name: Arc<str>,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}
