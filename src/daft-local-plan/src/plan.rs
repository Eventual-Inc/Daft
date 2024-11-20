use std::sync::Arc;

use common_resource_request::ResourceRequest;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use daft_core::prelude::*;
use daft_dsl::{AggExpr, ExprRef};
use daft_logical_plan::{InMemoryInfo, OutputFileInfo};

pub type LocalPhysicalPlanRef = Arc<LocalPhysicalPlan>;
#[derive(Debug, strum::IntoStaticStr)]
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
    Pivot(Pivot),
    Concat(Concat),
    HashJoin(HashJoin),
    // SortMergeJoin(SortMergeJoin),
    // BroadcastJoin(BroadcastJoin),
    PhysicalWrite(PhysicalWrite),
    // TabularWriteJson(TabularWriteJson),
    // TabularWriteCsv(TabularWriteCsv),
    #[cfg(feature = "python")]
    CatalogWrite(CatalogWrite),
    #[cfg(feature = "python")]
    LanceWrite(LanceWrite),
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

    pub(crate) fn in_memory_scan(in_memory_info: InMemoryInfo) -> LocalPhysicalPlanRef {
        Self::InMemoryScan(InMemoryScan {
            info: in_memory_info,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn physical_scan(
        scan_tasks: Vec<ScanTaskLikeRef>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::PhysicalScan(PhysicalScan {
            scan_tasks,
            pushdowns,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn empty_scan(schema: SchemaRef) -> LocalPhysicalPlanRef {
        Self::EmptyScan(EmptyScan {
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn filter(input: LocalPhysicalPlanRef, predicate: ExprRef) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::Filter(Filter {
            input,
            predicate,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn limit(input: LocalPhysicalPlanRef, num_rows: i64) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::Limit(Limit {
            input,
            num_rows,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn explode(
        input: LocalPhysicalPlanRef,
        to_explode: Vec<ExprRef>,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::Explode(Explode {
            input,
            to_explode,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn project(
        input: LocalPhysicalPlanRef,
        projection: Vec<ExprRef>,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::Project(Project {
            input,
            projection,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn actor_pool_project(
        input: LocalPhysicalPlanRef,
        projection: Vec<ExprRef>,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::ActorPoolProject(ActorPoolProject {
            input,
            projection,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn ungrouped_aggregate(
        input: LocalPhysicalPlanRef,
        aggregations: Vec<ExprRef>,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn hash_aggregate(
        input: LocalPhysicalPlanRef,
        aggregations: Vec<ExprRef>,
        group_by: Vec<ExprRef>,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn unpivot(
        input: LocalPhysicalPlanRef,
        ids: Vec<ExprRef>,
        values: Vec<ExprRef>,
        variable_name: String,
        value_name: String,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::Unpivot(Unpivot {
            input,
            ids,
            values,
            variable_name,
            value_name,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn pivot(
        input: LocalPhysicalPlanRef,
        group_by: Vec<ExprRef>,
        pivot_column: ExprRef,
        value_column: ExprRef,
        aggregation: AggExpr,
        names: Vec<String>,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::Pivot(Pivot {
            input,
            group_by,
            pivot_column,
            value_column,
            aggregation,
            names,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn sort(
        input: LocalPhysicalPlanRef,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::Sort(Sort {
            input,
            sort_by,
            nulls_first,
            descending,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn sample(
        input: LocalPhysicalPlanRef,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::Sample(Sample {
            input,
            fraction,
            with_replacement,
            seed,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn monotonically_increasing_id(
        input: LocalPhysicalPlanRef,
        column_name: String,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
            input,
            column_name,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn hash_join(
        left: LocalPhysicalPlanRef,
        right: LocalPhysicalPlanRef,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        null_equals_null: Option<Vec<bool>>,
        join_type: JoinType,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
            null_equals_null,
            join_type,
            schema,
        })
        .arced()
    }

    pub(crate) fn concat(
        input: LocalPhysicalPlanRef,
        other: LocalPhysicalPlanRef,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        Self::Concat(Concat {
            input,
            other,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn physical_write(
        input: LocalPhysicalPlanRef,
        data_schema: SchemaRef,
        file_schema: SchemaRef,
        file_info: OutputFileInfo,
    ) -> LocalPhysicalPlanRef {
        Self::PhysicalWrite(PhysicalWrite {
            input,
            data_schema,
            file_schema,
            file_info,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    #[cfg(feature = "python")]
    pub(crate) fn catalog_write(
        input: LocalPhysicalPlanRef,
        catalog_type: daft_logical_plan::CatalogType,
        data_schema: SchemaRef,
        file_schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::CatalogWrite(CatalogWrite {
            input,
            catalog_type,
            data_schema,
            file_schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    #[cfg(feature = "python")]
    pub(crate) fn lance_write(
        input: LocalPhysicalPlanRef,
        lance_info: daft_logical_plan::LanceCatalogInfo,
        data_schema: SchemaRef,
        file_schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        Self::LanceWrite(LanceWrite {
            input,
            lance_info,
            data_schema,
            file_schema,
            plan_stats: PlanStats {},
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
            | Self::Pivot(Pivot { schema, .. })
            | Self::Sort(Sort { schema, .. })
            | Self::Sample(Sample { schema, .. })
            | Self::HashJoin(HashJoin { schema, .. })
            | Self::Explode(Explode { schema, .. })
            | Self::Unpivot(Unpivot { schema, .. })
            | Self::Concat(Concat { schema, .. }) => schema,
            Self::InMemoryScan(InMemoryScan { info, .. }) => &info.source_schema,
            _ => todo!("{:?}", self),
        }
    }
}

#[derive(Debug)]
pub struct InMemoryScan {
    pub info: InMemoryInfo,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct PhysicalScan {
    pub scan_tasks: Vec<ScanTaskLikeRef>,
    pub pushdowns: Pushdowns,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct EmptyScan {
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct Project {
    pub input: LocalPhysicalPlanRef,
    pub projection: Vec<ExprRef>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct ActorPoolProject {
    pub input: LocalPhysicalPlanRef,
    pub projection: Vec<ExprRef>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct Filter {
    pub input: LocalPhysicalPlanRef,
    pub predicate: ExprRef,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct Limit {
    pub input: LocalPhysicalPlanRef,
    pub num_rows: i64,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct Explode {
    pub input: LocalPhysicalPlanRef,
    pub to_explode: Vec<ExprRef>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct Sort {
    pub input: LocalPhysicalPlanRef,
    pub sort_by: Vec<ExprRef>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct Sample {
    pub input: LocalPhysicalPlanRef,
    pub fraction: f64,
    pub with_replacement: bool,
    pub seed: Option<u64>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct MonotonicallyIncreasingId {
    pub input: LocalPhysicalPlanRef,
    pub column_name: String,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct UnGroupedAggregate {
    pub input: LocalPhysicalPlanRef,
    pub aggregations: Vec<ExprRef>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct HashAggregate {
    pub input: LocalPhysicalPlanRef,
    pub aggregations: Vec<ExprRef>,
    pub group_by: Vec<ExprRef>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct Unpivot {
    pub input: LocalPhysicalPlanRef,
    pub ids: Vec<ExprRef>,
    pub values: Vec<ExprRef>,
    pub variable_name: String,
    pub value_name: String,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct Pivot {
    pub input: LocalPhysicalPlanRef,
    pub group_by: Vec<ExprRef>,
    pub pivot_column: ExprRef,
    pub value_column: ExprRef,
    pub aggregation: AggExpr,
    pub names: Vec<String>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct HashJoin {
    pub left: LocalPhysicalPlanRef,
    pub right: LocalPhysicalPlanRef,
    pub left_on: Vec<ExprRef>,
    pub right_on: Vec<ExprRef>,
    pub null_equals_null: Option<Vec<bool>>,
    pub join_type: JoinType,
    pub schema: SchemaRef,
}

#[derive(Debug)]
pub struct Concat {
    pub input: LocalPhysicalPlanRef,
    pub other: LocalPhysicalPlanRef,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct PhysicalWrite {
    pub input: LocalPhysicalPlanRef,
    pub data_schema: SchemaRef,
    pub file_schema: SchemaRef,
    pub file_info: OutputFileInfo,
    pub plan_stats: PlanStats,
}

#[cfg(feature = "python")]
#[derive(Debug)]
pub struct CatalogWrite {
    pub input: LocalPhysicalPlanRef,
    pub catalog_type: daft_logical_plan::CatalogType,
    pub data_schema: SchemaRef,
    pub file_schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[cfg(feature = "python")]
#[derive(Debug)]
pub struct LanceWrite {
    pub input: LocalPhysicalPlanRef,
    pub lance_info: daft_logical_plan::LanceCatalogInfo,
    pub data_schema: SchemaRef,
    pub file_schema: SchemaRef,
    pub plan_stats: PlanStats,
}

#[derive(Debug)]
pub struct PlanStats {}
