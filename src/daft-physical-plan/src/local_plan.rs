use std::sync::Arc;

use daft_core::{schema::SchemaRef, JoinType};
use daft_dsl::{AggExpr, ExprRef};
use daft_plan::{InMemoryInfo, ResourceRequest};
use daft_scan::{ScanTask, ScanTaskRef};

pub type LocalPhysicalPlanRef = Arc<LocalPhysicalPlan>;
#[derive(Debug, strum::IntoStaticStr)]
pub enum LocalPhysicalPlan {
    InMemoryScan(InMemoryScan),
    PhysicalScan(PhysicalScan),
    // EmptyScan(EmptyScan),
    Project(Project),
    Filter(Filter),
    Limit(Limit),
    // Explode(Explode),
    // Unpivot(Unpivot),
    Sort(Sort),
    // Split(Split),
    // Sample(Sample),
    // MonotonicallyIncreasingId(MonotonicallyIncreasingId),
    // Coalesce(Coalesce),
    // Flatten(Flatten),
    // FanoutRandom(FanoutRandom),
    // FanoutByHash(FanoutByHash),
    // FanoutByRange(FanoutByRange),
    // ReduceMerge(ReduceMerge),
    UnGroupedAggregate(UnGroupedAggregate),
    HashAggregate(HashAggregate),
    // Pivot(Pivot),
    Concat(Concat),
    HashJoin(HashJoin),
    // SortMergeJoin(SortMergeJoin),
    // BroadcastJoin(BroadcastJoin),
    PhysicalWrite(PhysicalWrite),
    // TabularWriteJson(TabularWriteJson),
    // TabularWriteCsv(TabularWriteCsv),
    // #[cfg(feature = "python")]
    // IcebergWrite(IcebergWrite),
    // #[cfg(feature = "python")]
    // DeltaLakeWrite(DeltaLakeWrite),
    // #[cfg(feature = "python")]
    // LanceWrite(LanceWrite),
}

impl LocalPhysicalPlan {
    pub fn name(&self) -> &'static str {
        // uses strum::IntoStaticStr
        self.into()
    }

    pub fn arced(self) -> LocalPhysicalPlanRef {
        self.into()
    }

    pub(crate) fn in_memory_scan(in_memory_info: InMemoryInfo) -> LocalPhysicalPlanRef {
        LocalPhysicalPlan::InMemoryScan(InMemoryScan {
            info: in_memory_info,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn physical_scan(
        scan_tasks: Vec<ScanTaskRef>,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        LocalPhysicalPlan::PhysicalScan(PhysicalScan {
            scan_tasks,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn filter(input: LocalPhysicalPlanRef, predicate: ExprRef) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        LocalPhysicalPlan::Filter(Filter {
            input,
            predicate,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn limit(input: LocalPhysicalPlanRef, num_rows: i64) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        LocalPhysicalPlan::Limit(Limit {
            input,
            num_rows,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn project(
        input: LocalPhysicalPlanRef,
        projection: Vec<ExprRef>,
        resource_request: ResourceRequest,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        LocalPhysicalPlan::Project(Project {
            input,
            projection,
            resource_request,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn ungrouped_aggregate(
        input: LocalPhysicalPlanRef,
        aggregations: Vec<AggExpr>,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn hash_aggregate(
        input: LocalPhysicalPlanRef,
        aggregations: Vec<AggExpr>,
        group_by: Vec<ExprRef>,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        LocalPhysicalPlan::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn sort(
        input: LocalPhysicalPlanRef,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
    ) -> LocalPhysicalPlanRef {
        let schema = input.schema().clone();
        LocalPhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
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
        join_type: JoinType,
        schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        LocalPhysicalPlan::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
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
        LocalPhysicalPlan::Concat(Concat {
            input,
            other,
            schema,
            plan_stats: PlanStats {},
        })
        .arced()
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        match self {
            LocalPhysicalPlan::PhysicalScan(PhysicalScan { schema, .. })
            | LocalPhysicalPlan::Filter(Filter { schema, .. })
            | LocalPhysicalPlan::Limit(Limit { schema, .. })
            | LocalPhysicalPlan::Project(Project { schema, .. })
            | LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate { schema, .. })
            | LocalPhysicalPlan::HashAggregate(HashAggregate { schema, .. })
            | LocalPhysicalPlan::Sort(Sort { schema, .. })
            | LocalPhysicalPlan::HashJoin(HashJoin { schema, .. })
            | LocalPhysicalPlan::Concat(Concat { schema, .. }) => schema,
            LocalPhysicalPlan::InMemoryScan(InMemoryScan { info, .. }) => &info.source_schema,
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
    pub scan_tasks: Vec<ScanTaskRef>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}
#[derive(Debug)]

pub struct Project {
    pub input: LocalPhysicalPlanRef,
    pub projection: Vec<ExprRef>,
    pub resource_request: ResourceRequest,
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

pub struct Sort {
    pub input: LocalPhysicalPlanRef,
    pub sort_by: Vec<ExprRef>,
    pub descending: Vec<bool>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}
#[derive(Debug)]

pub struct UnGroupedAggregate {
    pub input: LocalPhysicalPlanRef,
    pub aggregations: Vec<AggExpr>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}
#[derive(Debug)]

pub struct HashAggregate {
    pub input: LocalPhysicalPlanRef,
    pub aggregations: Vec<AggExpr>,
    pub group_by: Vec<ExprRef>,
    pub schema: SchemaRef,
    pub plan_stats: PlanStats,
}
#[derive(Debug)]

pub struct HashJoin {
    pub left: LocalPhysicalPlanRef,
    pub right: LocalPhysicalPlanRef,
    pub left_on: Vec<ExprRef>,
    pub right_on: Vec<ExprRef>,
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

pub struct PhysicalWrite {}
#[derive(Debug)]

pub struct PlanStats {}
