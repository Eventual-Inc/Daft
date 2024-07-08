use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::{AggExpr, ExprRef};
use daft_plan::ResourceRequest;
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
    // #[allow(dead_code)]
    // FanoutByRange(FanoutByRange),
    // ReduceMerge(ReduceMerge),
    UnGroupedAggregate(UnGroupedAggregate),
    HashAggregate(HashAggregate),
    // Pivot(Pivot),
    // Concat(Concat),
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
    pub(crate) fn schema(&self) -> &SchemaRef {
        match self {
            LocalPhysicalPlan::PhysicalScan(PhysicalScan { schema, .. })
            | LocalPhysicalPlan::Filter(Filter { schema, .. })
            | LocalPhysicalPlan::Limit(Limit { schema, .. })
            | LocalPhysicalPlan::Project(Project { schema, .. })
            | LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate { schema, .. })
            | LocalPhysicalPlan::HashAggregate(HashAggregate { schema, .. }) => schema,
            _ => todo!("{:?}", self),
        }
    }
}

#[derive(Debug)]

pub struct InMemoryScan {}
#[derive(Debug)]

pub struct PhysicalScan {
    scan_tasks: Vec<ScanTaskRef>,
    schema: SchemaRef,
    plan_stats: PlanStats,
}
#[derive(Debug)]

pub struct Project {
    input: LocalPhysicalPlanRef,
    projection: Vec<ExprRef>,
    resource_request: ResourceRequest,
    schema: SchemaRef,
    plan_stats: PlanStats,
}
#[derive(Debug)]

pub struct Filter {
    input: LocalPhysicalPlanRef,
    predicate: ExprRef,
    schema: SchemaRef,
    plan_stats: PlanStats,
}
#[derive(Debug)]

pub struct Limit {
    input: LocalPhysicalPlanRef,
    num_rows: i64,
    schema: SchemaRef,
    plan_stats: PlanStats,
}
#[derive(Debug)]

pub struct Sort {}
#[derive(Debug)]

pub struct UnGroupedAggregate {
    input: LocalPhysicalPlanRef,
    aggregations: Vec<AggExpr>,
    schema: SchemaRef,
    plan_stats: PlanStats,
}
#[derive(Debug)]

pub struct HashAggregate {
    input: LocalPhysicalPlanRef,
    aggregations: Vec<AggExpr>,
    group_by: Vec<ExprRef>,
    schema: SchemaRef,
    plan_stats: PlanStats,
}
#[derive(Debug)]

pub struct HashJoin {}
#[derive(Debug)]

pub struct PhysicalWrite {}
#[derive(Debug)]

pub struct PlanStats {}
