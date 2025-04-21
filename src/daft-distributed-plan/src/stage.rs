use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_scan_info::{PartitionField, PhysicalScanInfo, Pushdowns, ScanState, ScanTaskLikeRef};
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{translate, LocalPhysicalPlanRef};
use daft_logical_plan::{
    ops::Source, optimization::OptimizerBuilder, LogicalPlan, LogicalPlanRef, SourceInfo,
};
use daft_schema::schema::SchemaRef;

const DEFAULT_PLAN_SIZE: usize = 1024 * 1024 * 1024 * 180; // 16GB

pub enum Stage {
    Collect(CollectStage),
}

#[derive(Debug)]
pub struct CollectStage {
    scan_info_provider: ScanInfoProvider,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    next_scan_task_idx: usize,
    logical_plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    output_schema: SchemaRef,
}

impl CollectStage {
    pub fn new(
        source: PhysicalScanInfo,
        output_schema: SchemaRef,
        logical_plan: LogicalPlanRef,
        config: Arc<DaftExecutionConfig>,
    ) -> Self {
        let scan_info_provider = ScanInfoProvider::new(&source);
        let scan_tasks = match source.scan_state {
            ScanState::Operator(_) => panic!("Operator scan state not supported"),
            ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
        };
        Self {
            scan_info_provider,
            scan_tasks,
            next_scan_task_idx: 0,
            logical_plan,
            config,
            output_schema,
        }
    }
}

impl CollectStage {
    pub fn next_plan(&mut self) -> DaftResult<Option<LocalPhysicalPlanRef>> {
        if self.next_scan_task_idx >= self.scan_tasks.len() {
            return Ok(None);
        }

        let mut scan_tasks = Vec::new();
        let mut scan_task_total_memory_cost = 0;

        while scan_task_total_memory_cost < DEFAULT_PLAN_SIZE
            && self.next_scan_task_idx < self.scan_tasks.len()
        {
            let scan_task = self.scan_tasks[self.next_scan_task_idx].clone();
            scan_task_total_memory_cost += scan_task
                .estimate_in_memory_size_bytes(Some(&self.config))
                .unwrap_or(0);
            scan_tasks.push(scan_task);
            self.next_scan_task_idx += 1;
        }

        let scan_info = self.scan_info_provider.make_scan_info(scan_tasks);
        let source_plan = LogicalPlan::Source(Source::new(
            self.output_schema.clone(),
            SourceInfo::Physical(scan_info).into(),
        ));

        let plan =
            replace_placeholders_with_sources(self.logical_plan.clone(), source_plan.into())?;
        let optimizer = OptimizerBuilder::new().enrich_with_stats().build();
        let optimized_logical_plan = optimizer.optimize(plan.into(), |_, _, _, _, _| {})?;
        let local_physical_plan = translate(&optimized_logical_plan)?;
        Ok(Some(local_physical_plan))
    }
}

#[derive(Debug)]
struct ScanInfoProvider {
    source_schema: SchemaRef,
    partitioning_keys: Vec<PartitionField>,
    pushdowns: Pushdowns,
}

impl ScanInfoProvider {
    pub fn new(source: &PhysicalScanInfo) -> Self {
        Self {
            source_schema: source.source_schema.clone(),
            partitioning_keys: source.partitioning_keys.clone(),
            pushdowns: source.pushdowns.clone(),
        }
    }

    pub fn make_scan_info(&self, scan_tasks: Vec<ScanTaskLikeRef>) -> PhysicalScanInfo {
        PhysicalScanInfo {
            source_schema: self.source_schema.clone(),
            partitioning_keys: self.partitioning_keys.clone(),
            pushdowns: self.pushdowns.clone(),
            scan_state: ScanState::Tasks(scan_tasks.into()),
        }
    }
}

pub fn replace_placeholders_with_sources(
    plan: LogicalPlanRef,
    new_source_plan: LogicalPlanRef,
) -> DaftResult<LogicalPlanRef> {
    let new_plan = plan.transform_up(|plan| match plan.as_ref() {
        LogicalPlan::Source(source) => match source.source_info.as_ref() {
            SourceInfo::PlaceHolder(_ph) => Ok(Transformed::yes(new_source_plan.clone())),
            _ => Ok(Transformed::no(plan)),
        },
        _ => Ok(Transformed::no(plan)),
    })?;
    Ok(new_plan.data)
}
