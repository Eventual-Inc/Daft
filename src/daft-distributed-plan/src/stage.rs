use std::{
    cmp::min,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_scan_info::{PartitionField, PhysicalScanInfo, Pushdowns, ScanState, ScanTaskLikeRef};
use daft_local_plan::{translate, LocalPhysicalPlanRef};
use daft_logical_plan::{
    ops::{Limit, Source},
    optimization::OptimizerBuilder,
    LogicalPlan, LogicalPlanRef, SourceInfo,
};
use daft_schema::schema::SchemaRef;

use crate::planner::replace_placeholders_with_sources;

const DEFAULT_PLAN_SIZE: usize = 1024 * 1024 * 1024 * 16; // 16GB

pub trait SwordfishStage: Send + Sync + Debug {
    fn update(&mut self, num_rows: usize) -> DaftResult<()>;
    fn next_plan(&mut self) -> DaftResult<Option<LocalPhysicalPlanRef>>;
    fn is_done(&self) -> bool;
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
        println!("scan tasks: {:#?}", scan_tasks);
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

impl SwordfishStage for CollectStage {
    fn update(&mut self, _num_rows: usize) -> DaftResult<()> {
        // CollectStage doesn't need to update anything based on rows processed
        Ok(())
    }

    fn next_plan(&mut self) -> DaftResult<Option<LocalPhysicalPlanRef>> {
        println!("collect stage next plan");
        if self.is_done() {
            println!("collect stage is done");
            return Ok(None);
        }
        println!("collect stage is not done");

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
        println!("collect stage next plan: {:#?}", local_physical_plan);
        Ok(Some(local_physical_plan))
    }

    fn is_done(&self) -> bool {
        println!(
            "collect stage is done: {}, {}",
            self.next_scan_task_idx,
            self.scan_tasks.len()
        );
        self.next_scan_task_idx >= self.scan_tasks.len()
    }
}

#[derive(Debug)]
pub struct LimitStage {
    scan_info_provider: ScanInfoProvider,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    next_scan_task_idx: usize,
    logical_plan: LogicalPlanRef,
    remaining_limit: usize,
    config: Arc<DaftExecutionConfig>,
    output_schema: SchemaRef,
}

impl LimitStage {
    pub fn new(
        source: PhysicalScanInfo,
        output_schema: SchemaRef,
        logical_plan: LogicalPlanRef,
        limit: usize,
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
            remaining_limit: limit,
            config,
            output_schema,
        }
    }

    fn make_plan(
        &self,
        logical_plan: LogicalPlanRef,
        new_limit: usize,
    ) -> DaftResult<LocalPhysicalPlanRef> {
        let plan_with_new_limit = match logical_plan.as_ref() {
            LogicalPlan::Limit(limit) => {
                let new_limit = Limit::new(
                    limit.input.clone(),
                    new_limit.try_into().unwrap(),
                    limit.eager,
                );
                LogicalPlan::Limit(new_limit)
            }
            _ => panic!("Unsupported logical plan"),
        };
        let optimizer = OptimizerBuilder::new().enrich_with_stats().build();
        let optimized_logical_plan =
            optimizer.optimize(plan_with_new_limit.into(), |_, _, _, _, _| {})?;
        let local_physical_plan = translate(&optimized_logical_plan)?;
        Ok(local_physical_plan)
    }
}

impl SwordfishStage for LimitStage {
    fn update(&mut self, num_rows: usize) -> DaftResult<()> {
        // Update the remaining limit based on the number of rows processed
        if self.remaining_limit <= num_rows {
            self.remaining_limit = 0;
        } else {
            self.remaining_limit -= num_rows;
        }

        Ok(())
    }

    fn next_plan(&mut self) -> DaftResult<Option<LocalPhysicalPlanRef>> {
        if self.remaining_limit == 0 {
            return Ok(None);
        }

        let mut scan_tasks = Vec::new();
        let mut estimated_rows = 0;
        let mut memory_cost = 0;

        while memory_cost < DEFAULT_PLAN_SIZE
            && self.next_scan_task_idx < self.scan_tasks.len()
            && estimated_rows < self.remaining_limit
        {
            let scan_task = self.scan_tasks[self.next_scan_task_idx].clone();
            estimated_rows += scan_task.approx_num_rows(Some(&self.config)).unwrap_or(0.0) as usize;
            memory_cost += scan_task
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
        let new_limit = min(self.remaining_limit, estimated_rows);
        let local_physical_plan = self.make_plan(plan, new_limit)?;
        Ok(Some(local_physical_plan))
    }

    fn is_done(&self) -> bool {
        if self.remaining_limit == 0 {
            return false;
        }

        self.next_scan_task_idx >= self.scan_tasks.len()
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
