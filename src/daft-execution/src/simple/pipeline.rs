use std::{cell::RefCell, collections::HashMap, sync::Arc};

use crate::simple::common::SourceResultType;

use super::{
    common::{IntermediateOperator, Sink, SinkResultType, Source},
    ops::{filter::FilterOperator, project::ProjectOperator},
    sinks::limit::LimitSink,
    sources::{in_memory::InMemoryScanSource, scan_task::ScanTaskSource},
};
use common_error::DaftResult;
use daft_dsl::common_treenode::TreeNode;
use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{Filter, InMemoryScan, Limit, Project, TabularScan},
    PhysicalPlan,
};

pub struct Pipeline {
    pub source: Option<Box<dyn Source>>,
    pub sink: Option<Box<dyn Sink>>,
    pub intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            source: None,
            sink: None,
            intermediate_operators: vec![],
        }
    }

    pub fn set_source(&mut self, source: Box<dyn Source>) {
        self.source = Some(source);
    }

    pub fn add_operator(&mut self, operator: Box<dyn IntermediateOperator>) {
        self.intermediate_operators.push(operator);
    }

    pub fn set_sink(&mut self, sink: Box<dyn Sink>) {
        self.sink = Some(sink);
    }

    pub fn execute(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let source = self.source.as_mut().unwrap();
        let sink = self.sink.as_mut().unwrap();
        loop {
            let source_result = source.get_data()?;
            match source_result {
                SourceResultType::HasMoreData(data) => {
                    let mut current_data = data;
                    for operator in &mut self.intermediate_operators {
                        current_data = operator.execute(&current_data)?;
                    }
                    let sink_result = sink.sink(&current_data)?;
                    match sink_result {
                        SinkResultType::NeedMoreInput => {
                            continue;
                        }
                        SinkResultType::Finished => {
                            break;
                        }
                    }
                }
                SourceResultType::Done => {
                    break;
                }
            }
        }
        sink.finalize()
    }
}

pub fn physical_plan_to_pipeline(
    plan: &PhysicalPlan,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Pipeline> {
    match plan {
        PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
            let scan_task_source = Box::new(ScanTaskSource::new(scan_tasks.clone()));

            let mut new_pipeline = Pipeline::new();
            new_pipeline.set_source(scan_task_source);
            Ok(new_pipeline)
        }
        PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
            let in_memory_scan_source = Box::new(InMemoryScanSource::new(
                psets.get(&in_memory_info.cache_key).unwrap().clone(),
            ));

            let mut new_pipeline = Pipeline::new();
            new_pipeline.set_source(in_memory_scan_source);
            Ok(new_pipeline)
        }
        PhysicalPlan::Filter(Filter { input, predicate }) => {
            let filter_operator = Box::new(FilterOperator::new(predicate.clone()));

            let mut current_pipeline = physical_plan_to_pipeline(input, psets)?;
            current_pipeline.add_operator(filter_operator);
            Ok(current_pipeline)
        }
        PhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let project_operator = Box::new(ProjectOperator::new(projection.clone()));

            let mut current_pipeline = physical_plan_to_pipeline(input, psets)?;
            current_pipeline.add_operator(project_operator);
            Ok(current_pipeline)
        }
        PhysicalPlan::Limit(Limit { input, limit, .. }) => {
            let limit_sink = Box::new(LimitSink::new(*limit as usize));

            let mut current_pipeline = physical_plan_to_pipeline(input, psets)?;
            current_pipeline.set_sink(limit_sink);
            Ok(current_pipeline)
        }
        _ => {
            todo!("Other physical plans not yet implemented");
        }
    }
}
