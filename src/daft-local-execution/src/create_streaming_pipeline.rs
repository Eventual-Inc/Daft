use std::{collections::HashMap, sync::Arc};

use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{Filter, InMemoryScan, Limit, Project, TabularScan},
    PhysicalPlan,
};

use crate::{
    intermediate_ops::{filter::FilterOperator, project::ProjectOperator},
    sinks::limit::LimitSink,
    sources::{in_memory::InMemorySource, scan_task::ScanTaskSource},
    streaming_pipeline::StreamingPipeline,
};

pub fn physical_plan_to_streaming_pipeline(
    physical_plan: &Arc<PhysicalPlan>,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> StreamingPipeline {
    match physical_plan.as_ref() {
        PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
            let partitions = psets.get(&in_memory_info.cache_key).unwrap();
            let new_pipeline =
                StreamingPipeline::new(Box::new(InMemorySource::new(partitions.clone())));
            new_pipeline
        }
        PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
            let new_pipeline =
                StreamingPipeline::new(Box::new(ScanTaskSource::new(scan_tasks.clone())));
            new_pipeline
        }
        PhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let current_pipeline = physical_plan_to_streaming_pipeline(input, psets);
            let proj_op = ProjectOperator::new(projection.clone());
            current_pipeline.with_intermediate_operator(Box::new(proj_op))
        }
        PhysicalPlan::Filter(Filter { input, predicate }) => {
            let current_pipeline = physical_plan_to_streaming_pipeline(input, psets);
            let filter_op = FilterOperator::new(predicate.clone());
            current_pipeline.with_intermediate_operator(Box::new(filter_op))
        }
        PhysicalPlan::Limit(Limit { limit, input, .. }) => {
            let current_pipeline = physical_plan_to_streaming_pipeline(input, psets);
            let sink = LimitSink::new(*limit as usize);
            let current_pipeline = current_pipeline.with_sink(Box::new(sink));

            let new_pipeline = StreamingPipeline::new(Box::new(current_pipeline));
            new_pipeline
        }
        _ => {
            unimplemented!(
                "Physical plan not supported: {:?}",
                physical_plan.to_string()
            )
        }
    }
}
