use std::{collections::HashMap, sync::Arc};

use daft_dsl::Expr;
use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{Aggregate, Filter, InMemoryScan, Limit, Project, TabularScan},
    PhysicalPlan,
};

use crate::{
    intermediate_ops::{filter::FilterOperator, project::ProjectOperator},
    pipeline::Pipeline,
    sinks::{aggregate::AggregateSink, limit::LimitSink},
    sources::{in_memory::InMemorySource, scan_task::ScanTaskSource},
};

pub fn physical_plan_to_pipeline(
    physical_plan: &Arc<PhysicalPlan>,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> Pipeline {
    match physical_plan.as_ref() {
        PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
            let partitions = psets
                .get(&in_memory_info.cache_key)
                .expect("Cache key not found");
            Pipeline::new(Box::new(InMemorySource::new(partitions.clone())))
        }
        PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
            Pipeline::new(Box::new(ScanTaskSource::new(scan_tasks.clone())))
        }
        PhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let current_pipeline = physical_plan_to_pipeline(input, psets);
            let proj_op = ProjectOperator::new(projection.clone());
            current_pipeline.with_intermediate_operator(Box::new(proj_op))
        }
        PhysicalPlan::Filter(Filter { input, predicate }) => {
            let current_pipeline = physical_plan_to_pipeline(input, psets);
            let filter_op = FilterOperator::new(predicate.clone());
            current_pipeline.with_intermediate_operator(Box::new(filter_op))
        }
        PhysicalPlan::Limit(Limit { limit, input, .. }) => {
            let current_pipeline = physical_plan_to_pipeline(input, psets);
            let sink = LimitSink::new(*limit as usize);
            let current_pipeline = current_pipeline.with_sink(Box::new(sink));

            Pipeline::new(Box::new(current_pipeline))
        }
        PhysicalPlan::Aggregate(Aggregate {
            input,
            aggregations,
            groupby,
        }) => {
            let current_pipeline = physical_plan_to_pipeline(input, psets);
            let sink = AggregateSink::new(
                aggregations
                    .iter()
                    .map(|agg| Arc::new(Expr::Agg(agg.clone())))
                    .collect::<Vec<_>>(),
                groupby.clone(),
            );
            let current_pipeline = current_pipeline.with_sink(Box::new(sink));

            Pipeline::new(Box::new(current_pipeline))
        }
        _ => {
            unimplemented!("Physical plan not supported: {}", physical_plan.name());
        }
    }
}
