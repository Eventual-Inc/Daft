use std::{collections::HashMap, sync::Arc};

use daft_dsl::Expr;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{
    Filter, InMemoryScan, Limit, LocalPhysicalPlan, PhysicalScan, Project, UnGroupedAggregate,
};
use daft_plan::populate_aggregation_stages;

use crate::{
    intermediate_ops::{
        aggregate::AggregateOperator, filter::FilterOperator, project::ProjectOperator,
    },
    pipeline::Pipeline,
    sinks::{aggregate::AggregateSink, limit::LimitSink},
    sources::{in_memory::InMemorySource, scan_task::ScanTaskSource},
};

pub fn physical_plan_to_pipeline(
    physical_plan: &LocalPhysicalPlan,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> Pipeline {
    match physical_plan {
        LocalPhysicalPlan::PhysicalScan(PhysicalScan { scan_tasks, .. }) => {
            Pipeline::new(Box::new(ScanTaskSource::new(scan_tasks.clone())))
        }
        LocalPhysicalPlan::InMemoryScan(InMemoryScan { info, .. }) => {
            let partitions = psets.get(&info.cache_key).expect("Cache key not found");
            Pipeline::new(Box::new(InMemorySource::new(partitions.clone())))
        }
        LocalPhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let current_pipeline = physical_plan_to_pipeline(input, psets);
            let proj_op = ProjectOperator::new(projection.clone());
            current_pipeline.with_intermediate_operator(Box::new(proj_op))
        }
        LocalPhysicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let current_pipeline = physical_plan_to_pipeline(input, psets);
            let filter_op = FilterOperator::new(predicate.clone());
            current_pipeline.with_intermediate_operator(Box::new(filter_op))
        }
        LocalPhysicalPlan::Limit(Limit {
            input, num_rows, ..
        }) => {
            let current_pipeline = physical_plan_to_pipeline(input, psets);
            let sink = LimitSink::new(*num_rows as usize);
            let current_pipeline = current_pipeline.with_sink(Box::new(sink));

            Pipeline::new(Box::new(current_pipeline))
        }
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            ..
        }) => {
            let current_pipeline = physical_plan_to_pipeline(input, psets);
            let (first_stage_aggs, second_stage_aggs, final_exprs) =
                populate_aggregation_stages(aggregations, schema, &[]);

            let first_stage_agg_op = AggregateOperator::new(
                first_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                vec![],
            );
            let current_pipeline =
                current_pipeline.with_intermediate_operator(Box::new(first_stage_agg_op));

            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                vec![],
            );
            let current_pipeline = current_pipeline.with_sink(Box::new(second_stage_agg_sink));

            let final_stage_project = ProjectOperator::new(final_exprs);
            let new_pipeline = Pipeline::new(Box::new(current_pipeline));
            new_pipeline.with_intermediate_operator(Box::new(final_stage_project))
        }
        _ => {
            unimplemented!("Physical plan not supported: {}", physical_plan.name());
        }
    }
}
