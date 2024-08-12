use std::{collections::HashMap, sync::Arc};

use crate::{
    intermediate_ops::{
        aggregate::AggregateSpec, filter::FilterSpec, intermediate_op::IntermediateNode,
        project::ProjectSpec,
    },
    sinks::{
        aggregate::AggregateSink,
        blocking_sink::BlockingSinkNode,
        hash_join::{HashJoinNode, HashJoinOperator},
        limit::LimitSink,
        sort::SortSink,
        streaming_sink::StreamingSinkNode,
    },
    sources::in_memory::InMemorySource,
    ExecutionRuntimeHandle,
};

use async_trait::async_trait;
use common_error::DaftResult;
use daft_dsl::Expr;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{
    Distinct, Filter, HashAggregate, HashJoin, InMemoryScan, Limit, LocalPhysicalPlan, Project,
    Sort, UnGroupedAggregate,
};
use daft_plan::populate_aggregation_stages;

use crate::channel::MultiSender;

#[async_trait]
pub trait PipelineNode: Sync + Send {
    fn children(&self) -> Vec<&dyn PipelineNode>;
    async fn start(
        &mut self,
        destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<()>;
}

pub fn physical_plan_to_pipeline(
    physical_plan: &LocalPhysicalPlan,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn PipelineNode>> {
    use crate::sources::scan_task::ScanTaskSource;
    use daft_physical_plan::PhysicalScan;
    let out: Box<dyn PipelineNode> = match physical_plan {
        LocalPhysicalPlan::PhysicalScan(PhysicalScan { scan_tasks, .. }) => {
            let scan_task_sources = scan_tasks
                .iter()
                .map(|st| ScanTaskSource::new(st.clone()).boxed())
                .collect::<Vec<_>>();
            scan_task_sources.into()
        }
        LocalPhysicalPlan::InMemoryScan(InMemoryScan { info, .. }) => {
            let partitions = psets.get(&info.cache_key).expect("Cache key not found");
            let in_memory_source = InMemorySource::new(partitions.clone()).boxed();
            vec![in_memory_source].into()
        }
        LocalPhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let proj_spec = ProjectSpec::new(projection.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            IntermediateNode::new(Arc::new(proj_spec), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let filter_spec = FilterSpec::new(predicate.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            IntermediateNode::new(Arc::new(filter_spec), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Limit(Limit {
            input, num_rows, ..
        }) => {
            let sink = LimitSink::new(*num_rows as usize);
            let child_node = physical_plan_to_pipeline(input, psets)?;
            StreamingSinkNode::new(sink.boxed(), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Concat(_) => {
            todo!("concat")
            // let sink = ConcatSink::new();
            // let left_child = physical_plan_to_pipeline(input, psets)?;
            // let right_child = physical_plan_to_pipeline(other, psets)?;
            // PipelineNode::double_sink(sink, left_child, right_child)
        }
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            ..
        }) => {
            let (first_stage_aggs, second_stage_aggs, final_exprs) =
                populate_aggregation_stages(aggregations, schema, &[]);
            let child_node = physical_plan_to_pipeline(input, psets)?;
            let post_first_agg_node = if first_stage_aggs.is_empty() {
                child_node
            } else {
                let first_stage_agg_spec = AggregateSpec::new(
                    first_stage_aggs
                        .values()
                        .cloned()
                        .map(|e| Arc::new(Expr::Agg(e.clone())))
                        .collect(),
                    vec![],
                    input.schema().clone(),
                );
                IntermediateNode::new(Arc::new(first_stage_agg_spec), vec![child_node]).boxed()
            };

            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                vec![],
                schema.clone(),
            );
            let second_stage_node =
                BlockingSinkNode::new(second_stage_agg_sink.boxed(), post_first_agg_node).boxed();

            let final_stage_project_spec = ProjectSpec::new(final_exprs);

            IntermediateNode::new(Arc::new(final_stage_project_spec), vec![second_stage_node])
                .boxed()
        }
        LocalPhysicalPlan::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            schema,
            ..
        }) => {
            let (first_stage_aggs, second_stage_aggs, final_exprs) =
                populate_aggregation_stages(aggregations, schema, group_by);
            let child_node = physical_plan_to_pipeline(input, psets)?;
            let post_first_agg_node = if first_stage_aggs.is_empty() {
                child_node
            } else {
                let first_stage_agg_spec = AggregateSpec::new(
                    first_stage_aggs
                        .values()
                        .cloned()
                        .map(|e| Arc::new(Expr::Agg(e.clone())))
                        .collect(),
                    group_by.clone(),
                    input.schema().clone(),
                );
                IntermediateNode::new(Arc::new(first_stage_agg_spec), vec![child_node]).boxed()
            };

            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                group_by.clone(),
                schema.clone(),
            );
            let second_stage_node =
                BlockingSinkNode::new(second_stage_agg_sink.boxed(), post_first_agg_node).boxed();

            let final_stage_project_spec = ProjectSpec::new(final_exprs);

            IntermediateNode::new(Arc::new(final_stage_project_spec), vec![second_stage_node])
                .boxed()
        }
        LocalPhysicalPlan::Distinct(Distinct {
            input,
            group_by,
            schema,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets)?;
            let local_distinct_spec =
                AggregateSpec::new(vec![], group_by.clone(), input.schema().clone());
            let local_distinct_node =
                IntermediateNode::new(Arc::new(local_distinct_spec), vec![child_node]).boxed();
            let global_distinct_sink = AggregateSink::new(vec![], group_by.clone(), schema.clone());
            BlockingSinkNode::new(global_distinct_sink.boxed(), local_distinct_node).boxed()
        }
        LocalPhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            schema,
            ..
        }) => {
            let sort_sink = SortSink::new(sort_by.clone(), descending.clone(), schema.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            BlockingSinkNode::new(sort_sink.boxed(), child_node).boxed()
        }
        LocalPhysicalPlan::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
            join_type,
            ..
        }) => {
            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_node = physical_plan_to_pipeline(left, psets)?;
            let right_node = physical_plan_to_pipeline(right, psets)?;

            // we should move to a builder pattern
            let sink = HashJoinOperator::new(
                left_on.clone(),
                right_on.clone(),
                *join_type,
                left_schema,
                right_schema,
            )?;
            HashJoinNode::new(sink, left_node, right_node).boxed()
        }
        _ => {
            unimplemented!("Physical plan not supported: {}", physical_plan.name());
        }
    };

    Ok(out)
}
