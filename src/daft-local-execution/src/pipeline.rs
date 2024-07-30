use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_treenode::{ConcreteTreeNode, TreeNode};
use daft_core::schema::Schema;
use daft_dsl::Expr;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{
    Concat, Filter, HashAggregate, HashJoin, InMemoryScan, Limit, LocalPhysicalPlan, PhysicalScan,
    Project, Sort, UnGroupedAggregate,
};
use daft_plan::populate_aggregation_stages;

use crate::{
    channel::MultiSender,
    intermediate_ops::{
        aggregate::AggregateOperator,
        filter::FilterOperator,
        intermediate_op::{run_intermediate_op, IntermediateOperator},
        project::ProjectOperator,
    },
    sinks::{
        aggregate::AggregateSink,
        concat::ConcatSink,
        hash_join::HashJoinSink,
        limit::LimitSink,
        sink::{run_sink, Sink as ExecSink},
        sort::SortSink,
    },
    sources::{
        in_memory::InMemorySource,
        scan_task::ScanTaskSource,
        source::{run_source, Source},
    },
};

pub enum PipelineNode {
    Source {
        source: Box<dyn Source>,
    },
    IntermediateOp {
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<PipelineNode>,
    },
    Sink {
        sink: Box<dyn ExecSink>,
        children: Vec<PipelineNode>,
    },
}

impl PipelineNode {
    fn single_sink<S: ExecSink + 'static>(sink: S, child: Self) -> Self {
        PipelineNode::Sink {
            sink: Box::new(sink),
            children: vec![child],
        }
    }

    fn double_sink<S: ExecSink + 'static>(sink: S, left: Self, right: Self) -> Self {
        let children = vec![left, right];
        let after = PipelineNode::Sink {
            sink: Box::new(sink),
            children,
        };
        after
    }

    pub fn start(self, sender: MultiSender) {
        match self {
            PipelineNode::Source { source } => {
                run_source(source, sender);
            }
            PipelineNode::IntermediateOp {
                intermediate_op,
                mut children,
            } => {
                assert!(
                    children.len() == 1,
                    "we can only handle 1 child for intermediate ops right now: {}",
                    children.len()
                );
                let child = children.pop().expect("exactly 1 child");
                let sender = run_intermediate_op(intermediate_op, sender);
                child.start(sender);
            }
            PipelineNode::Sink { sink, children } => {
                let senders = run_sink(sink, sender);
                children
                    .into_iter()
                    .zip(senders.into_iter())
                    .for_each(|(child, s)| child.start(s));
            }
        }
    }
}

pub fn physical_plan_to_pipeline(
    physical_plan: &LocalPhysicalPlan,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<PipelineNode> {
    let out = match physical_plan {
        LocalPhysicalPlan::PhysicalScan(PhysicalScan { scan_tasks, .. }) => {
            let scan_task_source = ScanTaskSource::new(scan_tasks.clone());
            PipelineNode::Source {
                source: Box::new(scan_task_source),
            }
        }
        LocalPhysicalPlan::InMemoryScan(InMemoryScan { info, .. }) => {
            let partitions = psets.get(&info.cache_key).expect("Cache key not found");
            let in_memory_source = InMemorySource::new(partitions.clone());
            PipelineNode::Source {
                source: Box::new(in_memory_source),
            }
        }
        LocalPhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let proj_op = ProjectOperator::new(projection.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            PipelineNode::IntermediateOp {
                intermediate_op: Arc::new(proj_op),
                children: vec![child_node],
            }
        }
        LocalPhysicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let filter_op = FilterOperator::new(predicate.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            PipelineNode::IntermediateOp {
                intermediate_op: Arc::new(filter_op),
                children: vec![child_node],
            }
        }
        LocalPhysicalPlan::Limit(Limit {
            input, num_rows, ..
        }) => {
            let sink = LimitSink::new(*num_rows as usize);
            let child_node = physical_plan_to_pipeline(input, psets)?;
            PipelineNode::single_sink(sink, child_node)
        }
        LocalPhysicalPlan::Concat(Concat { input, other, .. }) => {
            let sink = ConcatSink::new();
            let left_child = physical_plan_to_pipeline(input, psets)?;
            let right_child = physical_plan_to_pipeline(other, psets)?;
            PipelineNode::double_sink(sink, left_child, right_child)
        }
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            ..
        }) => {
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
            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                vec![],
            );
            let final_stage_project = ProjectOperator::new(final_exprs);

            let child_node = physical_plan_to_pipeline(input, psets)?;
            let intermediate_agg_op_node = PipelineNode::IntermediateOp {
                intermediate_op: Arc::new(first_stage_agg_op),
                children: vec![child_node],
            };

            let sink_node =
                PipelineNode::single_sink(second_stage_agg_sink, intermediate_agg_op_node);

            PipelineNode::IntermediateOp {
                intermediate_op: Arc::new(final_stage_project),
                children: vec![sink_node],
            }
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
            let first_stage_agg_op = AggregateOperator::new(
                first_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                group_by.clone(),
            );
            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                group_by.clone(),
            );
            let final_stage_project = ProjectOperator::new(final_exprs);

            let child_node = physical_plan_to_pipeline(input, psets)?;
            let intermediate_agg_op_node = PipelineNode::IntermediateOp {
                intermediate_op: Arc::new(first_stage_agg_op),
                children: vec![child_node],
            };

            let sink_node =
                PipelineNode::single_sink(second_stage_agg_sink, intermediate_agg_op_node);

            PipelineNode::IntermediateOp {
                intermediate_op: Arc::new(final_stage_project),
                children: vec![sink_node],
            }
        }
        LocalPhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            ..
        }) => {
            let sort_sink = SortSink::new(sort_by.clone(), descending.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            PipelineNode::single_sink(sort_sink, child_node)
        }
        LocalPhysicalPlan::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
            join_type,
            schema,
        }) => {
            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_node = physical_plan_to_pipeline(left, psets)?;
            let right_node = physical_plan_to_pipeline(right, psets)?;
            let sink = HashJoinSink::new(
                left_on.clone(),
                right_on.clone(),
                *join_type,
                left_schema,
                right_schema,
            )?;
            PipelineNode::double_sink(sink, left_node, right_node)
        }
        _ => {
            unimplemented!("Physical plan not supported: {}", physical_plan.name());
        }
    };

    Ok(out)
}

impl ConcreteTreeNode for PipelineNode {
    fn children(&self) -> Vec<&Self> {
        use PipelineNode::*;
        match self {
            Source { .. } => vec![],
            IntermediateOp { children, .. } | Sink { children, .. } => children.iter().collect(),
        }
    }
    fn take_children(mut self) -> (Self, Vec<Self>) {
        use PipelineNode::*;
        match &mut self {
            Source { .. } => (self, vec![]),
            IntermediateOp { children, .. } | Sink { children, .. } => {
                let children = std::mem::take(children);
                (self, children)
            }
        }
    }
    fn with_new_children(mut self, mut new_children: Vec<Self>) -> DaftResult<Self> {
        use PipelineNode::*;
        match &mut self {
            Source { .. } => {
                assert_eq!(new_children.len(), 0);
                Ok(self)
            }
            IntermediateOp { children, .. } | Sink { children, .. } => {
                std::mem::swap(children, &mut new_children);
                Ok(self)
            }
        }
    }
}
