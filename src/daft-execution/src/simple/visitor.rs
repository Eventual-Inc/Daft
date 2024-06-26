use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_dsl::common_treenode::{TreeNodeRecursion, TreeNodeVisitor};
use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{Aggregate, Coalesce, Filter, InMemoryScan, Limit, Project, TabularScan},
    PhysicalPlan,
};

use super::{
    common::SourceType,
    intermediate_op::IntermediateOperatorType,
    pipeline::Pipeline,
    sinks::{coalesce::CoalesceSink, limit::LimitSink},
};

pub struct PhysicalToPipelineVisitor {
    pub pipelines: Vec<Pipeline>,
    pub psets: HashMap<String, Vec<Arc<MicroPartition>>>,
}

impl TreeNodeVisitor for PhysicalToPipelineVisitor {
    type Node = Arc<PhysicalPlan>;

    fn f_down(&mut self, _node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        match node.as_ref() {
            PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
                let mut new_pipeline = Pipeline::new();
                new_pipeline.set_sources(
                    scan_tasks
                        .iter()
                        .map(|task| SourceType::ScanTask(task.clone()))
                        .collect(),
                );
                self.pipelines.push(new_pipeline);
                Ok(TreeNodeRecursion::Continue)
            }
            PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
                let mut partitions = self.psets.get(&in_memory_info.cache_key).unwrap();
                let mut new_pipeline = Pipeline::new();
                new_pipeline.set_sources(
                    partitions
                        .iter()
                        .map(|partition| SourceType::InMemory(partition.clone()))
                        .collect(),
                );
                self.pipelines.push(new_pipeline);
                Ok(TreeNodeRecursion::Continue)
            }
            PhysicalPlan::Filter(Filter { input, predicate }) => {
                let mut current_pipeline = self.pipelines.last_mut().unwrap();
                current_pipeline.add_operator(IntermediateOperatorType::Filter {
                    predicate: predicate.clone(),
                });
                Ok(TreeNodeRecursion::Continue)
            }
            PhysicalPlan::Project(Project {
                input, projection, ..
            }) => {
                let mut current_pipeline = self.pipelines.last_mut().unwrap();
                current_pipeline.add_operator(IntermediateOperatorType::Project {
                    projection: projection.clone(),
                });
                Ok(TreeNodeRecursion::Continue)
            }
            PhysicalPlan::Limit(Limit { input, limit, .. }) => {
                let limit_sink = Box::new(LimitSink::new(*limit as usize));

                let mut current_pipeline = self.pipelines.last_mut().unwrap();
                current_pipeline.set_sink(limit_sink);
                Ok(TreeNodeRecursion::Continue)
            }
            PhysicalPlan::Aggregate(Aggregate {
                input,
                aggregations,
                groupby,
            }) => {
                let mut current_pipeline = self.pipelines.last_mut().unwrap();
                let agg_op = IntermediateOperatorType::Aggregate {
                    aggregations: aggregations.clone(),
                    groupby: groupby.clone(),
                };
                // add this logic to all other intermediate operators, probably abstract it into the `add_operator` method
                if current_pipeline.sink.is_some() {
                    let mut new_pipeline = Pipeline::new();
                    new_pipeline.add_operator(agg_op);
                    self.pipelines.push(new_pipeline);
                } else {
                    current_pipeline.add_operator(agg_op);
                }

                Ok(TreeNodeRecursion::Continue)
            }
            PhysicalPlan::Coalesce(Coalesce {
                input,
                num_from,
                num_to,
            }) => {
                let coalesce_sink = Box::new(CoalesceSink::new(*num_from, *num_to));

                let mut current_pipeline = self.pipelines.last_mut().unwrap();
                current_pipeline.set_sink(coalesce_sink);
                Ok(TreeNodeRecursion::Continue)
            }
            _ => {
                todo!("Other physical plans not yet implemented");
            }
        }
    }
}
