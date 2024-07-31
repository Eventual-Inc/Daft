use std::{collections::HashMap, os::macos::raw::stat, sync::Arc};

use crate::{
    channel::{create_channel, spawn_compute_task, MultiReceiver},
    intermediate_ops::{
        aggregate::AggregateOperator,
        filter::FilterOperator,
        intermediate_op::{IntermediateOpActor, IntermediateOperator},
        project::ProjectOperator,
    },
    sinks::{
        aggregate::AggregateSink,
        blocking_sink::{BlockingSink, BlockingSinkStatus},
        hash_join::HashJoinOperator,
        limit::LimitSink,
        sink::SinkActor,
        sort::SortSink,
        streaming_sink::{StreamSinkOutput, StreamingSink},
    },
    sources::{
        in_memory::InMemorySource,
        source::{Source, SourceActor},
    },
    NUM_CPUS,
};

use async_trait::async_trait;
use common_error::DaftResult;
use daft_dsl::Expr;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{
    Concat, Filter, HashAggregate, HashJoin, InMemoryScan, Limit, LocalPhysicalPlan, Project, Sort,
    UnGroupedAggregate,
};
use daft_plan::populate_aggregation_stages;
use futures::{
    future::{join_all, try_join_all},
    stream, StreamExt,
};
use tracing::{info_span, Instrument};

use crate::channel::MultiSender;

#[async_trait]
pub trait PipelineNode: Sync + Send {
    fn children(&self) -> Vec<&dyn PipelineNode>;
    async fn start(&mut self, destination: MultiSender) -> DaftResult<()>;
}

struct IntermediateNode {
    intermediate_op: Arc<dyn IntermediateOperator>,
    children: Vec<Box<dyn PipelineNode>>,
}

impl IntermediateNode {
    fn new(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
    ) -> Self {
        IntermediateNode {
            intermediate_op,
            children,
        }
    }
    fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

#[async_trait]
impl PipelineNode for IntermediateNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children.iter().map(|v| v.as_ref()).collect()
    }

    async fn start(&mut self, destination: MultiSender) -> DaftResult<()> {
        assert_eq!(
            self.children.len(),
            1,
            "we only support 1 child for Intermediate Node for now"
        );

        let (sender, receiver) = create_channel(*NUM_CPUS, destination.in_order());

        let child = self
            .children
            .get_mut(0)
            .expect("we should only have 1 child");
        child.start(sender).await?;

        // let child_futures = self.children.into_iter().map(|c| c.start(requires_order));
        // let mut child_receivers = try_join_all(child_futures).await?;
        // let receiver = child_receivers.pop().expect("we should only have 1 child here");

        let mut actor =
            IntermediateOpActor::new(self.intermediate_op.clone(), receiver, destination);
        // this should ideally be in the actor
        spawn_compute_task(async move { actor.run_parallel().await });
        Ok(())
    }
}

struct HashJoinNode {
    // use a RW lock
    hash_join: Arc<tokio::sync::Mutex<HashJoinOperator>>,
    left: Box<dyn PipelineNode>,
    right: Box<dyn PipelineNode>,
}

impl HashJoinNode {
    fn new(
        op: HashJoinOperator,
        left: Box<dyn PipelineNode>,
        right: Box<dyn PipelineNode>,
    ) -> Self {
        HashJoinNode {
            hash_join: Arc::new(tokio::sync::Mutex::new(op)),
            left,
            right,
        }
    }
    fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

#[async_trait]
impl PipelineNode for HashJoinNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }

    async fn start(&mut self, destination: MultiSender) -> DaftResult<()> {
        let (sender, mut pt_receiver) = create_channel(*NUM_CPUS, false);
        self.left.start(sender).await?;
        let hash_join = self.hash_join.clone();

        let probe_table_build = tokio::spawn(async move {
            let span = info_span!("ProbeTable::sink");
            let mut guard = hash_join.lock().await;
            let sink = guard.as_sink();
            while let Some(val) = pt_receiver.recv().await {
                if let BlockingSinkStatus::Finished = span.in_scope(|| sink.sink(&val?))? {
                    break;
                }
            }

            info_span!("ProbeTable::finalize").in_scope(|| sink.finalize())?;
            DaftResult::Ok(())
        });
        // should wrap in context join handle

        let (right_sender, mut streaming_receiver) =
            create_channel(*NUM_CPUS, destination.in_order());
        // now we can start building the right side
        self.right.start(right_sender).await?;

        probe_table_build.await.unwrap()?;

        let hash_join = self.hash_join.clone();
        let mut destination = destination;
        tokio::spawn(async move {
            let span = info_span!("ProbeTable::probe");

            // this should be a RWLock and run in concurrent workers
            let guard = hash_join.lock().await;
            let int_op = guard.as_intermediate_op();
            while let Some(val) = streaming_receiver.recv().await {
                let result = span.in_scope(|| int_op.execute(&val?));
                let sender = destination.get_next_sender();
                sender.send(result).await.unwrap();
            }
            DaftResult::Ok(())
        });

        Ok(())
    }
}

struct SourceNode {
    source_op: Arc<tokio::sync::Mutex<Box<dyn Source>>>,
}

#[async_trait]
impl PipelineNode for SourceNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![]
    }
    async fn start(&mut self, mut destination: MultiSender) -> DaftResult<()> {
        let op = self.source_op.clone();
        tokio::spawn(async move {
            let guard = op.lock().await;
            let mut source_stream = guard.get_data();
            while let Some(val) = source_stream.next().in_current_span().await {
                let _ = destination.get_next_sender().send(val).await;
            }
            DaftResult::Ok(())
        });
        Ok(())
    }
}

impl From<Box<dyn Source>> for Box<dyn PipelineNode> {
    fn from(value: Box<dyn Source>) -> Self {
        Box::new(SourceNode {
            source_op: Arc::new(tokio::sync::Mutex::new(value)),
        })
    }
}

struct StreamingSinkNode {
    // use a RW lock
    op: Arc<tokio::sync::Mutex<Box<dyn StreamingSink>>>,
    children: Vec<Box<dyn PipelineNode>>,
}

impl StreamingSinkNode {
    fn new(op: Box<dyn StreamingSink>, children: Vec<Box<dyn PipelineNode>>) -> Self {
        StreamingSinkNode {
            op: Arc::new(tokio::sync::Mutex::new(op)),
            children,
        }
    }
    fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

#[async_trait]
impl PipelineNode for StreamingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children.iter().map(|v| v.as_ref()).collect()
    }

    async fn start(&mut self, mut destination: MultiSender) -> DaftResult<()> {
        let (sender, mut streaming_receiver) = create_channel(*NUM_CPUS, destination.in_order());
        // now we can start building the right side
        let child = self
            .children
            .get_mut(0)
            .expect("we should only have 1 child");
        child.start(sender).await?;
        let op = self.op.clone();
        tokio::spawn(async move {
            // this should be a RWLock and run in concurrent workers
            let mut sink = op.lock().await;
            let mut is_active = true;
            while is_active && let Some(val) = streaming_receiver.recv().await {
                let val = val?;
                loop {
                    let result = sink.execute(0, &val)?;
                    match result {
                        StreamSinkOutput::HasMoreOutput(mp) => {
                            let sender = destination.get_next_sender();
                            sender.send(Ok(mp)).await.unwrap();
                        }
                        StreamSinkOutput::NeedMoreInput(mp) => {
                            if let Some(mp) = mp {
                                let sender = destination.get_next_sender();
                                sender.send(Ok(mp)).await.unwrap();
                            }
                            break;
                        }
                        StreamSinkOutput::Finished(mp) => {
                            if let Some(mp) = mp {
                                let sender = destination.get_next_sender();
                                sender.send(Ok(mp)).await.unwrap();
                            }
                            is_active = false;
                            break;
                        }
                    }
                }
            }
            DaftResult::Ok(())
        });
        Ok(())
    }
}

struct BlockingSinkNode {
    // use a RW lock
    op: Arc<tokio::sync::Mutex<Box<dyn BlockingSink>>>,
    child: Box<dyn PipelineNode>,
}

impl BlockingSinkNode {
    fn new(op: Box<dyn BlockingSink>, child: Box<dyn PipelineNode>) -> Self {
        BlockingSinkNode {
            op: Arc::new(tokio::sync::Mutex::new(op)),
            child,
        }
    }
    fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

#[async_trait]
impl PipelineNode for BlockingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    async fn start(&mut self, mut destination: MultiSender) -> DaftResult<()> {
        let (sender, mut streaming_receiver) = create_channel(*NUM_CPUS, true);
        // now we can start building the right side
        let child = self.child.as_mut();
        child.start(sender).await?;
        let op = self.op.clone();
        let sink_build = tokio::spawn(async move {
            let mut guard = op.lock().await;
            while let Some(val) = streaming_receiver.recv().await {
                if let BlockingSinkStatus::Finished = guard.sink(&val?)? {
                    break;
                }
            }
            guard.finalize()?;
            DaftResult::Ok(())
        });
        sink_build.await.unwrap()?;
        let op = self.op.clone();

        tokio::spawn(async move {
            let mut guard = op.lock().await;
            let source = guard.as_source();
            let mut source_stream = source.get_data();
            while let Some(val) = source_stream.next().in_current_span().await {
                let _ = destination.get_next_sender().send(val).await;
            }
            DaftResult::Ok(())
        });

        Ok(())
    }
}

pub fn physical_plan_to_pipeline(
    physical_plan: &LocalPhysicalPlan,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn PipelineNode>> {
    use crate::sources::scan_task::ScanTaskSource;
    use daft_physical_plan::PhysicalScan;
    let out: Box<dyn PipelineNode> = match physical_plan {
        LocalPhysicalPlan::PhysicalScan(PhysicalScan { scan_tasks, .. }) => {
            let scan_task_source = ScanTaskSource::new(scan_tasks.clone());
            scan_task_source.boxed().into()
        }
        LocalPhysicalPlan::InMemoryScan(InMemoryScan { info, .. }) => {
            let partitions = psets.get(&info.cache_key).expect("Cache key not found");
            InMemorySource::new(partitions.clone()).boxed().into()
        }
        LocalPhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let proj_op = ProjectOperator::new(projection.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            IntermediateNode::new(Arc::new(proj_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let filter_op = FilterOperator::new(predicate.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            IntermediateNode::new(Arc::new(filter_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Limit(Limit {
            input, num_rows, ..
        }) => {
            let sink = LimitSink::new(*num_rows as usize);
            let child_node = physical_plan_to_pipeline(input, psets)?;
            StreamingSinkNode::new(sink.boxed(), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Concat(Concat { input, other, .. }) => {
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
            todo!("agg")
            // let (first_stage_aggs, second_stage_aggs, final_exprs) =
            //     populate_aggregation_stages(aggregations, schema, &[]);
            // let first_stage_agg_op = AggregateOperator::new(
            //     first_stage_aggs
            //         .values()
            //         .cloned()
            //         .map(|e| Arc::new(Expr::Agg(e.clone())))
            //         .collect(),
            //     vec![],
            // );
            // let second_stage_agg_sink = AggregateSink::new(
            //     second_stage_aggs
            //         .values()
            //         .cloned()
            //         .map(|e| Arc::new(Expr::Agg(e.clone())))
            //         .collect(),
            //     vec![],
            // );
            // let final_stage_project = ProjectOperator::new(final_exprs);

            // let child_node = physical_plan_to_pipeline(input, psets)?;
            // let intermediate_agg_op_node = PipelineNode::IntermediateOp {
            //     intermediate_op: Arc::new(first_stage_agg_op),
            //     children: vec![child_node],
            // };

            // let sink_node =
            //     PipelineNode::single_sink(second_stage_agg_sink, intermediate_agg_op_node);

            // PipelineNode::IntermediateOp {
            //     intermediate_op: Arc::new(final_stage_project),
            //     children: vec![sink_node],
            // }
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

            let child_node = physical_plan_to_pipeline(input, psets)?;
            let post_first_agg_node =
                IntermediateNode::new(Arc::new(first_stage_agg_op), vec![child_node]).boxed();

            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                group_by.clone(),
            );
            let second_stage_node =
                BlockingSinkNode::new(second_stage_agg_sink.boxed(), post_first_agg_node).boxed();

            let final_stage_project = ProjectOperator::new(final_exprs);

            IntermediateNode::new(Arc::new(final_stage_project), vec![second_stage_node]).boxed()
        }
        LocalPhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            ..
        }) => {
            let sort_sink = SortSink::new(sort_by.clone(), descending.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            BlockingSinkNode::new(sort_sink.boxed(), child_node).boxed()
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
