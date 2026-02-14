use std::{collections::VecDeque, sync::Arc};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeInfo, NodeType};
use common_runtime::OrderingAwareJoinSet;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_micropartition::MicroPartition;
use itertools::Itertools;

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, create_channel},
    pipeline::{BuilderContext, MorselSizeRequirement, PipelineNode},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats},
};

pub(crate) struct RepartitionState {
    states: VecDeque<Vec<MicroPartition>>,
    current_size_bytes: usize,
}

impl RepartitionState {
    fn new(num_partitions: usize) -> Self {
        Self {
            states: (0..num_partitions).map(|_| vec![]).collect(),
            current_size_bytes: 0,
        }
    }

    fn push(&mut self, parts: Vec<MicroPartition>) {
        for (vec, part) in self.states.iter_mut().zip(parts) {
            self.current_size_bytes += part.size_bytes();
            vec.push(part);
        }
    }

    fn clear(&mut self) {
        for vec in &mut self.states {
            vec.clear();
        }
        self.current_size_bytes = 0;
    }

    fn is_empty(&self) -> bool {
        self.current_size_bytes == 0
    }
}

pub struct RepartitionNode {
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    schema: SchemaRef,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    shuffle_spill_threshold: Option<usize>,
}

impl RepartitionNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        schema: SchemaRef,
        child: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &BuilderContext,
        context: &LocalNodeContext,
        shuffle_spill_threshold: Option<usize>,
    ) -> Self {
        let name: Arc<str> = "Repartition".into();
        let node_info = ctx.next_node_info(
            name,
            NodeType::Repartition,
            NodeCategory::BlockingSink,
            context,
        );
        let runtime_stats = Arc::new(DefaultRuntimeStats::new(&ctx.meter, node_info.id));

        Self {
            repartition_spec,
            num_partitions,
            schema,
            child,
            runtime_stats,
            plan_stats,
            node_info: Arc::new(node_info),
            shuffle_spill_threshold,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    async fn flush_state(
        state: &mut RepartitionState,
        num_partitions: usize,
        schema: &SchemaRef,
    ) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let mut outputs = Vec::with_capacity(num_partitions);

        for i in 0..num_partitions {
            let data = std::mem::take(&mut state.states[i]);
            let schema = schema.clone();

            // We can run this concurrently if needed, but for now keep it simple
            let together = MicroPartition::concat(&data)?;
            let concated = together.concat_or_get()?;
            let mp = MicroPartition::new_loaded(
                schema,
                Arc::new(if let Some(t) = concated {
                    vec![t]
                } else {
                    vec![]
                }),
                None,
            );
            outputs.push(Arc::new(mp));
        }

        state.clear();
        Ok(outputs)
    }
}

impl TreeDisplay for RepartitionNode {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "Repartition").unwrap();
            }
            _ => {
                let multiline_display = match &self.repartition_spec {
                    RepartitionSpec::Hash(config) => vec![format!(
                        "Repartition: By {} into {} partitions",
                        config.by.iter().map(|e| e.to_string()).join(", "),
                        self.num_partitions
                    )],
                    RepartitionSpec::Random(_) => vec![format!(
                        "Repartition: Random into {} partitions",
                        self.num_partitions
                    )],
                    RepartitionSpec::IntoPartitions(config) => vec![format!(
                        "Repartition: Into {} partitions",
                        config.num_partitions
                    )],
                    RepartitionSpec::Range(config) => {
                        let pairs = config
                            .by
                            .iter()
                            .zip(config.descending.iter())
                            .map(|(sb, d)| {
                                format!("({}, {})", sb, if *d { "descending" } else { "ascending" })
                            })
                            .join(", ");
                        vec![
                            format!("Repartition: Range into {} partitions", self.num_partitions),
                            format!("By: {:?}", pairs),
                        ]
                    }
                };

                writeln!(display, "{}", multiline_display.join("\n")).unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
            }
        }
        display
    }

    fn repr_json(&self) -> serde_json::Value {
        let children: Vec<serde_json::Value> = self
            .get_children()
            .iter()
            .map(|child| child.repr_json())
            .collect();

        serde_json::json!({
            "id": self.node_id(),
            "category": "BlockingSink",
            "type": "Repartition",
            "name": "Repartition",
            "children": children,
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl PipelineNode for RepartitionNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        vec![&self.child]
    }

    fn name(&self) -> Arc<str> {
        self.node_info.name.clone()
    }

    fn propagate_morsel_size_requirement(
        &mut self,
        _downstream_requirement: MorselSizeRequirement,
        default_morsel_size: MorselSizeRequirement,
    ) {
        self.child
            .propagate_morsel_size_requirement(default_morsel_size, default_morsel_size);
    }

    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let mut child_results_receiver = self.child.start(false, runtime_handle)?;
        let (destination_sender, destination_receiver) = create_channel(1);

        let repartition_spec = self.repartition_spec.clone();
        let num_partitions = self.num_partitions;
        let schema = self.schema.clone();
        let shuffle_spill_threshold = self.shuffle_spill_threshold;
        let runtime_stats = self.runtime_stats.clone();
        let node_id = self.node_id();
        let stats_manager = runtime_handle.stats_manager();

        runtime_handle.spawn(
            async move {
                let mut state = RepartitionState::new(num_partitions);
                let mut task_set: OrderingAwareJoinSet<DaftResult<Vec<MicroPartition>>> =
                    OrderingAwareJoinSet::new(maintain_order);
                let mut node_initialized = false;

                // Helper: consume a completed task result, push into state, and spill if threshold is exceeded.
                macro_rules! consume_and_maybe_spill {
                    ($result:expr) => {{
                        let partitioned = $result??;
                        state.push(partitioned);

                        if let Some(threshold) = shuffle_spill_threshold
                            && state.current_size_bytes >= threshold
                        {
                            let outputs =
                                Self::flush_state(&mut state, num_partitions, &schema).await?;
                            for output in outputs {
                                runtime_stats.add_rows_out(output.len() as u64);
                                if destination_sender.send(output).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                    }};
                }

                while let Some(input) = child_results_receiver.recv().await {
                    if !node_initialized {
                        stats_manager.activate_node(node_id);
                        node_initialized = true;
                    }
                    runtime_stats.add_rows_in(input.len() as u64);

                    let repartition_spec = repartition_spec.clone();
                    let schema_clone = schema.clone();

                    task_set.spawn(async move {
                        let partitioned = match repartition_spec {
                            RepartitionSpec::Hash(config) => {
                                let bound_exprs = config
                                    .by
                                    .iter()
                                    .map(|e| BoundExpr::try_new(e.clone(), &schema_clone))
                                    .collect::<DaftResult<Vec<_>>>()?;
                                input.partition_by_hash(&bound_exprs, num_partitions)?
                            }
                            RepartitionSpec::Random(_) => {
                                input.partition_by_random(num_partitions, 0)?
                            }
                            RepartitionSpec::Range(config) => input.partition_by_range(
                                &config.by,
                                &config.boundaries,
                                &config.descending,
                            )?,
                            RepartitionSpec::IntoPartitions(_) => {
                                todo!("FLOTILLA_MS3: Support other types of repartition");
                            }
                        };
                        Ok(partitioned)
                    });

                    while let Some(result) = task_set.join_next().await {
                        consume_and_maybe_spill!(result);
                    }
                }

                // Drain any remaining tasks
                while let Some(result) = task_set.join_next().await {
                    consume_and_maybe_spill!(result);
                }

                // Final flush
                if !state.is_empty() {
                    let outputs = Self::flush_state(&mut state, num_partitions, &schema).await?;
                    for output in outputs {
                        runtime_stats.add_rows_out(output.len() as u64);
                        if destination_sender.send(output).await.is_err() {
                            return Ok(());
                        }
                    }
                }

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &self.name(),
        );

        Ok(destination_receiver)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
    fn node_id(&self) -> usize {
        self.node_info.id
    }
    fn plan_id(&self) -> Arc<str> {
        Arc::from(self.node_info.context.get("plan_id").unwrap().clone())
    }
    fn node_info(&self) -> Arc<NodeInfo> {
        self.node_info.clone()
    }
    fn runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        self.runtime_stats.clone()
    }
}
