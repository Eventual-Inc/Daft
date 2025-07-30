use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::{DaftError, DaftResult};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::HashClusteringConfig;
use daft_schema::schema::SchemaRef;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;

use super::{DistributedPipelineNode, SubmittableTaskStream};
use crate::{
    pipeline_node::{
        shuffles, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
    },
    stage::{StageConfig, StageExecutionContext},
    utils::channel::create_channel,
};

#[derive(Clone, Debug)]
pub enum ShuffleExchangeStrategy {
    NaiveFullyMaterializingMapReduce(shuffles::map_reduce::NaiveFullyMaterializingMapReduce),
    MapReduceWithPreShuffleMerge(shuffles::pre_shuffle_merge::MapReduceWithPreShuffleMerge),
}

pub(crate) struct ShuffleExchangeNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    columns: Vec<BoundExpr>,
    num_partitions: usize,
    strategy: ShuffleExchangeStrategy,
    child: Arc<dyn DistributedPipelineNode>,
}

impl ShuffleExchangeNode {
    const NODE_NAME: NodeName = "ShuffleExchange";
    const PARTITION_THRESHOLD_TO_USE_PRE_SHUFFLE_MERGE: usize = 200;

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        columns: Vec<BoundExpr>,
        num_partitions: Option<usize>,
        schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> DaftResult<Self> {
        let num_partitions =
            num_partitions.unwrap_or_else(|| child.config().clustering_spec.num_partitions());

        let strategy = Self::determine_strategy(&stage_config.config, &child, num_partitions)?;

        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            schema,
            stage_config.config.clone(),
            Arc::new(
                HashClusteringConfig::new(
                    num_partitions,
                    columns.clone().into_iter().map(|e| e.into()).collect(),
                )
                .into(),
            ),
        );

        Ok(Self {
            config,
            context,
            columns,
            num_partitions,
            strategy,
            child,
        })
    }

    fn determine_strategy(
        execution_config: &Arc<common_daft_config::DaftExecutionConfig>,
        child: &Arc<dyn DistributedPipelineNode>,
        target_num_partitions: usize,
    ) -> DaftResult<ShuffleExchangeStrategy> {
        let input_num_partitions = child.config().clustering_spec.num_partitions();

        match execution_config.shuffle_algorithm.as_str() {
            "pre_shuffle_merge" => Ok(ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge(
                shuffles::pre_shuffle_merge::MapReduceWithPreShuffleMerge::new(
                    execution_config.pre_shuffle_merge_threshold,
                ),
            )),
            "map_reduce" => Ok(ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce(
                shuffles::map_reduce::NaiveFullyMaterializingMapReduce,
            )),
            "flight_shuffle" => Err(DaftError::ValueError(
                "Flight shuffle not yet implemented for flotilla".to_string(),
            )),
            "auto" => {
                if Self::should_use_pre_shuffle_merge(input_num_partitions, target_num_partitions) {
                    Ok(ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge(
                        shuffles::pre_shuffle_merge::MapReduceWithPreShuffleMerge::new(
                            execution_config.pre_shuffle_merge_threshold,
                        ),
                    ))
                } else {
                    Ok(ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce(
                        shuffles::map_reduce::NaiveFullyMaterializingMapReduce,
                    ))
                }
            }
            _ => Ok(ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce(
                shuffles::map_reduce::NaiveFullyMaterializingMapReduce,
            )),
        }
    }

    fn should_use_pre_shuffle_merge(
        input_num_partitions: usize,
        target_num_partitions: usize,
    ) -> bool {
        let total_num_partitions = input_num_partitions * target_num_partitions;
        let geometric_mean = (total_num_partitions as f64).sqrt() as usize;
        geometric_mean > Self::PARTITION_THRESHOLD_TO_USE_PRE_SHUFFLE_MERGE
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    pub fn columns(&self) -> &Vec<BoundExpr> {
        &self.columns
    }

    pub fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec![];
        res.push(format!(
            "Repartition: {}",
            self.columns.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Num partitions = {}", self.num_partitions));

        match &self.strategy {
            ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce(_) => {
                res.push("Strategy: NaiveFullyMaterializingMapReduce".to_string());
            }
            ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge(strategy) => {
                res.push("Strategy: MapReduceWithPreShuffleMerge".to_string());
                res.push(format!(
                    "Pre-Shuffle Merge Threshold: {}",
                    strategy.pre_shuffle_merge_threshold
                ));
            }
        }
        res
    }

    pub async fn transpose_materialized_outputs(
        materialized_stream: impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin,
        num_partitions: usize,
    ) -> DaftResult<Vec<Vec<MaterializedOutput>>> {
        let materialized_partitions = materialized_stream
            .map(|mat| mat.map(|mat| mat.split_into_materialized_outputs()))
            .try_collect::<Vec<_>>()
            .await?;

        debug_assert!(
            materialized_partitions
                .iter()
                .all(|mat| mat.len() == num_partitions),
            "Expected all outputs to have {} partitions, got {}",
            num_partitions,
            materialized_partitions
                .iter()
                .map(|mat| mat.len())
                .join(", ")
        );

        let mut transposed_outputs = Vec::with_capacity(num_partitions);
        for idx in 0..num_partitions {
            let mut partition_group = Vec::with_capacity(materialized_partitions.len());
            for materialized_partition in &materialized_partitions {
                let part = &materialized_partition[idx];
                if part.num_rows()? > 0 {
                    partition_group.push(part.clone());
                }
            }
            transposed_outputs.push(partition_group);
        }

        assert_eq!(transposed_outputs.len(), num_partitions);
        Ok(transposed_outputs)
    }
}

impl TreeDisplay for ShuffleExchangeNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.context.node_name).unwrap();
            }
            _ => {
                let multiline_display = self.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.context.node_name.to_string()
    }
}

impl DistributedPipelineNode for ShuffleExchangeNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(stage_context);
        let self_arc = self.clone();

        let (result_tx, result_rx) = create_channel(1);

        // Move strategy and node to avoid borrowing issues
        let strategy = self_arc.strategy.clone();
        let task_id_counter = stage_context.task_id_counter();
        let scheduler_handle = stage_context.scheduler_handle();

        // Create the execution future that owns all its data
        let strategy_execution = async move {
            match strategy {
                ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce(naive) => {
                    naive
                        .execute(
                            self_arc,
                            input_node,
                            task_id_counter,
                            result_tx,
                            scheduler_handle,
                        )
                        .await
                }
                ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge(pre_shuffle) => {
                    pre_shuffle
                        .execute(
                            self_arc,
                            input_node,
                            task_id_counter,
                            result_tx,
                            scheduler_handle,
                        )
                        .await
                }
            }
        };

        stage_context.spawn(strategy_execution);
        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
