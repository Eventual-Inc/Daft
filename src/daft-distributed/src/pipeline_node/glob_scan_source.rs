use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_io_config::IOConfig;
use common_scan_info::Pushdowns;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{ClusteringSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;

use super::{
    DistributedPipelineNode, NodeName, PipelineNodeConfig, PipelineNodeContext, TaskBuilderStream,
};
use crate::{
    pipeline_node::{NodeID, PipelineNodeImpl},
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::{Sender, create_channel},
};

pub struct GlobScanSourceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    glob_paths: Arc<Vec<String>>,
    pushdowns: Pushdowns,
    io_config: Option<IOConfig>,
}

impl GlobScanSourceNode {
    const NODE_NAME: NodeName = "GlobScanSource";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        glob_paths: Arc<Vec<String>>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        io_config: Option<IOConfig>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            Arc::new(ClusteringSpec::unknown_with_num_partitions(1)),
        );
        Self {
            config,
            context,
            glob_paths,
            pushdowns,
            io_config,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    async fn execution_loop(
        self: Arc<Self>,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        if self.glob_paths.is_empty() {
            let transformed_plan = LocalPhysicalPlan::streaming_in_memory_scan(
                self.node_id().to_string(),
                self.config.schema.clone(),
                0,
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(self.node_id() as usize),
                    additional: None,
                },
            );
            let empty_glob_task = SwordfishTaskBuilder::new(transformed_plan, self.as_ref())
                .with_psets(HashMap::from([(self.node_id().to_string(), vec![])]));
            let _ = result_tx.send(empty_glob_task).await;
            return Ok(());
        }

        // Create a streaming_glob_scan plan instead of glob_scan
        let streaming_glob_scan = LocalPhysicalPlan::streaming_glob_scan(
            self.node_id().to_string(),
            self.pushdowns.clone(),
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            self.io_config.clone(),
            LocalNodeContext {
                origin_node_id: Some(self.node_id() as usize),
                additional: None,
            },
        );

        // Create glob_paths_map with a single entry containing all glob paths
        let glob_paths_map = HashMap::from([(
            self.node_id().to_string(),
            self.glob_paths.iter().cloned().collect::<Vec<String>>(),
        )]);
        let builder = SwordfishTaskBuilder::new(streaming_glob_scan, self.as_ref())
            .with_glob_paths(glob_paths_map);
        let _ = result_tx.send(builder).await;
        Ok(())
    }
}

impl PipelineNodeImpl for GlobScanSourceNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(result_tx);
        plan_context.spawn(execution_loop);
        TaskBuilderStream::from(result_rx)
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![
            "GlobScanSource".to_string(),
            format!("Glob paths = {:?}", self.glob_paths),
        ];
        res.extend(self.pushdowns.multiline_display());
        res
    }
}
