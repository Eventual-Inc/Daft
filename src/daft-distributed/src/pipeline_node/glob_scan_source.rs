use std::sync::Arc;

use common_error::DaftResult;
use common_io_config::IOConfig;
use common_scan_info::Pushdowns;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{ClusteringSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;

use super::{
    DistributedPipelineNode, NodeName, PipelineNodeConfig, PipelineNodeContext,
    SubmittableTaskStream,
};
use crate::{
    pipeline_node::NodeID,
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{Sender, create_channel},
};

pub struct GlobScanSourceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    glob_paths: Vec<String>,
    pushdowns: Pushdowns,
    io_config: Option<IOConfig>,
}

impl GlobScanSourceNode {
    const NODE_NAME: NodeName = "GlobScanSource";

    pub fn new(
        node_id: NodeID,
        stage_config: &StageConfig,
        glob_paths: Vec<String>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        logical_node_id: Option<NodeID>,
        io_config: Option<IOConfig>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![],
            vec![],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            schema,
            stage_config.config.clone(),
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

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    async fn execution_loop(
        self: Arc<Self>,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        // For now, return a placeholder task
        let task =
            self.make_glob_scan_task(TaskContext::from((&self.context, task_id_counter.next())))?;
        let _ = result_tx.send(SubmittableTask::new(task)).await;
        Ok(())
    }

    fn make_glob_scan_task(&self, task_context: TaskContext) -> DaftResult<SwordfishTask> {
        // Create a LocalPhysicalPlan::glob_scan task
        let physical_glob_scan = LocalPhysicalPlan::glob_scan(
            self.glob_paths.clone(),
            self.pushdowns.clone(),
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            self.io_config.clone(),
        );

        let task = SwordfishTask::new(
            task_context,
            physical_glob_scan,
            self.config.execution_config.clone(),
            Default::default(),
            SchedulingStrategy::Spread,
            self.context.to_hashmap(),
        );
        Ok(task)
    }
}

impl DistributedPipelineNode for GlobScanSourceNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![]
    }

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let (tx, rx) = create_channel(1000);
        let task_id_counter = stage_context.task_id_counter().clone();
        tokio::spawn(async move {
            if let Err(e) = self.execution_loop(tx, task_id_counter).await {
                eprintln!("Error in glob scan execution loop: {}", e);
            }
        });
        rx.into()
    }

    fn as_tree_display(&self) -> &dyn common_display::tree::TreeDisplay {
        self
    }
}

impl common_display::tree::TreeDisplay for GlobScanSourceNode {
    fn display_as(&self, _level: common_display::DisplayLevel) -> String {
        format!(
            "GlobScanSource(glob_paths={:?}, pushdowns={:?})",
            self.glob_paths, self.pushdowns
        )
    }

    fn get_children(&self) -> Vec<&dyn common_display::tree::TreeDisplay> {
        vec![]
    }
}
