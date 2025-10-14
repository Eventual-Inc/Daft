use std::{collections::HashMap, sync::Arc};

use common_display::{DisplayAs, DisplayLevel};
use common_error::DaftResult;
use common_file_formats::FileFormatConfig;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{ClusteringSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;

use super::{
    NodeName, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl, SubmittableTaskStream,
};
use crate::{
    pipeline_node::{DistributedPipelineNode, NodeID},
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    utils::channel::{Sender, create_channel},
};

pub(crate) struct ScanSourceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    pushdowns: Pushdowns,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
}

impl ScanSourceNode {
    const NODE_NAME: NodeName = "ScanSource";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        schema: SchemaRef,
        logical_node_id: Option<NodeID>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.plan_id,
            node_id,
            Self::NODE_NAME,
            vec![],
            vec![],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            Arc::new(ClusteringSpec::unknown_with_num_partitions(
                scan_tasks.len(),
            )),
        );
        Self {
            config,
            context,
            pushdowns,
            scan_tasks,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    async fn execution_loop(
        self: Arc<Self>,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        if self.scan_tasks.is_empty() {
            let empty_scan_task = self
                .make_empty_scan_task(TaskContext::from((&self.context, task_id_counter.next())))?;
            let _ = result_tx.send(SubmittableTask::new(empty_scan_task)).await;
            return Ok(());
        }

        for scan_task in self.scan_tasks.iter() {
            let task = self.make_source_tasks(
                vec![scan_task.clone()].into(),
                TaskContext::from((&self.context, task_id_counter.next())),
            )?;
            if result_tx.send(SubmittableTask::new(task)).await.is_err() {
                break;
            }
        }

        Ok(())
    }

    fn make_source_tasks(
        &self,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        task_context: TaskContext,
    ) -> DaftResult<SwordfishTask> {
        let physical_scan = LocalPhysicalPlan::physical_scan(
            scan_tasks.clone(),
            self.pushdowns.clone(),
            self.config.schema.clone(),
            StatsState::NotMaterialized,
        );

        let task = SwordfishTask::new(
            task_context,
            physical_scan,
            self.config.execution_config.clone(),
            Default::default(),
            SchedulingStrategy::Spread,
            self.context.to_hashmap(),
        );
        Ok(task)
    }

    fn make_empty_scan_task(&self, task_context: TaskContext) -> DaftResult<SwordfishTask> {
        let transformed_plan = LocalPhysicalPlan::empty_scan(self.config.schema.clone());
        let psets = HashMap::new();
        let task = SwordfishTask::new(
            task_context,
            transformed_plan,
            self.config.execution_config.clone(),
            psets,
            SchedulingStrategy::Spread,
            self.context.to_hashmap(),
        );
        Ok(task)
    }
}

impl PipelineNodeImpl for ScanSourceNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![]
    }

    fn multiline_display(&self, verbose: bool) -> Vec<String> {
        fn base_display(scan: &ScanSourceNode) -> Vec<String> {
            let num_scan_tasks = scan.scan_tasks.len();
            let total_bytes: usize = scan
                .scan_tasks
                .iter()
                .map(|st| st.size_bytes_on_disk().unwrap_or(0))
                .sum();

            #[allow(unused_mut)]
            let mut s = vec![
                "ScanTaskSource:".to_string(),
                format!("Num Scan Tasks = {num_scan_tasks}"),
                format!("Estimated Scan Bytes = {total_bytes}"),
            ];
            #[cfg(feature = "python")]
            if let FileFormatConfig::Database(config) =
                scan.scan_tasks[0].file_format_config().as_ref()
            {
                if num_scan_tasks == 1 {
                    s.push(format!("SQL Query = {}", &config.sql));
                } else {
                    s.push(format!("SQL Queries = [{},..]", &config.sql));
                }
            }
            s
        }

        let mut s = base_display(self);
        if !verbose {
            // We're only going to display the pushdowns and schema for the first scan task.
            let pushdown = self.scan_tasks[0].pushdowns();
            if !pushdown.is_empty() {
                s.push(pushdown.display_as(DisplayLevel::Compact));
            }

            let schema = self.scan_tasks[0].schema();
            s.push(format!(
                "Schema: {{{}}}",
                schema.display_as(DisplayLevel::Compact)
            ));

            let tasks = self.scan_tasks.iter();

            s.push("Scan Tasks: [".to_string());
            for (i, st) in tasks.enumerate() {
                if i < 3 || i >= self.scan_tasks.len() - 3 {
                    s.push(st.as_ref().display_as(DisplayLevel::Compact));
                } else if i == 3 {
                    s.push("...".to_string());
                }
            }
        } else {
            s.push("Scan Tasks: [".to_string());

            for st in self.scan_tasks.iter() {
                s.push(st.as_ref().display_as(DisplayLevel::Verbose));
            }
        }
        s.push("]".to_string());
        s
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(result_tx, plan_context.task_id_counter());
        plan_context.spawn(execution_loop);

        SubmittableTaskStream::from(result_rx)
    }
}
