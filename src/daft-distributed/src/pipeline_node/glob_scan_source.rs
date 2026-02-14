use std::sync::Arc;

use common_error::DaftResult;
use common_io_config::IOConfig;
use common_metrics::ops::{NodeCategory, NodeType};
use common_scan_info::Pushdowns;
use daft_io::utils::group_glob_paths;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{ClusteringSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, stream};
use opentelemetry::metrics::Meter;

use super::{
    DistributedPipelineNode, NodeName, PipelineNodeConfig, PipelineNodeContext,
    scan_source::SourceStats,
};
use crate::{
    pipeline_node::{NodeID, PipelineNodeImpl, TaskBuilderStream},
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    statistics::stats::RuntimeStatsRef,
};

pub struct GlobScanSourceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    glob_paths: Arc<Vec<Vec<String>>>,
    pushdowns: Pushdowns,
    io_config: Option<IOConfig>,
}

impl GlobScanSourceNode {
    const NODE_NAME: NodeName = "GlobScanSource";

    pub fn try_new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        glob_paths: Arc<Vec<String>>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        io_config: Option<IOConfig>,
    ) -> DaftResult<Self> {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
            NodeType::GlobScan,
            NodeCategory::Source,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            Arc::new(ClusteringSpec::unknown_with_num_partitions(1)),
        );

        let glob_paths = Arc::new(group_glob_paths(&glob_paths)?);
        Ok(Self {
            config,
            context,
            glob_paths,
            pushdowns,
            io_config,
        })
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
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
        _plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let task_builders = self
            .glob_paths
            .iter()
            .map(|paths| {
                let glob_scan_plan = LocalPhysicalPlan::glob_scan(
                    Arc::new(paths.clone()),
                    self.pushdowns.clone(),
                    self.config.schema.clone(),
                    StatsState::NotMaterialized,
                    self.io_config.clone(),
                    LocalNodeContext {
                        origin_node_id: Some(self.node_id() as usize),
                        additional: None,
                    },
                );
                SwordfishTaskBuilder::new(glob_scan_plan, self.as_ref())
            })
            .collect::<Vec<_>>();

        TaskBuilderStream::new(stream::iter(task_builders).boxed())
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![
            "GlobScanSource:".to_string(),
            format!("Num Glob Tasks = {}", self.glob_paths.len()),
        ];

        res.push("Glob paths: [".to_string());
        for group in self.glob_paths.iter() {
            res.push("  {".to_string());
            for path in group {
                res.push(format!("    {}", path));
            }
            res.push("  }".to_string());
        }
        res.push("]".to_string());

        res.extend(self.pushdowns.multiline_display());
        res
    }

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(SourceStats::new(meter, self.node_id()))
    }
}
