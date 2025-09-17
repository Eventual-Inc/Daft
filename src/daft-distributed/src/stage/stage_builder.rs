use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_logical_plan::{LogicalPlanRef, partitioning::ClusteringSpecRef};
use daft_schema::schema::SchemaRef;

use super::{DataChannel, OutputChannel, Stage, StageID, StagePlan, StageType};

pub(crate) struct StagePlanBuilder {
    stages: HashMap<StageID, Stage>,
    stage_id_counter: StageID,
}

impl StagePlanBuilder {
    pub fn new() -> Self {
        Self {
            stages: HashMap::new(),
            stage_id_counter: 0,
        }
    }

    fn next_stage_id(&mut self) -> StageID {
        let curr = self.stage_id_counter;
        self.stage_id_counter += 1;
        curr
    }

    fn build_stages_from_plan(&mut self, plan: LogicalPlanRef) -> DaftResult<StageID> {
        let schema = plan.schema();
        // Create a MapPipeline stage
        let stage_id = self.next_stage_id();
        let stage = Stage {
            id: stage_id,
            type_: StageType::MapPipeline { plan },
            input_channels: vec![],
            output_channels: vec![self.create_output_channel(schema, None)?],
        };

        // TODO: Add upstream stage to output channel stages
        self.stages.insert(stage_id, stage);
        Ok(stage_id)
    }

    pub fn build_stage_plan(
        mut self,
        plan: LogicalPlanRef,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<StagePlan> {
        let root_stage_id = self.build_stages_from_plan(plan)?;
        Ok(StagePlan {
            stages: self.stages,
            root_stage: root_stage_id,
            config,
        })
    }

    fn create_output_channel(
        &self,
        schema: SchemaRef,
        clustering_spec: Option<ClusteringSpecRef>,
    ) -> DaftResult<OutputChannel> {
        Ok(OutputChannel {
            to_stages: vec![], // Will be populated later when connections are established
            data_channel: DataChannel {
                schema,
                clustering_spec,
                stats: None, // Stats will be computed during execution
            },
        })
    }
}
