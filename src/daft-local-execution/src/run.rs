use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_dsl::common_treenode::TreeNode;
use daft_micropartition::MicroPartition;
use daft_plan::QueryStageOutput;

use crate::{pipeline::execute_pipelines, visitor::PhysicalToPipelineVisitor};

pub fn run_simple(
    query_stage: &QueryStageOutput,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
    let (physical_plan, _is_final) = match query_stage {
        QueryStageOutput::Partial { physical_plan, .. } => (physical_plan.as_ref(), false),
        QueryStageOutput::Final { physical_plan, .. } => (physical_plan.as_ref(), true),
    };
    let mut pipeline_visitor = PhysicalToPipelineVisitor {
        pipelines: Vec::new(),
        psets: psets.clone(),
    };
    Arc::new(physical_plan.clone()).visit(&mut pipeline_visitor)?;
    let pipelines = pipeline_visitor.pipelines;
    let result = execute_pipelines(pipelines)?;
    Ok(Box::new(result.into_iter().map(Ok)))
}
