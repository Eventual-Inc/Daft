use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::QueryStageOutput;
use futures::StreamExt;

use crate::{create_pipeline::physical_plan_to_pipeline, sources::source::Source};

pub fn run_streaming(
    query_stage: &QueryStageOutput,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
    let (physical_plan, _is_final) = match query_stage {
        QueryStageOutput::Partial { physical_plan, .. } => (physical_plan, false),
        QueryStageOutput::Final { physical_plan, .. } => (physical_plan, true),
    };
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let res = runtime.block_on(async {
        let pipeline = physical_plan_to_pipeline(physical_plan, &psets);
        pipeline.get_data().collect::<Vec<_>>().await
        // TODO: Convert the stream to an iterator
    });
    Ok(Box::new(res.into_iter()))
}
