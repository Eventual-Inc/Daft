use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::QueryStageOutput;

use crate::create_streaming_pipeline::physical_plan_to_streaming_pipeline;

pub fn run_streaming(
    query_stage: &QueryStageOutput,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
    let (physical_plan, _is_final) = match query_stage {
        QueryStageOutput::Partial { physical_plan, .. } => (physical_plan, false),
        QueryStageOutput::Final { physical_plan, .. } => (physical_plan, true),
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let res = runtime.block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Arc<MicroPartition>>>(32);
        let mut streaming_pipelines = physical_plan_to_streaming_pipeline(physical_plan, &psets);
        let mut last = streaming_pipelines.pop().unwrap();
        last.set_next_pipeline_tx(tx);

        for pipeline in streaming_pipelines {
            tokio::spawn(async move {
                pipeline.run().await;
            });
        }

        tokio::spawn(async move {
            last.run().await;
        });

        // TODO: don't collect here, just create an iterator
        let mut res = vec![];
        while let Some(part) = rx.recv().await {
            res.extend(part);
        }
        res
    });
    Ok(Box::new(res.into_iter().map(Ok)))
}
