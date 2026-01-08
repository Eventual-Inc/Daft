use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use tracing::{Span, instrument};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
    StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

struct UuidParams {
    column_name: String,
    output_schema: SchemaRef,
}

pub struct UuidSink {
    params: Arc<UuidParams>,
}

impl UuidSink {
    pub fn new(column_name: String, output_schema: SchemaRef) -> Self {
        Self {
            params: Arc::new(UuidParams {
                column_name,
                output_schema,
            }),
        }
    }
}

impl StreamingSink for UuidSink {
    type State = ();
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;

    #[instrument(skip_all, name = "UuidSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let tables = input.record_batches();
                    let mut results = Vec::with_capacity(tables.len());
                    for t in tables {
                        results.push(t.add_uuid_column(&params.column_name)?);
                    }

                    let out = MicroPartition::new_loaded(
                        params.output_schema.clone(),
                        results.into(),
                        None,
                    );

                    Ok((
                        state,
                        StreamingSinkOutput::NeedMoreInput(Some(Arc::new(out))),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Uuid".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Uuid
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["Uuid".to_string()]
    }

    fn finalize(
        &self,
        _states: Vec<Self::State>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        Ok(StreamingSinkFinalizeOutput::Finished(None)).into()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(())
    }

    fn batching_strategy(&self) -> Self::BatchingStrategy {
        crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        )
    }
}
