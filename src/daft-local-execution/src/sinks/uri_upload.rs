use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::get_compute_pool_num_threads;
use daft_dsl::ExprRef;
use daft_io::{get_io_client, IOClient};
use tokio::task::JoinSet;
use daft_micropartition::MicroPartition;
use tracing::{instrument, Span};
use daft_functions_uri::UrlUploadArgs;

use super::streaming_sink::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    StreamingSinkState,
};
use crate::{
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

struct UriUploadSinkState {
    in_flight_uploads: JoinSet<DaftResult<usize>>,
    io_client: Arc<IOClient>,
    submitted_uploads: usize,
}

impl UriUploadSinkState {
    fn new() -> DaftResult<Self> {
        Ok(Self {
            in_flight_uploads: JoinSet::new(),
            io_client: get_io_client(true, todo!())?,
            submitted_uploads: 0,
        })
    }

    fn upload(&mut self, args: UrlUploadArgs<ExprRef>, input: Arc<MicroPartition>) -> DaftResult<()> {
        self.in_flight_uploads.spawn(async move {
            self.io_client.single_url_upload(
                0,
                uri_col,
                input,
                true,
                todo!(),
            )
            .await?;
            Ok(self.submitted_uploads)
        });
        self.submitted_uploads += 1;

        Ok(())
    }

    async fn poll_finished(&mut self, args: UrlUploadArgs<ExprRef>) -> DaftResult<()> {
        // TODO: With capacity
        let mut completed_uploads = vec![];
        
        // Wait for uploads to complete until we are under the active limit
        let max_in_flight = args.max_connections.unwrap_or(32) * 4;
        while self.in_flight_uploads.len() > max_in_flight {
            let Some(result) = self.in_flight_uploads.join_next().await else {
                unreachable!("There should always be at least one upload in flight");
            };

            let result = result.unwrap().unwrap();
            completed_uploads.push(result);
        }

        // If any additional uploads are completed, pop them off the join set
        while let Some(result) = self.in_flight_uploads.try_join_next().await {
            let result = result.unwrap().unwrap();
            completed_uploads.push(result);
        }

        // Construct the output using the indexes
        Ok(())
    }

    async fn finish_all(&mut self) -> DaftResult<()> {
        let results = self.in_flight_uploads.join_all().await.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>();
        
        // Construct the output using the indexes
        Ok(())
    }
}

impl StreamingSinkState for UriUploadSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct UriUploadSink {
    uri_column: String,
}

impl UriUploadSink {
    pub fn new(uri_column: String) -> Self {
        Self { uri_column }
    }
}

impl StreamingSink for UriUploadSink {
    #[instrument(skip_all, name = "UriUploadSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult {

        spawner.spawn(
            async move {
                let state = state
                    .as_any_mut()
                    .downcast_mut::<UriUploadSinkState>()
                    .expect("UriUpload sink should have UriUploadSinkState");

                
            },
            Span::current(),
        ).into()
    }

    fn name(&self) -> &'static str {
        "UriUpload"
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("UriUpload")]
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn StreamingSinkState>>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(UriUploadSinkState::new())
    }

    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn dispatch_spawner(
        &self,
        _runtime_handle: &ExecutionRuntimeContext,
        _maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        // Limits are greedy, so we don't need to buffer any input.
        // They are also not concurrent, so we don't need to worry about ordering.
        Arc::new(UnorderedDispatcher::unbounded())
    }
}
