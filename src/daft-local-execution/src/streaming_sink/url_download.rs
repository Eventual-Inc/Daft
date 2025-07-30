use std::sync::Arc;

use arrow_array::builder::LargeBinaryBuilder;
use common_error::DaftResult;
use common_runtime::{get_io_runtime, RuntimeRef};
use daft_core::{
    prelude::{AsArrow, BinaryArray, DataType, Field, SchemaRef},
    series::IntoSeries,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions_uri::{download::UrlDownloadArgsDefault, UrlDownloadArgs};
use daft_io::{get_io_client, IOClient};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tokio::task::JoinSet;
use tokio_util::bytes::Bytes;
use tracing::{instrument, Span};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
    StreamingSinkFinalizeResult, StreamingSinkOutput, StreamingSinkState,
};
use crate::{
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    pipeline::NodeName,
    streaming_sink::base::build_output,
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

const IN_FLIGHT_SCALE_FACTOR: usize = 32;

struct UrlDownloadSinkState {
    // Arguments
    args: Arc<UrlDownloadArgsDefault<BoundExpr>>,
    output_schema: SchemaRef,
    output_column: String,

    // Fixed state
    io_client: Arc<IOClient>,
    io_runtime_handle: RuntimeRef,

    // Updatable state
    in_flight_downloads: JoinSet<DaftResult<(usize, Option<Bytes>)>>,
    all_inputs: Arc<MicroPartition>,
    submitted_downloads: usize,
}

impl UrlDownloadSinkState {
    fn new(
        args: Arc<UrlDownloadArgsDefault<BoundExpr>>,
        output_column: String,
        input_schema: SchemaRef,
    ) -> Self {
        let multi_thread = args.multi_thread;
        let io_config = args.io_config.clone();

        // Generate output schema initially
        let mut output_schema = input_schema.as_ref().clone();
        output_schema.append(Field::new(output_column.as_str(), DataType::Binary));

        Self {
            args,
            output_schema: Arc::new(output_schema),
            output_column,

            io_runtime_handle: get_io_runtime(true),
            io_client: get_io_client(multi_thread, io_config).expect("Failed to get IO client"),

            in_flight_downloads: JoinSet::new(),
            all_inputs: Arc::new(MicroPartition::empty(Some(input_schema))),
            submitted_downloads: 0,
        }
    }

    pub fn get_in_flight(&self) -> usize {
        self.in_flight_downloads.len()
    }

    fn max_in_flight(&self) -> usize {
        self.args.max_connections * IN_FLIGHT_SCALE_FACTOR
    }

    /// Start downloads for urls in given input
    pub fn start_download(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        if input.is_empty() {
            return Ok(());
        }

        let raise_error_on_failure = self.args.raise_error_on_failure;

        for input in input.get_tables()?.iter() {
            let url_col = input.eval_expression(&self.args.input)?;
            let url_col = url_col.utf8()?;
            let url_col = url_col.as_arrow();

            for url_val in url_col {
                let row_idx = self.submitted_downloads;
                let url_val = url_val.map(ToString::to_string);
                let io_client = self.io_client.clone();

                let handle = self.io_runtime_handle.spawn(async move {
                    let contents = io_client
                        .single_url_download(row_idx, url_val, raise_error_on_failure, None)
                        .await?;

                    Ok((row_idx, contents))
                });

                self.in_flight_downloads.spawn(async move { handle.await? });
                self.submitted_downloads += 1;
            }
        }

        self.all_inputs = Arc::new(MicroPartition::concat([self.all_inputs.clone(), input])?);
        Ok(())
    }

    pub async fn poll_finished(&mut self, finished: bool) -> DaftResult<RecordBatch> {
        let exp_capacity = if finished {
            self.in_flight_downloads.len()
        } else if self.in_flight_downloads.len() > self.max_in_flight() {
            std::cmp::min(
                self.in_flight_downloads.len() - self.max_in_flight(),
                self.args.max_connections,
            )
        } else {
            0
        };

        let mut completed_idxs = Vec::with_capacity(exp_capacity);
        let mut completed_contents = LargeBinaryBuilder::with_capacity(exp_capacity, exp_capacity);

        // Wait for downloads to complete until we are under the active limit
        while completed_idxs.len() < exp_capacity {
            let Some(result) = self.in_flight_downloads.join_next().await else {
                unreachable!("There should always be at least one download in flight");
            };

            let (idx, contents) = result.expect("Failed to join download")?;
            completed_idxs.push(idx as u64);
            completed_contents.append_option(contents);
        }

        let completed_contents =
            BinaryArray::from((self.output_column.as_str(), completed_contents.finish()))
                .into_series();

        build_output(
            self.all_inputs.clone(),
            completed_idxs,
            completed_contents,
            self.output_schema.clone(),
        )
    }
}

impl StreamingSinkState for UrlDownloadSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct UrlDownloadSink {
    args: Arc<UrlDownloadArgsDefault<BoundExpr>>,
    output_column: String,
    input_schema: SchemaRef,
}

impl UrlDownloadSink {
    pub fn new(
        args: UrlDownloadArgs<BoundExpr>,
        output_column: String,
        input_schema: SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            args: Arc::new(args.unwrap_or_default()?),
            output_column,
            input_schema,
        })
    }
}

impl StreamingSink for UrlDownloadSink {
    #[instrument(skip_all, name = "UrlDownloadSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult {
        let input_schema = self.input_schema.clone();

        spawner
            .spawn(
                async move {
                    let url_state = state
                        .as_any_mut()
                        .downcast_mut::<UrlDownloadSinkState>()
                        .expect("UrlDownload sink should have UrlDownloadSinkState");

                    url_state.start_download(input)?;
                    let output = url_state.poll_finished(false).await?;

                    let schema = output.schema.clone();
                    let output = Arc::new(MicroPartition::new_loaded(
                        schema,
                        Arc::new(vec![output]),
                        None,
                    ));

                    // Accept more input if we have room
                    if url_state.get_in_flight() <= url_state.max_in_flight() {
                        Ok((state, StreamingSinkOutput::NeedMoreInput(Some(output))))
                    } else {
                        let next_input = Arc::new(MicroPartition::empty(Some(input_schema)));
                        Ok((
                            state,
                            StreamingSinkOutput::HasMoreOutput { next_input, output },
                        ))
                    }
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "UrlDownloadSink::finalize")]
    fn finalize(
        &self,
        mut states: Vec<Box<dyn StreamingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        spawner
            .spawn(
                async move {
                    assert!(states.len() == 1);
                    let state = states
                        .get_mut(0)
                        .expect("There should be exactly one UrlDownloadSinkState")
                        .as_any_mut()
                        .downcast_mut::<UrlDownloadSinkState>()
                        .expect("UrlDownload sink state should be UrlDownloadSinkState");

                    let output = state.poll_finished(true).await?;
                    let schema = output.schema.clone();
                    let output = Arc::new(MicroPartition::new_loaded(
                        schema,
                        Arc::new(vec![output]),
                        None,
                    ));

                    if state.get_in_flight() > 0 {
                        Ok(StreamingSinkFinalizeOutput::HasMoreOutput {
                            states,
                            output: Some(output),
                        })
                    } else {
                        Ok(StreamingSinkFinalizeOutput::Finished(
                            if !output.is_empty() {
                                Some(output)
                            } else {
                                None
                            },
                        ))
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "URL Download".into()
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!(
                "URL Download: {} -> {}",
                self.args.input, self.output_column
            ),
            format!("Multi-thread: {}", self.args.multi_thread),
            format!("Max Connections: {}", self.args.max_connections),
            format!(
                "On Failure: {}",
                if self.args.raise_error_on_failure {
                    "raise"
                } else {
                    "null"
                }
            ),
        ]
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(UrlDownloadSinkState::new(
            self.args.clone(),
            self.output_column.clone(),
            self.input_schema.clone(),
        ))
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn dispatch_spawner(
        &self,
        _runtime_handle: &ExecutionRuntimeContext,
        _maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        // To buffer the input better, we should use smaller input batches
        Arc::new(UnorderedDispatcher::new(0, self.args.max_connections))
    }
}
