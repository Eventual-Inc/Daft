use std::sync::Arc;

use arrow_array::builder::LargeBinaryBuilder;
use common_error::DaftResult;
use common_runtime::{get_io_runtime, RuntimeRef};
use daft_core::{
    prelude::{AsArrow, BinaryArray, DataType, Field, Schema, SchemaRef},
    series::{IntoSeries, Series},
};
use daft_dsl::expr::{bound_expr::BoundExpr, BoundColumn};
use daft_functions_uri::{download::UrlDownloadArgsDefault, UrlDownloadArgs};
use daft_io::{get_io_client, IOClient};
use daft_micropartition::MicroPartition;
use tokio::task::JoinSet;
use tokio_util::bytes::Bytes;
use tracing::{instrument, Span};

use crate::{
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    ops::NodeType,
    pipeline::NodeName,
    runtime_stats::RuntimeStats,
    streaming_sink::{
        async_ops::template::{
            AsyncOpRuntimeStatsBuilder, AsyncOpState, AsyncSinkState, IN_FLIGHT_SCALE_FACTOR,
        },
        base::{
            StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
            StreamingSinkFinalizeResult, StreamingSinkOutput, StreamingSinkState,
        },
    },
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

struct UrlDownloadOpState {
    // Arguments
    args: Arc<UrlDownloadArgsDefault<BoundExpr>>,

    // Fixed state
    io_client: Arc<IOClient>,
    io_runtime_handle: RuntimeRef,

    // Updatable state
    submitted_downloads: usize,
    in_flight_downloads: JoinSet<DaftResult<(usize, Option<Bytes>)>>,
}

impl UrlDownloadOpState {
    fn new(args: Arc<UrlDownloadArgsDefault<BoundExpr>>) -> Self {
        let multi_thread = args.multi_thread;
        let io_config = args.io_config.clone();

        Self {
            args,

            io_runtime_handle: get_io_runtime(true),
            io_client: get_io_client(multi_thread, io_config).expect("Failed to get IO client"),

            submitted_downloads: 0,
            in_flight_downloads: JoinSet::new(),
        }
    }
}

impl AsyncOpState for UrlDownloadOpState {
    fn num_active_tasks(&self) -> usize {
        self.in_flight_downloads.len()
    }

    fn execute_batch_size(&self) -> usize {
        if self.num_active_tasks() < self.args.max_connections {
            0
        } else {
            std::cmp::min(
                self.num_active_tasks() - self.args.max_connections,
                self.args.max_connections,
            )
        }
    }

    fn finalize_batch_size(&self) -> usize {
        std::cmp::min(self.num_active_tasks(), self.args.max_connections)
    }

    fn accept_more_input(&self) -> bool {
        self.num_active_tasks() < (self.args.max_connections * *IN_FLIGHT_SCALE_FACTOR)
    }

    fn process_input(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        let raise_error_on_failure = self.args.raise_error_on_failure;

        for input in input.get_tables()?.iter() {
            let url_col = input.eval_expression(&self.args.input)?;
            let url_col = url_col.utf8()?.as_arrow();

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

        Ok(())
    }

    async fn poll_finished(
        &mut self,
        num_rows: usize,
        stats: &AsyncOpRuntimeStatsBuilder,
    ) -> DaftResult<(Vec<u64>, Series)> {
        let mut completed_idxs = Vec::with_capacity(num_rows);
        let mut completed_contents = LargeBinaryBuilder::with_capacity(num_rows, num_rows);

        // Wait for enough downloads to complete
        while completed_idxs.len() < num_rows {
            let Some(result) = self.in_flight_downloads.join_next().await else {
                unreachable!("There should always be at least one download in flight");
            };

            let (idx, contents) = result.expect("Failed to join download")?;
            if contents.is_none() {
                stats.inc_num_failed();
            }

            completed_idxs.push(idx as u64);
            completed_contents.append_option(contents);
        }

        let completed_contents = BinaryArray::from(("", completed_contents.finish())).into_series();

        Ok((completed_idxs, completed_contents))
    }
}

type UrlDownloadSinkState = AsyncSinkState<UrlDownloadOpState>;

pub struct UrlDownloadSink {
    args: Arc<UrlDownloadArgsDefault<BoundExpr>>,
    passthrough_columns: Vec<BoundColumn>,
    save_schema: SchemaRef,
    output_schema: SchemaRef,
    output_column: String,
    input_schema: SchemaRef,
    input_size_bytes_buffer: usize,
}

impl UrlDownloadSink {
    pub fn new(
        args: UrlDownloadArgs<BoundExpr>,
        passthrough_columns: Vec<BoundColumn>,
        output_column: String,
        input_schema: SchemaRef,
        input_size_bytes_buffer: usize,
    ) -> DaftResult<Self> {
        // Generate output schema initially
        let mut save_fields = passthrough_columns
            .iter()
            .map(|expr| input_schema[expr.index].clone())
            .collect::<Vec<_>>();
        let save_schema = Arc::new(Schema::new(save_fields.clone()));

        save_fields.push(Field::new(output_column.as_str(), DataType::Binary));
        let output_schema = Arc::new(Schema::new(save_fields));

        Ok(Self {
            args: Arc::new(args.unwrap_or_default()?),
            passthrough_columns,
            save_schema,
            output_schema,
            output_column,
            input_schema,
            input_size_bytes_buffer,
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
        let stats = spawner.runtime_stats.clone();

        spawner
            .spawn(
                async move {
                    let url_state = state
                        .as_any_mut()
                        .downcast_mut::<UrlDownloadSinkState>()
                        .expect("UrlDownload sink should have UrlDownloadSinkState");

                    let stats = stats.as_any_arc();
                    let stats = stats.downcast_ref::<AsyncOpRuntimeStatsBuilder>().expect(
                        "AsyncOpRuntimeStatsBuilder should be the additional stats builder",
                    );

                    url_state.process_input(input)?;
                    let output = url_state.poll_finished(false, stats).await?;

                    let schema = output.schema.clone();
                    let output = Arc::new(MicroPartition::new_loaded(
                        schema,
                        Arc::new(vec![output]),
                        None,
                    ));

                    // Accept more input if we have room
                    if url_state.accept_more_input() {
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
        let stats = spawner.runtime_stats.clone();

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

                    let stats = stats.as_any_arc();
                    let stats = stats.downcast_ref::<AsyncOpRuntimeStatsBuilder>().expect(
                        "AsyncOpRuntimeStatsBuilder should be the additional stats builder",
                    );

                    let output = state.poll_finished(true, stats).await?;
                    let schema = output.schema.clone();
                    let output = Arc::new(MicroPartition::new_loaded(
                        schema,
                        Arc::new(vec![output]),
                        None,
                    ));

                    if state.num_active_tasks() > 0 {
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

    fn op_type(&self) -> NodeType {
        NodeType::UrlDownload
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
        let inner_state = UrlDownloadOpState::new(self.args.clone());

        Box::new(UrlDownloadSinkState::new(
            self.passthrough_columns.clone(),
            self.save_schema.clone(),
            self.output_schema.clone(),
            self.output_column.clone(),
            self.input_size_bytes_buffer,
            inner_state,
        ))
    }

    fn make_runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        Arc::new(AsyncOpRuntimeStatsBuilder::new())
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
