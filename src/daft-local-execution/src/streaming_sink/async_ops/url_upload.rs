use std::sync::Arc;

use arrow_array::builder::LargeStringBuilder;
use common_error::DaftResult;
use common_runtime::{get_io_runtime, RuntimeRef};
use daft_core::{
    prelude::{AsArrow, DataType, Field, Schema, SchemaRef, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::expr::{bound_expr::BoundExpr, BoundColumn};
use daft_functions_uri::{upload::UrlUploadArgsDefault, UrlUploadArgs};
use daft_io::{get_io_client, IOClient};
use daft_micropartition::MicroPartition;
use tokio::task::JoinSet;
use tokio_util::bytes::Bytes;
use tracing::{instrument, Span};

use crate::{
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    pipeline::NodeName,
    runtime_stats::RuntimeStatsBuilder,
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

struct UrlUploadOpState {
    // Arguments
    args: Arc<UrlUploadArgsDefault<BoundExpr>>,

    // Fixed state
    io_client: Arc<IOClient>,
    io_runtime_handle: RuntimeRef,

    // Updatable state
    in_flight_uploads: JoinSet<DaftResult<(usize, Option<String>)>>,
    submitted_uploads: usize,
}

impl UrlUploadOpState {
    fn new(args: Arc<UrlUploadArgsDefault<BoundExpr>>) -> Self {
        let multi_thread = args.multi_thread;
        let io_config = args.io_config.clone();

        Self {
            args,

            io_runtime_handle: get_io_runtime(multi_thread),
            io_client: get_io_client(multi_thread, io_config).expect("Failed to get IO client"),

            in_flight_uploads: JoinSet::new(),
            submitted_uploads: 0,
        }
    }
}

impl AsyncOpState for UrlUploadOpState {
    fn num_active_tasks(&self) -> usize {
        self.in_flight_uploads.len()
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
            let data_col = input.eval_expression(&self.args.input)?;
            let data_col = data_col.binary()?.as_arrow();

            let location_col = input.eval_expression(&self.args.location)?;
            let location_col = location_col.utf8()?.as_arrow();

            for (data_val, location_val) in data_col.iter().zip(location_col.iter()) {
                let submitted_uploads = self.submitted_uploads;
                let data_val = data_val.map(Bytes::copy_from_slice);
                let Some(location_val) = location_val.map(ToString::to_string) else {
                    continue;
                };
                let io_client = self.io_client.clone();
                let handle = self.io_runtime_handle.spawn(async move {
                    let url = io_client
                        .single_url_upload(
                            submitted_uploads,
                            location_val,
                            data_val,
                            raise_error_on_failure,
                            None,
                        )
                        .await?;

                    Ok((submitted_uploads, url))
                });

                self.in_flight_uploads.spawn(async move { handle.await? });
                self.submitted_uploads += 1;
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
        let mut completed_urls = LargeStringBuilder::with_capacity(num_rows, num_rows);

        // Wait for uploads to complete until we are under the active limit
        while completed_idxs.len() < num_rows {
            let Some(result) = self.in_flight_uploads.join_next().await else {
                unreachable!("There should always be at least one upload in flight");
            };

            let (idx, contents) = result
                .expect("Failed to join upload")
                .expect("Failed to get upload result");

            if contents.is_none() {
                stats.inc_num_failed();
            }

            completed_idxs.push(idx as u64);
            completed_urls.append_option(contents);
        }

        // If any additional uploads are completed, pop them off the join set
        while let Some(result) = self.in_flight_uploads.try_join_next() {
            let (idx, contents) = result
                .expect("Failed to join upload")
                .expect("Failed to get upload result");

            if contents.is_none() {
                stats.inc_num_failed();
            }

            completed_idxs.push(idx as u64);
            completed_urls.append_option(contents);
        }

        let completed_urls = Utf8Array::from(("", completed_urls.finish())).into_series();
        Ok((completed_idxs, completed_urls))
    }
}

type UrlUploadSinkState = AsyncSinkState<UrlUploadOpState>;

pub struct UrlUploadSink {
    args: Arc<UrlUploadArgsDefault<BoundExpr>>,
    passthrough_columns: Vec<BoundColumn>,
    save_schema: SchemaRef,
    output_schema: SchemaRef,
    output_column: String,
    input_schema: SchemaRef,
    input_size_bytes_buffer: usize,
}

impl UrlUploadSink {
    pub fn new(
        args: UrlUploadArgs<BoundExpr>,
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

        save_fields.push(Field::new(output_column.as_str(), DataType::Utf8));
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

impl StreamingSink for UrlUploadSink {
    #[instrument(skip_all, name = "UrlUploadSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult {
        let input_schema = self.input_schema.clone();
        let builder = spawner.runtime_context.builder.clone();

        spawner
            .spawn(
                async move {
                    let url_state = state
                        .as_any_mut()
                        .downcast_mut::<UrlUploadSinkState>()
                        .expect("UrlUpload sink should have UrlUploadSinkState");

                    let stats = builder.as_any_arc();
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

    #[instrument(skip_all, name = "UrlUploadSink::finalize")]
    fn finalize(
        &self,
        mut states: Vec<Box<dyn StreamingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        let builder = spawner.runtime_context.builder.clone();

        spawner
            .spawn(
                async move {
                    assert!(states.len() == 1);
                    let state = states
                        .get_mut(0)
                        .expect("UrlUpload should have exactly one state")
                        .as_any_mut()
                        .downcast_mut::<UrlUploadSinkState>()
                        .expect("UrlUpload sink state should be UrlUploadSinkState");

                    let stats = builder.as_any_arc();
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
        "Url Upload".into()
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            "URL Upload".to_string(),
            format!("Input DataColumn: {}", self.args.input),
            format!("Output DataColumn: {}", self.output_column),
        ]
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        let inner_state = UrlUploadOpState::new(self.args.clone());

        Box::new(UrlUploadSinkState::new(
            self.passthrough_columns.clone(),
            self.save_schema.clone(),
            self.output_schema.clone(),
            self.output_column.clone(),
            self.input_size_bytes_buffer,
            inner_state,
        ))
    }

    fn make_runtime_stats_builder(&self) -> Arc<dyn RuntimeStatsBuilder> {
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
        // Limits are greedy, so we don't need to buffer any input.
        // They are also not concurrent, so we don't need to worry about ordering.
        Arc::new(UnorderedDispatcher::unbounded())
    }
}
