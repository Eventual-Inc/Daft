use std::sync::Arc;

use arrow_array::builder::LargeStringBuilder;
use common_error::DaftResult;
use common_runtime::{get_io_runtime, RuntimeRef};
use daft_core::{
    prelude::{AsArrow, DataType, Field, SchemaRef, Utf8Array},
    series::IntoSeries,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions_uri::{upload::UrlUploadArgsDefault, UrlUploadArgs};
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

struct UrlUploadSinkState {
    // Arguments
    args: Arc<UrlUploadArgsDefault<BoundExpr>>,
    output_schema: SchemaRef,
    output_column: String,

    // Fixed state
    io_client: Arc<IOClient>,
    io_runtime_handle: RuntimeRef,

    // Updatable state
    in_flight_uploads: JoinSet<DaftResult<(usize, Option<String>)>>,
    all_inputs: Arc<MicroPartition>,
    submitted_uploads: usize,
}

impl UrlUploadSinkState {
    fn new(
        args: Arc<UrlUploadArgsDefault<BoundExpr>>,
        output_column: String,
        input_schema: SchemaRef,
    ) -> Self {
        let multi_thread = args.multi_thread;
        let io_config = args.io_config.clone();

        // Generate output schema initially
        let mut output_schema = input_schema.as_ref().clone();
        output_schema.append(Field::new(output_column.as_str(), DataType::Utf8));

        Self {
            args,
            output_schema: Arc::new(output_schema),
            output_column,

            io_runtime_handle: get_io_runtime(multi_thread),
            io_client: get_io_client(multi_thread, io_config).expect("Failed to get IO client"),

            in_flight_uploads: JoinSet::new(),
            all_inputs: Arc::new(MicroPartition::empty(Some(input_schema))),
            submitted_uploads: 0,
        }
    }

    fn upload(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        if input.is_empty() {
            return Ok(());
        }

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

        self.all_inputs = Arc::new(MicroPartition::concat([self.all_inputs.clone(), input])?);
        Ok(())
    }

    async fn poll_finished(&mut self, finished: bool) -> DaftResult<RecordBatch> {
        let exp_capacity = if finished {
            std::cmp::min(self.in_flight_uploads.len(), 32)
        } else {
            0
        };

        let mut completed_idxs = Vec::with_capacity(exp_capacity);
        let mut completed_urls = LargeStringBuilder::with_capacity(exp_capacity, exp_capacity);

        // Wait for uploads to complete until we are under the active limit
        while completed_idxs.len() < exp_capacity {
            let Some(result) = self.in_flight_uploads.join_next().await else {
                unreachable!("There should always be at least one upload in flight");
            };

            let (idx, contents) = result
                .expect("Failed to join upload")
                .expect("Failed to get upload result");
            completed_idxs.push(idx as u64);
            completed_urls.append_option(contents);
        }

        // If any additional uploads are completed, pop them off the join set
        while let Some(result) = self.in_flight_uploads.try_join_next() {
            let (idx, contents) = result
                .expect("Failed to join upload")
                .expect("Failed to get upload result");
            completed_idxs.push(idx as u64);
            completed_urls.append_option(contents);
        }

        let completed_urls =
            Utf8Array::from((self.output_column.as_str(), completed_urls.finish())).into_series();

        build_output(
            self.all_inputs.clone(),
            completed_idxs,
            completed_urls,
            self.output_schema.clone(),
        )
    }
}

impl StreamingSinkState for UrlUploadSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct UrlUploadSink {
    args: Arc<UrlUploadArgsDefault<BoundExpr>>,
    output_column: String,
    input_schema: SchemaRef,
}

impl UrlUploadSink {
    pub fn new(
        args: UrlUploadArgs<BoundExpr>,
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

impl StreamingSink for UrlUploadSink {
    #[instrument(skip_all, name = "UrlUploadSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult {
        spawner
            .spawn(
                async move {
                    let url_state = state
                        .as_any_mut()
                        .downcast_mut::<UrlUploadSinkState>()
                        .expect("UrlUpload sink should have UrlUploadSinkState");

                    url_state.upload(input)?;
                    let output = url_state.poll_finished(false).await?;

                    let schema = output.schema.clone();
                    let output = MicroPartition::new_loaded(schema, Arc::new(vec![output]), None);

                    Ok((
                        state,
                        StreamingSinkOutput::NeedMoreInput(Some(Arc::new(output))),
                    ))
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

                    let output = state.poll_finished(true).await?;
                    let schema = output.schema.clone();
                    let output = Arc::new(MicroPartition::new_loaded(
                        schema,
                        Arc::new(vec![output]),
                        None,
                    ));

                    if !state.in_flight_uploads.is_empty() {
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
        Box::new(UrlUploadSinkState::new(
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
        // Limits are greedy, so we don't need to buffer any input.
        // They are also not concurrent, so we don't need to worry about ordering.
        Arc::new(UnorderedDispatcher::unbounded())
    }
}
