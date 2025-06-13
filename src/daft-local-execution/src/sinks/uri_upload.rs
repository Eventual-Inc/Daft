use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    prelude::{AsArrow, DataType, Field, SchemaRef, UInt64Array, Utf8Array},
    series::IntoSeries,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions_uri::UrlUploadArgs;
use daft_io::{get_io_client, IOClient};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tokio::task::JoinSet;
use tokio_util::bytes::Bytes;
use tracing::{instrument, Span};

use super::streaming_sink::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    StreamingSinkState,
};
use crate::{
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    sinks::streaming_sink::StreamingSinkFinalizeOutput,
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

struct UriUploadSinkState {
    in_flight_uploads: JoinSet<DaftResult<(usize, Option<String>)>>,
    all_inputs: Arc<MicroPartition>,
    io_client: Arc<IOClient>,
    submitted_uploads: usize,

    input_schema: SchemaRef,
    // max_in_flight: usize,
    data_col: BoundExpr,
    location_col: BoundExpr,
    _is_single_folder: bool,
    raise_error_on_failure: bool,
    output_column: String,
}

impl UriUploadSinkState {
    fn new(args: UrlUploadArgs<BoundExpr>, output_column: String, input_schema: SchemaRef) -> Self {
        let UrlUploadArgs {
            input,
            on_error,
            io_config,
            multi_thread,
            // max_connections,
            location,
            is_single_folder,
            ..
        } = args;

        let multi_thread = multi_thread.unwrap_or(true);
        let io_config = io_config.unwrap_or_default();
        // let max_connections = max_connections.unwrap_or(32);
        let _is_single_folder = is_single_folder.unwrap_or(false);

        let on_error = on_error.unwrap_or_else(|| "raise".to_string());
        let raise_error_on_failure = match on_error.as_str() {
            "raise" => true,
            "null" => false,
            _ => {
                panic!("Invalid value for 'on_error': {}", on_error)
            }
        };

        Self {
            in_flight_uploads: JoinSet::new(),
            all_inputs: Arc::new(MicroPartition::empty(Some(input_schema.clone()))),
            io_client: get_io_client(multi_thread, Arc::new(io_config)).unwrap(),
            submitted_uploads: 0,
            input_schema,

            // max_in_flight: max_connections * 4,
            data_col: input,
            location_col: location,
            _is_single_folder,
            raise_error_on_failure,
            output_column,
        }
    }

    fn upload(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        let raise_error_on_failure = self.raise_error_on_failure;

        for input in input.get_tables()?.iter() {
            let data_col = input.eval_expression(&self.data_col)?;
            let data_col = data_col.binary()?;
            let data_col = data_col.as_arrow();

            let location_col = input.eval_expression(&self.location_col)?;
            let location_col = location_col.utf8()?;
            let location_col = location_col.as_arrow();

            for (data_val, location_val) in data_col.iter().zip(location_col.iter()) {
                let submitted_uploads = self.submitted_uploads;
                let data_val = data_val.map(Bytes::copy_from_slice);
                let Some(location_val) = location_val.map(ToString::to_string) else {
                    continue;
                };
                let io_client = self.io_client.clone();

                self.in_flight_uploads.spawn(async move {
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

                self.submitted_uploads += 1;
            }
        }

        self.all_inputs = Arc::new(MicroPartition::concat([self.all_inputs.clone(), input])?);
        Ok(())
    }

    fn build_output(
        &self,
        completed_uploads: Vec<u64>,
        completed_urls: Vec<Option<String>>,
    ) -> DaftResult<RecordBatch> {
        if completed_uploads.is_empty() {
            let mut schema = self.input_schema.as_ref().clone();
            schema.append(Field::new(self.output_column.as_str(), DataType::Utf8));

            return RecordBatch::empty(Some(Arc::new(schema)));
        }

        let idxs = UInt64Array::from(("idxs", completed_uploads)).into_series();
        let original_rows = &self.all_inputs.take(&idxs)?.get_tables()?[0];

        let mut valid = Vec::with_capacity(completed_urls.len());
        valid.reserve(completed_urls.len());

        let cap_needed: usize = completed_urls
            .iter()
            .filter_map(|f| f.as_ref().map(String::len))
            .sum();
        let mut data = Vec::with_capacity(cap_needed);
        for b in completed_urls {
            if let Some(b) = b {
                data.push(b);
                valid.push(true);
            } else {
                valid.push(false);
            }
        }
        let contents = Utf8Array::from(("contents", data.as_slice()))
            .with_validity_slice(valid.as_slice())
            .unwrap()
            .into_series();

        Ok(original_rows.with_column(self.output_column.as_str(), contents))
    }

    async fn poll_finished(&mut self, finished: bool) -> DaftResult<RecordBatch> {
        let exp_capacity = if finished { 32 } else { 0 };

        let mut completed_idxs = Vec::with_capacity(exp_capacity);
        let mut completed_urls = Vec::with_capacity(exp_capacity);

        // Wait for uploads to complete until we are under the active limit
        while completed_idxs.len() < exp_capacity {
            let Some(result) = self.in_flight_uploads.join_next().await else {
                unreachable!("There should always be at least one upload in flight");
            };

            let (idx, contents) = result.unwrap().unwrap();
            completed_idxs.push(idx as u64);
            completed_urls.push(contents);
        }

        // If any additional uploads are completed, pop them off the join set
        while let Some(result) = self.in_flight_uploads.try_join_next() {
            let (idx, contents) = result.unwrap().unwrap();
            completed_idxs.push(idx as u64);
            completed_urls.push(contents);
        }

        self.build_output(completed_idxs, completed_urls)
    }
}

impl StreamingSinkState for UriUploadSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct UriUploadSink {
    args: UrlUploadArgs<BoundExpr>,
    output_column: String,
    input_schema: SchemaRef,
}

impl UriUploadSink {
    pub fn new(
        args: UrlUploadArgs<BoundExpr>,
        output_column: String,
        input_schema: SchemaRef,
    ) -> Self {
        Self {
            args,
            output_column,
            input_schema,
        }
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
        spawner
            .spawn(
                async move {
                    let url_state = state
                        .as_any_mut()
                        .downcast_mut::<UriUploadSinkState>()
                        .expect("UriUpload sink should have UriUploadSinkState");

                    url_state.upload(input)?;
                    let output = url_state.poll_finished(false).await?;

                    let schema = output.schema.clone();
                    let output = MicroPartition::new_loaded(schema, Arc::new(vec![output]), None);

                    let next_input = Arc::new(MicroPartition::empty(None));
                    Ok((
                        state,
                        StreamingSinkOutput::HasMoreOutput {
                            next_input,
                            output: Arc::new(output),
                        },
                    ))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "UriUploadSink::finalize")]
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
                        .unwrap()
                        .as_any_mut()
                        .downcast_mut::<UriUploadSinkState>()
                        .expect("UriUpload sink should have UriUploadSinkState");

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

    fn name(&self) -> &'static str {
        "UriUpload"
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            "URL Upload".to_string(),
            format!("Input DataColumn: {}", self.args.input),
            format!("Output DataColumn: {}", self.output_column),
        ]
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(UriUploadSinkState::new(
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
