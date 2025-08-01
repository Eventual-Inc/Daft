use std::sync::Arc;

use arrow_array::builder::LargeBinaryBuilder;
use common_error::DaftResult;
use common_runtime::{get_io_runtime, RuntimeRef};
use daft_core::{
    prelude::{AsArrow, BinaryArray, BooleanArray, DataType, Field, Schema, SchemaRef},
    series::IntoSeries,
};
use daft_dsl::expr::{bound_expr::BoundExpr, BoundColumn};
use daft_functions_uri::{download::UrlDownloadArgsDefault, UrlDownloadArgs};
use daft_io::{get_io_client, IOClient};
use daft_micropartition::{partitioning::Partition, MicroPartition};
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
    passthrough_columns: Vec<BoundColumn>,
    output_schema: SchemaRef,
    output_column: String,
    // Max size of saved inputs before we start pruning rows
    input_size_bytes_buffer: usize,

    // Fixed state
    io_client: Arc<IOClient>,
    io_runtime_handle: RuntimeRef,

    // Updatable state
    submitted_downloads: usize,
    in_flight_downloads: JoinSet<DaftResult<(usize, Option<Bytes>)>>,
    // Save input columns to pass through to output
    saved_inputs: Arc<MicroPartition>,
    // True when row is not finished downloading.
    // Note: unfinished_inputs.len() == all_inputs.num_rows()
    unfinished_inputs: Vec<bool>,
    // Map idx from in_flight_downloads to row in all_inputs
    // Used to filter out finished rows from all_inputs while keeping in_flight_downloads in sync
    // Note in_flight_inputs_idx.len() == submitted_downloads
    in_flight_inputs_idx: Vec<usize>,
}

impl UrlDownloadSinkState {
    fn new(
        args: Arc<UrlDownloadArgsDefault<BoundExpr>>,
        passthrough_columns: Vec<BoundColumn>,
        save_schema: SchemaRef,
        output_schema: SchemaRef,
        output_column: String,
        input_size_bytes_buffer: usize,
    ) -> Self {
        let multi_thread = args.multi_thread;
        let io_config = args.io_config.clone();

        Self {
            args,
            passthrough_columns,
            output_schema,
            output_column,
            input_size_bytes_buffer,

            io_runtime_handle: get_io_runtime(true),
            io_client: get_io_client(multi_thread, io_config).expect("Failed to get IO client"),

            submitted_downloads: 0,
            in_flight_downloads: JoinSet::new(),
            saved_inputs: Arc::new(MicroPartition::empty(Some(save_schema))),
            unfinished_inputs: Vec::new(),
            in_flight_inputs_idx: Vec::new(),
        }
    }

    pub fn get_in_flight(&self) -> usize {
        self.in_flight_downloads.len()
    }

    fn max_in_flight(&self) -> usize {
        self.args.max_connections * IN_FLIGHT_SCALE_FACTOR
    }

    fn input_size(&self) -> usize {
        // Expect that all inputs are loaded
        self.saved_inputs.size_bytes().unwrap().unwrap_or(0)
    }

    /// Start downloads for given URLs
    pub fn start_download(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        if input.is_empty() {
            return Ok(());
        }

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

        // Add to row tracking data structures
        let curr_num_rows = self.saved_inputs.num_rows()?;
        self.in_flight_inputs_idx
            .extend(curr_num_rows..(curr_num_rows + input.num_rows()?));
        self.unfinished_inputs.extend(vec![true; input.num_rows()?]);
        self.saved_inputs = Arc::new(MicroPartition::concat([
            self.saved_inputs.clone(),
            Arc::new(input.select_columns(&self.passthrough_columns)?),
        ])?);

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
            completed_idxs.push(self.in_flight_inputs_idx[idx] as u64);
            completed_contents.append_option(contents);
        }
        // Mark finished rows as finished
        for idx in &completed_idxs {
            self.unfinished_inputs[*idx as usize] = false;
        }

        let completed_contents =
            BinaryArray::from((self.output_column.as_str(), completed_contents.finish()))
                .into_series();
        let output = build_output(
            self.saved_inputs.clone(),
            completed_idxs,
            completed_contents,
            self.output_schema.clone(),
        )?;

        // Mark finished rows and cleanup
        if self.input_size() > self.input_size_bytes_buffer {
            let unfinished_inputs =
                BooleanArray::from(("mask", self.unfinished_inputs.as_slice())).into_series();
            self.saved_inputs = Arc::new(self.saved_inputs.mask_filter(&unfinished_inputs)?);

            // Update the in_flight_inputs_idx with the new locations in all_inputs
            let mut cnt = 0;
            for (idx, val) in self.unfinished_inputs.iter().enumerate() {
                if *val {
                    self.in_flight_inputs_idx[idx] = cnt;
                    cnt += 1;
                }
            }
            self.unfinished_inputs = vec![true; self.saved_inputs.num_rows()?];
        }

        Ok(output)
    }
}

impl StreamingSinkState for UrlDownloadSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

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
            self.passthrough_columns.clone(),
            self.save_schema.clone(),
            self.output_schema.clone(),
            self.output_column.clone(),
            self.input_size_bytes_buffer,
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
