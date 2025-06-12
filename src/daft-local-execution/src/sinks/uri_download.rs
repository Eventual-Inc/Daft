use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    prelude::{AsArrow, BinaryArray, DataType, Field, SchemaRef, UInt64Array},
    series::IntoSeries,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions_uri::UrlDownloadArgs;
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
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

const SCALE_FACTOR: usize = 8;

struct UriDownloadSinkState {
    in_flight_uploads: JoinSet<DaftResult<(usize, Option<Bytes>)>>,
    all_inputs: Arc<MicroPartition>,
    io_client: Arc<IOClient>,
    submitted_downloads: usize,

    input_schema: SchemaRef,
    max_in_flight: usize,
    uri_col: BoundExpr,
    raise_error_on_failure: bool,
    output_column: String,
}

impl UriDownloadSinkState {
    fn new(
        args: UrlDownloadArgs<BoundExpr>,
        output_column: String,
        input_schema: SchemaRef,
    ) -> Self {
        let UrlDownloadArgs {
            input,
            on_error,
            io_config,
            multi_thread,
            max_connections,
        } = args;

        let multi_thread = multi_thread.unwrap_or(true);
        let io_config = io_config.unwrap_or_default();
        let max_connections = max_connections.unwrap_or(32);

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
            input_schema,

            io_client: get_io_client(multi_thread, Arc::new(io_config)).unwrap(),
            submitted_downloads: 0,

            max_in_flight: max_connections * SCALE_FACTOR,
            uri_col: input,
            raise_error_on_failure,
            output_column,
        }
    }

    pub fn get_in_flight(&self) -> usize {
        self.in_flight_uploads.len()
    }

    fn download(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        let raise_error_on_failure = self.raise_error_on_failure;

        for input in input.get_tables()?.iter() {
            let uri_col = input.eval_expression(&self.uri_col)?;
            let uri_col = uri_col.utf8()?;
            let uri_col = uri_col.as_arrow();

            for uri_val in uri_col {
                let submitted_downloads = self.submitted_downloads;
                let uri_val = uri_val.map(ToString::to_string);
                let io_client = self.io_client.clone();

                self.in_flight_uploads.spawn(async move {
                    let contents = io_client
                        .single_url_download(
                            submitted_downloads,
                            uri_val,
                            raise_error_on_failure,
                            None,
                        )
                        .await?;

                    Ok((submitted_downloads, contents))
                });

                self.submitted_downloads += 1;
            }
        }

        self.all_inputs = Arc::new(MicroPartition::concat([self.all_inputs.clone(), input])?);
        Ok(())
    }

    fn build_output(
        &self,
        completed_downloads: Vec<u64>,
        completed_contents: Vec<Option<Bytes>>,
    ) -> DaftResult<RecordBatch> {
        if completed_downloads.is_empty() {
            let mut schema = self.input_schema.as_ref().clone();
            schema.append(Field::new(self.output_column.as_str(), DataType::Binary));

            return RecordBatch::empty(Some(Arc::new(schema)));
        }

        let idxs = UInt64Array::from(("idxs", completed_downloads)).into_series();
        let original_rows = &self.all_inputs.take(&idxs)?.get_tables()?[0];

        let mut offsets: Vec<i64> = Vec::with_capacity(completed_contents.len() + 1);
        offsets.push(0);
        let mut valid = Vec::with_capacity(completed_contents.len());
        valid.reserve(completed_contents.len());

        let cap_needed: usize = completed_contents
            .iter()
            .filter_map(|f| f.as_ref().map(Bytes::len))
            .sum();
        let mut data = Vec::with_capacity(cap_needed);
        for b in completed_contents {
            if let Some(b) = b {
                data.extend(b.as_ref());
                offsets.push(b.len() as i64 + offsets.last().unwrap());
                valid.push(true);
            } else {
                offsets.push(*offsets.last().unwrap());
                valid.push(false);
            }
        }
        let contents = BinaryArray::try_from((self.output_column.as_str(), data, offsets))?
            .with_validity_slice(valid.as_slice())
            .unwrap()
            .into_series();

        Ok(original_rows.with_column(self.output_column.as_str(), contents))
    }

    async fn poll_finished(&mut self) -> DaftResult<RecordBatch> {
        let exp_capacity = if self.in_flight_uploads.len() > self.max_in_flight {
            self.in_flight_uploads.len() - self.max_in_flight
        } else {
            0
        };

        let mut completed_downloads = Vec::with_capacity(exp_capacity);
        let mut completed_contents = Vec::with_capacity(exp_capacity);

        // Wait for downloads to complete until we are under the active limit
        while self.in_flight_uploads.len() > self.max_in_flight {
            let Some(result) = self.in_flight_uploads.join_next().await else {
                unreachable!("There should always be at least one upload in flight");
            };

            let (idx, contents) = result.unwrap().unwrap();
            completed_downloads.push(idx as u64);
            completed_contents.push(contents);
        }

        // If any additional uploads are completed, pop them off the join set
        while let Some(result) = self.in_flight_uploads.try_join_next() {
            let (idx, contents) = result.unwrap().unwrap();
            completed_downloads.push(idx as u64);
            completed_contents.push(contents);
        }

        self.build_output(completed_downloads, completed_contents)
    }

    async fn finish_all(&mut self) -> DaftResult<RecordBatch> {
        let in_flight_uploads = std::mem::take(&mut self.in_flight_uploads);

        let results = in_flight_uploads
            .join_all()
            .await
            .into_iter()
            .map(|x| x.unwrap())
            .collect::<Vec<_>>();
        let completed_downloads = results.iter().map(|x| x.0 as u64).collect::<Vec<_>>();
        let completed_contents = results.iter().map(|x| x.1.clone()).collect::<Vec<_>>();

        self.build_output(completed_downloads, completed_contents)
    }
}

impl StreamingSinkState for UriDownloadSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct UriDownloadSink {
    args: UrlDownloadArgs<BoundExpr>,
    output_column: String,
    input_schema: SchemaRef,
}

impl UriDownloadSink {
    pub fn new(
        args: UrlDownloadArgs<BoundExpr>,
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

impl StreamingSink for UriDownloadSink {
    #[instrument(skip_all, name = "UriDownloadSink::sink")]
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
                        .downcast_mut::<UriDownloadSinkState>()
                        .expect("UriDownload sink should have UriDownloadSinkState");

                    url_state.download(input)?;
                    let output = url_state.poll_finished().await?;

                    let schema = output.schema.clone();
                    let output = Arc::new(MicroPartition::new_loaded(
                        schema,
                        Arc::new(vec![output]),
                        None,
                    ));

                    if url_state.get_in_flight() <= url_state.max_in_flight {
                        Ok((state, StreamingSinkOutput::NeedMoreInput(Some(output))))
                    } else {
                        Ok((state, StreamingSinkOutput::HasMoreOutput(output)))
                    }
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "UriDownloadSink::finalize")]
    fn finalize(
        &self,
        mut states: Vec<Box<dyn StreamingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        spawner
            .spawn(
                async move {
                    let results = states
                        .iter_mut()
                        .map(|state| {
                            state
                                .as_any_mut()
                                .downcast_mut::<UriDownloadSinkState>()
                                .expect("UriDownload sink should have UriDownloadSinkState")
                                .finish_all()
                        })
                        .collect::<Vec<_>>();

                    let results = futures::future::join_all(results)
                        .await
                        .into_iter()
                        .map(|res| res.unwrap())
                        .collect::<Vec<_>>();

                    Ok(Some(Arc::new(MicroPartition::new_loaded(
                        results[0].schema.clone(),
                        Arc::new(results),
                        None,
                    ))))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "UriDownload"
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            "URL Download".to_string(),
            format!("Input URL Column: {}", self.args.input),
            format!("Output DataColumn: {}", self.output_column),
        ]
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(UriDownloadSinkState::new(
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
        Arc::new(UnorderedDispatcher::new(0, 32 * SCALE_FACTOR))
    }
}
