use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_runtime::get_compute_pool_num_threads;
use daft_core::{
    prelude::{AsArrow, BinaryArray, UInt64Array},
    series::IntoSeries,
};
use daft_dsl::{expr::bound_expr::BoundExpr, ExprRef};
use daft_functions_uri::{UrlDownloadArgs, UrlUploadArgs};
use daft_io::{get_io_client, IOClient, IOConfig, IOStatsContext};
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

struct UriDownloadSinkState {
    in_flight_uploads: JoinSet<DaftResult<(usize, Option<Bytes>)>>,
    all_inputs: Arc<MicroPartition>,

    io_client: Arc<IOClient>,
    submitted_downloads: usize,
    max_in_flight: usize,
    uri_col: BoundExpr,
    raise_error_on_failure: bool,
}

impl UriDownloadSinkState {
    fn new(args: UrlDownloadArgs<BoundExpr>) -> Self {
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
            all_inputs: Arc::new(MicroPartition::empty(None)),

            io_client: get_io_client(multi_thread, Arc::new(io_config)).unwrap(),
            submitted_downloads: 0,
            max_in_flight: max_connections * 4,
            uri_col: input,
            raise_error_on_failure,
        }
    }

    fn upload(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        let raise_error_on_failure = self.raise_error_on_failure;

        for input in input.get_tables()?.iter() {
            let uri_col = input.eval_expression(&self.uri_col)?;
            let uri_col = uri_col.utf8()?;
            let uri_col = uri_col.as_arrow();

            for uri_val in uri_col.iter() {
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
        let contents = BinaryArray::try_from(("contents", data, offsets))?
            .with_validity_slice(valid.as_slice())
            .unwrap()
            .into_series();

        Ok(original_rows.with_column("contents", contents))
    }

    async fn poll_finished(&mut self) -> DaftResult<RecordBatch> {
        let exp_capacity = if self.in_flight_uploads.len() > self.max_in_flight {
            self.in_flight_uploads.len() - self.max_in_flight
        } else {
            0
        };

        let mut completed_downloads = Vec::with_capacity(exp_capacity);
        let mut completed_contents = Vec::with_capacity(exp_capacity);

        // Wait for uploads to complete until we are under the active limit
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
}

impl UriDownloadSink {
    pub fn new(args: UrlDownloadArgs<BoundExpr>) -> DaftResult<Self> {
        Ok(Self { args })
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

                    url_state.upload(input)?;
                    let output = url_state.poll_finished().await?;

                    let schema = output.schema.clone();
                    let output = MicroPartition::new_loaded(schema, Arc::new(vec![output]), None);

                    Ok((state, StreamingSinkOutput::HasMoreOutput(Arc::new(output))))
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
        vec![format!("UriDownload")]
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(UriDownloadSinkState::new(self.args.clone()))
    }

    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn dispatch_spawner(
        &self,
        _runtime_handle: &ExecutionRuntimeContext,
        _maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        Arc::new(UnorderedDispatcher::unbounded())
    }
}
