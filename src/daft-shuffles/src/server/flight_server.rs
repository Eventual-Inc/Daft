use std::{
    collections::HashMap,
    io::SeekFrom,
    pin::Pin,
    sync::{Arc, Mutex},
};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    decode::FlightRecordBatchStream,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
};
use arrow_ipc::writer::IpcWriteOptions;
use common_error::{DaftError, DaftResult};
use common_runtime::RuntimeTask;
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt, stream::BoxStream};
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tonic::{Request, Response, Status, transport::Server};

use super::stream::FlightDataStreamReader;
use crate::{
    client::flight_client::FlightRecordBatchStreamToDaftRecordBatchStream,
    shuffle_cache::PartitionCache,
};

/// A `do_get` request: which shuffle, and which `(attempt, partition_ref_id)`
/// pairs. Refs are addressed together with the attempt that produced them so a
/// registration left behind by a superseded attempt of the same task can never
/// satisfy a request meant for the attempt the coordinator selected.
struct ParsedTicket {
    shuffle_id: u64,
    refs: Vec<(u64, u64)>,
}

impl ParsedTicket {
    /// Ticket format: `"{shuffle_id}:{attempt_hex}={ref},{ref};{attempt_hex}={ref}"`.
    /// Refs are grouped by attempt because a reducer's request typically spans
    /// many map inputs (each its own attempt) but one partition per input.
    fn from_ticket(ticket: &Ticket) -> Result<Self, Status> {
        let ticket_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let Some((shuffle_part, groups_part)) = ticket_str.split_once(':') else {
            return Err(Status::invalid_argument(
                "Invalid ticket format. Expected 'shuffle_id:attempt=refs;attempt=refs'",
            ));
        };

        let shuffle_id = shuffle_part
            .parse::<u64>()
            .map_err(|e| Status::invalid_argument(format!("Invalid shuffle id: {}", e)))?;

        let mut refs = Vec::new();
        for group in groups_part.split(';').filter(|g| !g.is_empty()) {
            let Some((attempt_part, refs_part)) = group.split_once('=') else {
                return Err(Status::invalid_argument(format!(
                    "Invalid ticket group '{}'. Expected 'attempt=ref,ref'",
                    group
                )));
            };
            let attempt = u64::from_str_radix(attempt_part, 16)
                .map_err(|e| Status::invalid_argument(format!("Invalid attempt: {}", e)))?;
            for id in refs_part.split(',').filter(|id| !id.is_empty()) {
                let ref_id = id.parse::<u64>().map_err(|e| {
                    Status::invalid_argument(format!("Invalid partition ref id: {}", e))
                })?;
                refs.push((attempt, ref_id));
            }
        }

        Ok(Self { shuffle_id, refs })
    }
}

/// Encode a request in the form [`ParsedTicket::from_ticket`] reads.
pub fn encode_ticket(shuffle_id: u64, refs: &[(u64, u64)]) -> String {
    let mut by_attempt: std::collections::BTreeMap<u64, Vec<u64>> =
        std::collections::BTreeMap::new();
    for (attempt, ref_id) in refs {
        by_attempt.entry(*attempt).or_default().push(*ref_id);
    }
    let groups = by_attempt
        .into_iter()
        .map(|(attempt, ids)| {
            let ids = ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",");
            format!("{:x}={}", attempt, ids)
        })
        .collect::<Vec<_>>()
        .join(";");
    format!("{}:{}", shuffle_id, groups)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct FlightPartitionKey {
    shuffle_id: u64,
    attempt: u64,
    partition_ref_id: u64,
}

/// How to read one file's contribution to a Flight response.
enum FileReadSpec {
    /// Read the entire IPC stream file (per-partition cache).
    Whole { path: String },
    /// Read one or more `(start, end)` ranges from a single file (combined-file shuffle).
    Ranges {
        path: String,
        ranges: Vec<(u64, u64)>,
    },
}

/// Every output partition this worker has written and can still serve.
///
/// A plain `std::sync::Mutex` rather than an async one: nothing under this lock
/// awaits — registration inserts, lookup resolves paths and ranges and hands
/// them back — so an async mutex would only add scheduling to an uncontended
/// critical section, and a synchronous one can also be taken from the Python
/// thread that drops a finished shuffle's entries.
type PartitionRegistry = Mutex<HashMap<FlightPartitionKey, PartitionCache>>;

#[derive(Clone, Default)]
pub struct ShuffleFlightServer {
    shuffle_partitions: Arc<PartitionRegistry>,
}

impl ShuffleFlightServer {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock_partitions(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<FlightPartitionKey, PartitionCache>> {
        self.shuffle_partitions
            .lock()
            .expect("shuffle partition registry poisoned")
    }

    /// Forget every registration belonging to `shuffle_ids`.
    ///
    /// Registrations are per map task attempt and never expire on their own, so
    /// without this a worker's registry is a monotonically growing map: one entry
    /// per output partition per attempt per query, each holding a schema, the file
    /// paths and the byte ranges. That is memory the worker keeps for the rest of
    /// its life, and — worse on the local-placement path — it keeps refs from
    /// finished queries answerable long after the files behind them are gone.
    ///
    /// Called when the query that owns the shuffle drops its spill trees, so the
    /// registry and the files it points at disappear together. Also drops the
    /// directory memo for those shuffles, whose paths have just been removed.
    ///
    /// Returns how many entries were dropped, which is what the caller logs; a
    /// worker that took no part in a shuffle correctly reports zero.
    pub fn unregister_shuffles(&self, shuffle_ids: &[u64]) -> usize {
        if shuffle_ids.is_empty() {
            return 0;
        }
        let dropped = {
            let mut partitions = self.lock_partitions();
            let before = partitions.len();
            partitions.retain(|key, _| !shuffle_ids.contains(&key.shuffle_id));
            before - partitions.len()
        };
        for shuffle_id in shuffle_ids {
            crate::store::forget_created_dirs(*shuffle_id);
        }
        dropped
    }

    /// Register one attempt's output partitions.
    ///
    /// Keyed by attempt as well as ref: two attempts of one map task can both
    /// run to completion in this process, and a reader that asks for one must
    /// not be served the other.
    pub fn register_shuffle_partitions(
        &self,
        shuffle_id: u64,
        attempt: u64,
        partitions: Vec<PartitionCache>,
    ) -> DaftResult<()> {
        let mut shuffle_partitions = self.lock_partitions();
        for partition in partitions {
            shuffle_partitions.insert(
                FlightPartitionKey {
                    shuffle_id,
                    attempt,
                    partition_ref_id: partition.partition_ref_id,
                },
                partition,
            );
        }
        Ok(())
    }

    /// Resolve every requested `(attempt, ref)` to file reads.
    ///
    /// All-or-nothing: if any ref is not registered here, the whole request is
    /// refused with the missing refs (`Err`). Serving the ones that are present
    /// would hand the caller a stream that looks complete and is not — the caller
    /// has no way to tell which inputs were skipped. Refusing lets it fall back
    /// to shared storage or fail loudly.
    fn get_shuffle_file_specs(
        &self,
        shuffle_id: u64,
        refs: &[(u64, u64)],
    ) -> Result<(Vec<FileReadSpec>, SchemaRef), Vec<(u64, u64)>> {
        let partitions = self.lock_partitions();

        let mut missing = Vec::new();
        let mut schema: Option<SchemaRef> = None;
        let mut caches = Vec::with_capacity(refs.len());
        for (attempt, partition_ref_id) in refs {
            match partitions.get(&FlightPartitionKey {
                shuffle_id,
                attempt: *attempt,
                partition_ref_id: *partition_ref_id,
            }) {
                Some(cache) => {
                    schema.get_or_insert_with(|| cache.schema.clone());
                    caches.push(cache);
                }
                None => missing.push((*attempt, *partition_ref_id)),
            }
        }
        if !missing.is_empty() {
            return Err(missing);
        }
        let Some(schema) = schema else {
            // An empty request has nothing to serve and no schema to serve it with.
            return Err(Vec::new());
        };

        // Group ranged reads by file path so each physical file is read from a single FD.
        let mut specs: Vec<FileReadSpec> = Vec::new();
        let mut ranges_by_path: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for cache in caches {
            match &cache.byte_ranges {
                Some(ranges) => {
                    for (path, (start, end)) in cache.file_paths.iter().zip(ranges.iter()) {
                        let entry = ranges_by_path.entry(path.clone()).or_insert_with(|| {
                            order.push(path.clone());
                            Vec::new()
                        });
                        entry.push((*start, *end));
                    }
                }
                None => {
                    for path in &cache.file_paths {
                        specs.push(FileReadSpec::Whole { path: path.clone() });
                    }
                }
            }
        }

        for path in order {
            let mut ranges = ranges_by_path.remove(&path).unwrap_or_default();
            // Sort by start so sequential reads stay forward-going (kind to readahead).
            ranges.sort_unstable_by_key(|r| r.0);
            specs.push(FileReadSpec::Ranges { path, ranges });
        }

        Ok((specs, schema))
    }

    /// Get partition data in-process (no gRPC). Returns a stream of Daft RecordBatches.
    /// Used when the reader runs on the same node as the shuffle server.
    pub async fn get_partition_local(
        &self,
        shuffle_id: u64,
        refs: &[(u64, u64)],
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let (specs, schema) = self
            .get_shuffle_file_specs(shuffle_id, refs)
            .map_err(|missing| {
                DaftError::ValueError(format!(
                    "Shuffle partitions not registered on this worker for shuffle {}: (attempt, ref) {:?}",
                    shuffle_id, missing
                ))
            })?;

        let spec_stream = futures::stream::iter(specs);
        let flight_data_stream = spec_stream
            .then(move |spec| {
                let schema = schema.clone();
                async move {
                    let inner_stream = open_spec_as_flight_stream(spec)
                        .map_err(|e| FlightError::from_external_error(Box::new(e)));

                    let arrow_schema = schema.to_arrow().map_err(|e| {
                        DaftError::InternalError(format!("Error converting schema to arrow: {}", e))
                    })?;
                    let options = IpcWriteOptions::default();
                    let flight_schema = SchemaAsIpc::new(&arrow_schema, &options).into();
                    let flight_data =
                        futures::stream::once(async { Ok(flight_schema) }).chain(inner_stream);

                    // Doing some shenanigans here to reuse existing code
                    // TODO: Refactor this to get Arrow RecordBatchStream directly using async IO
                    let arrow_stream = FlightRecordBatchStream::new_from_flight_data(flight_data);
                    let daft_stream =
                        FlightRecordBatchStreamToDaftRecordBatchStream::new(arrow_stream, schema);
                    Ok::<_, DaftError>(daft_stream)
                }
            })
            .try_flatten();

        Ok(Box::pin(flight_data_stream))
    }
}

#[tonic::async_trait]
impl FlightService for ShuffleFlightServer {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!("Handshake is not supported for shuffle server")
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        unimplemented!("List flights is not supported for shuffle server")
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        unimplemented!("Get flight info is not supported for shuffle server")
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        unimplemented!("Poll flight info is not supported for shuffle server")
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        unimplemented!("Get schema is not supported for shuffle server")
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let ticket = ParsedTicket::from_ticket(&ticket)?;

        let (specs, schema) = self
            .get_shuffle_file_specs(ticket.shuffle_id, &ticket.refs)
            .map_err(|missing| {
                Status::not_found(format!(
                    "Shuffle partitions not registered for shuffle {}: (attempt, ref) {:?}",
                    ticket.shuffle_id, missing
                ))
            })?;

        let arrow_schema = schema
            .to_arrow()
            .map_err(|e| Status::internal(format!("schema to arrow: {}", e)))?;
        let flight_schema = SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default()).into();

        let data_stream = futures::stream::iter(specs)
            .flat_map(open_spec_as_flight_stream)
            .map_err(|e| Status::internal(format!("flight stream: {}", e)));
        let flight_data = futures::stream::once(async { Ok(flight_schema) }).chain(data_stream);
        Ok(Response::new(Box::pin(flight_data)))
    }

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        unimplemented!("Do put is not supported for shuffle server")
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented!("Do exchange is not supported for shuffle server")
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        unimplemented!("Do action is not supported for shuffle server")
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!("List actions is not supported for shuffle server")
    }
}

fn open_spec_as_flight_stream(spec: FileReadSpec) -> BoxStream<'static, DaftResult<FlightData>> {
    Box::pin(async_stream::try_stream! {
        match spec {
            FileReadSpec::Whole { path } => {
                let file = tokio::fs::File::open(&path).await.map_err(DaftError::IoError)?;
                let reader = FlightDataStreamReader::try_new(BufReader::new(file)).await?;
                let inner = reader.into_stream();
                futures::pin_mut!(inner);
                while let Some(item) = inner.next().await {
                    yield item?;
                }
            }
            FileReadSpec::Ranges { path, ranges } => {
                let mut file = tokio::fs::File::open(&path).await.map_err(DaftError::IoError)?;
                for (start, end) in ranges {
                    file.seek(SeekFrom::Start(start)).await.map_err(DaftError::IoError)?;
                    let limited = (&mut file).take(end - start);
                    let reader = FlightDataStreamReader::from_skipped(BufReader::new(limited));
                    let inner = reader.into_stream();
                    futures::pin_mut!(inner);
                    while let Some(item) = inner.next().await {
                        yield item?;
                    }
                }
            }
        }
    })
}

pub struct FlightServerConnectionHandle {
    ip: String,
    port: u16,
    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
    server_task: Option<RuntimeTask<DaftResult<()>>>,
}

impl FlightServerConnectionHandle {
    pub fn shutdown(&mut self) -> DaftResult<()> {
        let Some(shutdown_signal) = self.shutdown_signal.take() else {
            return Ok(());
        };
        let _ = shutdown_signal.send(());
        let Some(server_task) = self.server_task.take() else {
            return Ok(());
        };
        common_runtime::get_io_runtime(true).block_on_current_thread(server_task)??;
        Ok(())
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn shuffle_address(&self) -> String {
        format!("grpc://{}:{}", self.ip, self.port)
    }
}

pub fn start_server_loop(
    ip: &str,
    server: Arc<ShuffleFlightServer>,
) -> FlightServerConnectionHandle {
    let io_runtime = common_runtime::get_io_runtime(true);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (port_tx, port_rx) = tokio::sync::oneshot::channel();

    let addr = format!("{}:0", ip);
    let server_task = io_runtime.spawn(async {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("Failed to bind to port");

        let port = listener
            .local_addr()
            .expect("Failed to get local address")
            .port();

        port_tx.send(port).expect("Failed to send port");

        let incoming = tonic::transport::server::TcpIncoming::from(listener)
            .with_nodelay(Some(true))
            .with_keepalive(None);

        let flight_server = server;
        Server::builder()
            .add_service(FlightServiceServer::from_arc(flight_server))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|e| DaftError::InternalError(format!("Error serving flight server: {}", e)))?;

        Ok(())
    });

    let port = port_rx.blocking_recv().expect("Failed to receive port");

    FlightServerConnectionHandle {
        ip: ip.to_string(),
        port,
        shutdown_signal: Some(shutdown_tx),
        server_task: Some(server_task),
    }
}

#[cfg(test)]
mod tests {
    use daft_schema::{dtype::DataType, field::Field, schema::Schema};

    use super::*;

    fn cache(partition_ref_id: u64) -> PartitionCache {
        PartitionCache {
            partition_ref_id,
            schema: Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)])),
            bytes_per_file: vec![4],
            file_paths: vec!["/tmp/does-not-need-to-exist".to_string()],
            num_rows: 1,
            size_bytes: 4,
            byte_ranges: Some(vec![(0, 4)]),
        }
    }

    #[test]
    fn unregistering_drops_only_the_named_shuffles() {
        let server = ShuffleFlightServer::new();
        server
            .register_shuffle_partitions(1, 0xaa, vec![cache(10), cache(11)])
            .unwrap();
        // A superseded attempt of the same shuffle leaves entries behind too.
        server
            .register_shuffle_partitions(1, 0xbb, vec![cache(10)])
            .unwrap();
        server
            .register_shuffle_partitions(2, 0xcc, vec![cache(20)])
            .unwrap();

        assert_eq!(server.unregister_shuffles(&[1]), 3);
        assert!(server.get_shuffle_file_specs(1, &[(0xaa, 10)]).is_err());
        // The shuffle that is still running is untouched.
        assert!(server.get_shuffle_file_specs(2, &[(0xcc, 20)]).is_ok());

        // Idempotent: a second cleanup pass, or a worker that held nothing, is a no-op.
        assert_eq!(server.unregister_shuffles(&[1]), 0);
        assert_eq!(server.unregister_shuffles(&[]), 0);
        assert_eq!(server.unregister_shuffles(&[2]), 1);
    }
}
