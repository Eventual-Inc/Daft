use std::{collections::HashMap, io::SeekFrom, pin::Pin, sync::Arc};

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
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
    sync::Mutex,
};
use tonic::{Request, Response, Status, transport::Server};

use super::stream::FlightDataStreamReader;
use crate::{
    client::flight_client::FlightRecordBatchStreamToDaftRecordBatchStream,
    shuffle_cache::PartitionCache,
};

struct ParsedTicket {
    shuffle_id: u64,
    partition_ref_ids: Vec<u64>,
}

impl ParsedTicket {
    fn from_ticket(ticket: &Ticket) -> Result<Self, Status> {
        let ticket_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Ticket format: "shuffle_id:partition_ref_ids" where partition_ref_ids is comma-separated list of u64s
        let parts: Vec<&str> = ticket_str.splitn(2, ':').collect();
        if parts.len() < 2 {
            return Err(Status::invalid_argument(
                "Invalid ticket format. Expected 'shuffle_id:partition_ref_ids'",
            ));
        }

        let shuffle_id = parts[0]
            .parse::<u64>()
            .map_err(|e| Status::invalid_argument(format!("Invalid shuffle id: {}", e)))?;
        let partition_ref_ids = parts[1]
            .split(',')
            .filter(|id| !id.is_empty())
            .map(|id| {
                id.parse::<u64>().map_err(|e| {
                    Status::invalid_argument(format!("Invalid partition ref id: {}", e))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            shuffle_id,
            partition_ref_ids,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct FlightPartitionKey {
    shuffle_id: u64,
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

#[derive(Clone, Default)]
pub struct ShuffleFlightServer {
    shuffle_partitions: Arc<Mutex<HashMap<FlightPartitionKey, PartitionCache>>>,
}

impl ShuffleFlightServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_shuffle_partitions(
        &self,
        shuffle_id: u64,
        partitions: Vec<PartitionCache>,
    ) -> DaftResult<()> {
        let mut shuffle_partitions = self.shuffle_partitions.lock().await;
        for partition in partitions {
            shuffle_partitions.insert(
                FlightPartitionKey {
                    shuffle_id,
                    partition_ref_id: partition.partition_ref_id,
                },
                partition,
            );
        }
        Ok(())
    }

    async fn get_shuffle_file_specs(
        &self,
        shuffle_id: u64,
        partition_ref_ids: &[u64],
    ) -> Option<(Vec<FileReadSpec>, SchemaRef)> {
        let partitions = self.shuffle_partitions.lock().await;

        let schema = partitions
            .get(&FlightPartitionKey {
                shuffle_id,
                partition_ref_id: *partition_ref_ids
                    .first()
                    .expect("Expected at least one partition"),
            })
            .expect("No partitions found")
            .schema
            .clone();

        // Group ranged reads by file path so each physical file is read from a single FD.
        let mut specs: Vec<FileReadSpec> = Vec::new();
        let mut ranges_by_path: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for partition_ref_id in partition_ref_ids {
            let Some(cache) = partitions.get(&FlightPartitionKey {
                shuffle_id,
                partition_ref_id: *partition_ref_id,
            }) else {
                continue;
            };
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

        Some((specs, schema))
    }

    /// Get partition data in-process (no gRPC). Returns a stream of Daft RecordBatches.
    /// Used when the reader runs on the same node as the shuffle server.
    pub async fn get_partition_local(
        &self,
        shuffle_id: u64,
        partition_ref_ids: &[u64],
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let (specs, schema) = self
            .get_shuffle_file_specs(shuffle_id, partition_ref_ids)
            .await
            .ok_or_else(|| {
                DaftError::ValueError(format!(
                    "Shuffle partitions not found for shuffle {} refs {:?}",
                    shuffle_id, partition_ref_ids
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
            .get_shuffle_file_specs(ticket.shuffle_id, &ticket.partition_ref_ids)
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Shuffle partitions not found for shuffle {} refs {:?}",
                    ticket.shuffle_id, ticket.partition_ref_ids
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
