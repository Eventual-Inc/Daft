use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    flight_service_server::{FlightService, FlightServiceServer},
};
use arrow_ipc::{reader::FileReader, writer::IpcWriteOptions};
use common_error::{DaftError, DaftResult};
use common_runtime::RuntimeTask;
use daft_recordbatch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::{io::BufReader, sync::Mutex};
use tonic::{Request, Response, Status, transport::Server};

use super::stream::FlightDataStreamReader;
use crate::shuffle_cache::{InProgressShuffleCache, ShuffleCache};

struct ParsedTicket {
    shuffle_id: u64,
    partition_idx: usize,
    _cache_ids: Option<Vec<u32>>,
}

impl ParsedTicket {
    fn from_ticket(ticket: &Ticket) -> Result<Self, Status> {
        let ticket_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Ticket format: "shuffle_id:partition_idx:cache_ids" where cache_ids is comma-separated list of u32s
        let parts: Vec<&str> = ticket_str.splitn(3, ':').collect();
        if parts.len() < 2 {
            return Err(Status::invalid_argument(
                "Invalid ticket format. Expected 'shuffle_id:partition_idx' or 'shuffle_id:partition_idx:cache_ids'",
            ));
        }

        let shuffle_id = parts[0]
            .parse::<u64>()
            .map_err(|e| Status::invalid_argument(format!("Invalid shuffle id: {}", e)))?;
        let partition_idx = parts[1]
            .parse::<usize>()
            .map_err(|e| Status::invalid_argument(format!("Invalid partition index: {}", e)))?;

        // Parse cache_ids if provided (third part of ticket)
        let cache_ids: Option<Vec<u32>> = if parts.len() == 3 && !parts[2].is_empty() {
            let ids = parts[2]
                .split(',')
                .map(|id| id.parse::<u32>())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| Status::invalid_argument(format!("Invalid cache id: {}", e)))?;
            Some(ids)
        } else {
            None
        };

        Ok(Self {
            shuffle_id,
            partition_idx,
            _cache_ids: cache_ids,
        })
    }
}

struct InProgressShuffleEntry {
    cache: Arc<InProgressShuffleCache>,
    active_cache_ids: HashSet<String>,
}

#[derive(Clone)]
pub struct ShuffleFlightServer {
    pub ip_address: String,
    shuffle_caches: Arc<Mutex<HashMap<u64, Vec<Arc<ShuffleCache>>>>>,
    in_progress_shuffle_caches: Arc<std::sync::Mutex<HashMap<u64, InProgressShuffleEntry>>>,
}

impl ShuffleFlightServer {
    #[must_use]
    pub fn new(ip_address: String) -> Self {
        Self {
            ip_address,
            shuffle_caches: Default::default(),
            in_progress_shuffle_caches: Default::default(),
        }
    }

    pub fn get_or_create_in_progress_shuffle_cache(
        &self,
        num_partitions: usize,
        shuffle_dirs: &[String],
        shuffle_id: u64,
        target_filesize: usize,
        compression: Option<&str>,
        cache_id: String,
    ) -> DaftResult<Arc<InProgressShuffleCache>> {
        {
            let mut in_progress = self.in_progress_shuffle_caches.lock().unwrap();
            if let Some(entry) = in_progress.get_mut(&shuffle_id) {
                entry.active_cache_ids.insert(cache_id);
                return Ok(entry.cache.clone());
            }
        };

        let cache = Arc::new(InProgressShuffleCache::try_new(
            num_partitions,
            shuffle_dirs,
            shuffle_id,
            target_filesize,
            compression,
        )?);

        let mut in_progress = self.in_progress_shuffle_caches.lock().unwrap();
        let entry = in_progress
            .entry(shuffle_id)
            .or_insert_with(|| InProgressShuffleEntry {
                cache: cache.clone(),
                active_cache_ids: HashSet::new(),
            });
        entry.active_cache_ids.insert(cache_id);
        Ok(entry.cache.clone())
    }

    pub async fn release_in_progress_shuffle_cache(
        &self,
        shuffle_id: u64,
        cache_id: &str,
    ) -> DaftResult<Option<Arc<ShuffleCache>>> {
        let cache_to_finalize = {
            let mut in_progress = self.in_progress_shuffle_caches.lock().unwrap();
            let Some(entry) = in_progress.get_mut(&shuffle_id) else {
                return Ok(None);
            };
            entry.active_cache_ids.remove(cache_id);
            if !entry.active_cache_ids.is_empty() {
                return Ok(None);
            }
            in_progress.remove(&shuffle_id).map(|entry| entry.cache)
        };

        if let Some(cache) = cache_to_finalize {
            let finalized = Arc::new(cache.close().await?);
            self.register_shuffle_cache(shuffle_id, finalized.clone())
                .await?;
            Ok(Some(finalized))
        } else {
            Ok(None)
        }
    }

    pub async fn register_shuffle_cache(
        &self,
        shuffle_id: u64,
        cache: Arc<ShuffleCache>,
    ) -> DaftResult<()> {
        let mut caches = self.shuffle_caches.lock().await;
        caches.entry(shuffle_id).or_insert(Vec::new()).push(cache);
        Ok(())
    }

    async fn get_shuffle_caches(&self, shuffle_id: u64) -> Option<Vec<Arc<ShuffleCache>>> {
        let caches = self.shuffle_caches.lock().await;
        caches.get(&shuffle_id).cloned()
    }

    /// Get partition data in-process (no gRPC). Returns an iterator of Daft RecordBatches.
    /// Used when the reader runs on the same node as the shuffle server.
    pub fn get_partition_local(
        &self,
        shuffle_id: u64,
        partition_idx: usize,
        _cache_ids: Option<&[u32]>,
    ) -> DaftResult<Vec<RecordBatch>> {
        let caches = self.shuffle_caches.blocking_lock();
        let filtered_caches = caches.get(&shuffle_id).cloned().ok_or_else(|| {
            DaftError::ValueError(format!("Shuffle cache not found for id: {}", shuffle_id))
        })?;

        if filtered_caches.is_empty() {
            return Err(DaftError::ValueError("No caches for partition".to_string()));
        }

        let handles = filtered_caches
            .iter()
            .flat_map(|cache| cache.file_paths_for_partition(partition_idx))
            .map(|file_path| std::thread::spawn(move || read_ipc_file_batches(file_path)))
            .collect::<Vec<_>>();

        let mut all_batches = Vec::new();
        for handle in handles {
            let file_batches = handle.join().map_err(|_| {
                DaftError::InternalError("local IPC file reader thread panicked".to_string())
            })??;
            all_batches.extend(file_batches);
        }

        Ok(all_batches)
    }
}

fn read_ipc_file_batches(file_path: String) -> DaftResult<Vec<RecordBatch>> {
    let file = std::fs::File::open(&file_path).map_err(|e| {
        DaftError::IoError(std::io::Error::other(format!(
            "Error opening file {}: {}",
            file_path, e
        )))
    })?;
    let reader = FileReader::try_new(file, None).map_err(|e| {
        DaftError::InternalError(format!(
            "Error creating IPC file reader for {}: {}",
            file_path, e
        ))
    })?;

    reader
        .map(|batch_result| {
            let arrow_batch = batch_result.map_err(|e| {
                DaftError::InternalError(format!(
                    "Error reading IPC record batch from {}: {}",
                    file_path, e
                ))
            })?;
            RecordBatch::try_from(&arrow_batch).map_err(|e| {
                DaftError::InternalError(format!(
                    "Error converting Arrow batch from {}: {}",
                    file_path, e
                ))
            })
        })
        .collect()
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

        let shuffle_caches = self
            .get_shuffle_caches(ticket.shuffle_id)
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Shuffle cache not found for id: {}",
                    ticket.shuffle_id
                ))
            })?;

        let filtered_caches = shuffle_caches;

        let file_paths = filtered_caches
            .iter()
            .flat_map(|cache| cache.file_paths_for_partition(ticket.partition_idx))
            .collect::<Vec<_>>();

        let file_path_stream = futures::stream::iter(file_paths);
        let flight_data_stream = file_path_stream
            .then(|file_path| async move {
                let file = tokio::fs::File::open(file_path)
                    .await
                    .map_err(|e| Status::internal(format!("Error opening file: {}", e)))?;
                let reader = FlightDataStreamReader::try_new(BufReader::new(file))
                    .await
                    .map_err(|e| {
                        Status::internal(format!("Error creating flight data reader: {}", e))
                    })?;
                Ok::<_, Status>(
                    reader
                        .into_stream()
                        .map_err(|e| Status::internal(e.to_string())),
                )
            })
            .try_flatten();

        let schema = filtered_caches
            .first()
            .expect("Expected at least one cache")
            .schema()
            .to_arrow()
            .map_err(|e| Status::internal(format!("Error converting schema to arrow: {}", e)))?;

        let options = IpcWriteOptions::default();
        let flight_schema = SchemaAsIpc::new(&schema, &options).into();
        let flight_data =
            futures::stream::once(async { Ok(flight_schema) }).chain(flight_data_stream);

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

pub struct FlightServerConnectionHandle {
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
        port,
        shutdown_signal: Some(shutdown_tx),
        server_task: Some(server_task),
    }
}
