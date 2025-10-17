use std::{collections::HashMap, fs::File, pin::Pin, sync::Arc};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::{FlightService, FlightServiceServer},
};
use arrow2::io::{flight::default_ipc_fields, ipc::write::schema_to_bytes};
use common_error::{DaftError, DaftResult};
use common_runtime::RuntimeTask;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::sync::{Mutex, OnceCell};
use tonic::{Request, Response, Status, transport::Server};

use super::stream::FlightDataStreamReader;
use crate::shuffle_cache::ShuffleCache;

#[derive(Clone)]
struct ShuffleFlightServer {
    shuffle_caches: Arc<Mutex<HashMap<u64, Vec<Arc<ShuffleCache>>>>>,
}

static GLOBAL_FLIGHT_SERVER: OnceCell<Arc<ShuffleFlightServer>> = OnceCell::const_new();

impl ShuffleFlightServer {
    fn new() -> Self {
        Self {
            shuffle_caches: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get_or_create_global() -> Arc<Self> {
        GLOBAL_FLIGHT_SERVER
            .get_or_init(|| async { Arc::new(Self::new()) })
            .await
            .clone()
    }

    async fn register_shuffle_cache(
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
        let ticket = request.into_inner().ticket;
        let ticket_str = String::from_utf8(ticket.to_vec())
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

        let shuffle_caches = self.get_shuffle_caches(shuffle_id).await.ok_or_else(|| {
            Status::not_found(format!("Shuffle cache not found for id: {}", shuffle_id))
        })?;

        // Filter caches by cache_ids if provided
        let filtered_caches: Vec<_> = if let Some(ref ids) = cache_ids {
            shuffle_caches
                .iter()
                .filter(|cache| {
                    // Parse cache_id from cache and check if it's in the requested ids
                    cache
                        .cache_id()
                        .parse::<u32>()
                        .map(|cache_id| ids.contains(&cache_id))
                        .unwrap_or(false)
                })
                .cloned()
                .collect()
        } else {
            shuffle_caches
        };

        let file_paths = filtered_caches
            .iter()
            .flat_map(|cache| cache.file_paths_for_partition(partition_idx))
            .collect::<Vec<_>>();

        let file_path_stream = futures::stream::iter(file_paths);
        let flight_data_stream = file_path_stream
            .map(|file_path| {
                let reader = File::open(file_path)
                    .map_err(|e| Status::internal(format!("Error opening file: {}", e)))?;
                let iter = FlightDataStreamReader::try_new(reader).map_err(|e| {
                    Status::internal(format!("Error creating flight data reader: {}", e))
                })?;
                let stream =
                    futures::stream::iter(iter).map_err(|e| Status::internal(e.to_string()));

                Ok::<_, Status>(stream)
            })
            .try_flatten();

        let schema = filtered_caches
            .first()
            .unwrap()
            .schema()
            .to_arrow()
            .map_err(|e| Status::internal(format!("Error converting schema to arrow: {}", e)))?;

        let flight_schema = FlightData {
            data_header: schema_to_bytes(&schema, &default_ipc_fields(&schema.fields)).into(),
            ..Default::default()
        };
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

pub async fn register_shuffle_cache(
    shuffle_id: u64,
    shuffle_cache: Arc<ShuffleCache>,
) -> DaftResult<()> {
    let server = ShuffleFlightServer::get_or_create_global().await;
    server
        .register_shuffle_cache(shuffle_id, shuffle_cache)
        .await
}

#[allow(clippy::result_large_err)]
pub fn start_flight_server(ip: &str) -> FlightServerConnectionHandle {
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

        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .expect("Failed to create TCP incoming connection from listener");

        let flight_server = ShuffleFlightServer::get_or_create_global().await;
        Server::builder()
            .add_service(FlightServiceServer::new((*flight_server).clone()))
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
