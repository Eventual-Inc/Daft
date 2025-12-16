use std::{fs::File, pin::Pin, sync::Arc};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::{FlightService, FlightServiceServer},
};
use common_error::{DaftError, DaftResult};
use common_runtime::RuntimeTask;
use daft_arrow::io::{flight::default_ipc_fields, ipc::write::schema_to_bytes};
use futures::{Stream, StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, transport::Server};

use super::stream::FlightDataStreamReader;
use crate::shuffle_cache::ShuffleCache;

struct ShuffleFlightServer {
    shuffle_cache: Arc<ShuffleCache>,
}

impl ShuffleFlightServer {
    fn new(shuffle_cache: Arc<ShuffleCache>) -> Self {
        Self { shuffle_cache }
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
        let partition_idx_str = String::from_utf8(ticket.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let partition_idx = partition_idx_str
            .parse::<usize>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let file_paths = self.shuffle_cache.file_paths_for_partition(partition_idx);

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

        #[allow(deprecated, reason = "arrow2 migration")]
        let schema =
            self.shuffle_cache.schema().to_arrow2().map_err(|e| {
                Status::internal(format!("Error converting schema to arrow: {}", e))
            })?;
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

pub fn start_flight_server(
    shuffle_cache: Arc<ShuffleCache>,
    ip: &str,
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

        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .expect("Failed to create TCP incoming connection from listener");

        Server::builder()
            .add_service(FlightServiceServer::new(ShuffleFlightServer::new(
                shuffle_cache,
            )))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|e| DaftError::InternalError(format!("Error serving flight server: {}", e)))
    });

    let port = port_rx.blocking_recv().expect("Failed to receive port");

    FlightServerConnectionHandle {
        port,
        shutdown_signal: Some(shutdown_tx),
        server_task: Some(server_task),
    }
}
