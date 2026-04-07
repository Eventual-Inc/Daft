use std::{collections::HashMap, io::Cursor, pin::Pin, sync::Arc};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    flight_service_server::{FlightService, FlightServiceServer},
};
use arrow_ipc::{
    MessageArgs, MessageHeader, MetadataVersion, finish_message_buffer, writer::IpcWriteOptions,
};
use common_error::{DaftError, DaftResult};
use common_runtime::RuntimeTask;
use daft_core::{prelude::SchemaRef, series::Series};
use daft_recordbatch::RecordBatch;
use daft_schema::field::FieldRef;
use flatbuffers::FlatBufferBuilder;
use futures::{Stream, StreamExt, TryStreamExt, stream::BoxStream};
use tokio::{io::BufReader, sync::Mutex};
use tonic::{Request, Response, Status, transport::Server};

use super::stream::FlightDataStreamReader;
use crate::partition_store::RegisteredFlightPartition;

struct ParsedTicket {
    shuffle_id: u64,
    partition_ref_ids: Vec<u64>,
}

impl ParsedTicket {
    fn from_ticket(ticket: &Ticket) -> Result<Self, Status> {
        let ticket_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

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
        if partition_ref_ids.is_empty() {
            return Err(Status::invalid_argument(
                "At least one partition ref id must be provided",
            ));
        }

        Ok(Self {
            shuffle_id,
            partition_ref_ids,
        })
    }
}

fn partition_boundary_flight_data(partition_ref_id: u64) -> FlightData {
    let mut builder = FlatBufferBuilder::new();
    let message = arrow_ipc::Message::create(
        &mut builder,
        &MessageArgs {
            version: MetadataVersion::V5,
            header_type: MessageHeader::NONE,
            header: None,
            bodyLength: 0,
            custom_metadata: None,
        },
    );
    finish_message_buffer(&mut builder, message);
    FlightData {
        data_header: builder.finished_data().to_vec().into(),
        app_metadata: format!("partition_ref_id:{partition_ref_id}").into(),
        ..Default::default()
    }
}

#[derive(Clone, Default)]
pub struct ShuffleFlightServer {
    shuffle_partitions: Arc<Mutex<HashMap<u64, HashMap<u64, RegisteredFlightPartition>>>>,
}

impl ShuffleFlightServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_shuffle_partitions(
        &self,
        shuffle_id: u64,
        partitions: Vec<RegisteredFlightPartition>,
    ) -> DaftResult<()> {
        let mut shuffle_partitions = self.shuffle_partitions.lock().await;
        let partition_map = shuffle_partitions.entry(shuffle_id).or_default();
        for partition in partitions {
            partition_map.insert(partition.partition_ref_id, partition);
        }
        Ok(())
    }

    pub async fn clear_shuffle(&self, shuffle_id: u64) -> bool {
        let mut shuffle_partitions = self.shuffle_partitions.lock().await;
        shuffle_partitions.remove(&shuffle_id).is_some()
    }

    pub async fn clear_shuffles(&self, shuffle_ids: &[u64]) -> usize {
        let mut shuffle_partitions = self.shuffle_partitions.lock().await;
        shuffle_ids
            .iter()
            .filter(|shuffle_id| shuffle_partitions.remove(shuffle_id).is_some())
            .count()
    }

    /// Read partition data directly from disk, bypassing the gRPC stack.
    /// Used when the shuffle reader runs on the same node as the shuffle server.
    pub async fn read_local_partitions(
        &self,
        shuffle_id: u64,
        partition_ref_ids: &[u64],
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let partitions = self
            .get_shuffle_partitions(shuffle_id, partition_ref_ids)
            .await
            .ok_or_else(|| {
                DaftError::ValueError(format!(
                    "Shuffle partitions not found for shuffle {} refs {:?}",
                    shuffle_id, partition_ref_ids
                ))
            })?;

        let file_paths: Vec<String> = partitions
            .into_iter()
            .filter_map(|p| if p.has_data() { p.file_path } else { None })
            .collect();

        if file_paths.is_empty() {
            return Ok(futures::stream::empty().boxed());
        }

        let fields: Vec<FieldRef> = schema
            .fields()
            .iter()
            .map(|f| Arc::new(f.clone()))
            .collect();

        let stream = futures::stream::iter(file_paths)
            .then(move |file_path| {
                let schema = schema.clone();
                let fields = fields.clone();
                async move {
                    let bytes = tokio::fs::read(&file_path)
                        .await
                        .map_err(DaftError::IoError)?;
                    let batches = read_ipc_to_record_batches(&bytes, &schema, &fields)?;
                    Ok::<_, DaftError>(futures::stream::iter(batches.into_iter().map(Ok)))
                }
            })
            .try_flatten();

        Ok(stream.boxed())
    }

    async fn get_shuffle_partitions(
        &self,
        shuffle_id: u64,
        partition_ref_ids: &[u64],
    ) -> Option<Vec<RegisteredFlightPartition>> {
        let partitions = self.shuffle_partitions.lock().await;
        let shuffle_partitions = partitions.get(&shuffle_id)?;
        partition_ref_ids
            .iter()
            .map(|partition_ref_id| shuffle_partitions.get(partition_ref_id).cloned())
            .collect()
    }
}

/// Read an Arrow IPC streaming file directly into Daft RecordBatches.
fn read_ipc_to_record_batches(
    bytes: &[u8],
    schema: &SchemaRef,
    fields: &[FieldRef],
) -> DaftResult<Vec<RecordBatch>> {
    let mut cursor = Cursor::new(bytes);
    let reader = arrow_ipc::reader::StreamReader::try_new(&mut cursor, None)
        .map_err(|e| DaftError::InternalError(format!("Error reading IPC stream: {}", e)))?;

    let mut batches = Vec::new();
    for arrow_batch_result in reader {
        let arrow_batch = arrow_batch_result
            .map_err(|e| DaftError::InternalError(format!("Error reading IPC batch: {}", e)))?;
        let columns = fields
            .iter()
            .zip(arrow_batch.columns())
            .map(|(field, array)| Series::from_arrow(field.clone(), array.clone()))
            .collect::<DaftResult<Vec<_>>>()?;
        batches.push(RecordBatch::new_with_size(
            schema.clone(),
            columns,
            arrow_batch.num_rows(),
        )?);
    }
    Ok(batches)
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

        let partitions = self
            .get_shuffle_partitions(ticket.shuffle_id, &ticket.partition_ref_ids)
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Shuffle partition refs not found for shuffle {} refs {:?}",
                    ticket.shuffle_id, ticket.partition_ref_ids
                ))
            })?;
        let schema = partitions
            .first()
            .expect("expected at least one shuffle partition")
            .schema
            .to_arrow()
            .map_err(|e| Status::internal(format!("Error converting schema to arrow: {}", e)))?;

        let partition_stream = futures::stream::iter(partitions)
            .then(|partition| async move {
                let boundary_stream = futures::stream::once(async move {
                    Ok::<_, Status>(partition_boundary_flight_data(partition.partition_ref_id))
                });

                let data_stream = if let Some(file_path) = partition.file_path.clone() {
                    let file = tokio::fs::File::open(file_path)
                        .await
                        .map_err(|e| Status::internal(format!("Error opening file: {}", e)))?;
                    let reader = FlightDataStreamReader::try_new(BufReader::new(file))
                        .await
                        .map_err(|e| {
                            Status::internal(format!("Error creating flight data reader: {}", e))
                        })?;
                    reader
                        .into_stream()
                        .map_err(|e| Status::internal(e.to_string()))
                        .boxed()
                } else {
                    futures::stream::empty::<Result<FlightData, Status>>().boxed()
                };

                Ok::<_, Status>(boundary_stream.chain(data_stream).boxed())
            })
            .try_flatten();

        let options = IpcWriteOptions::default();
        let flight_schema = SchemaAsIpc::new(&schema, &options).into();
        let flight_data: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>> =
            Box::pin(
                futures::stream::once(async move { Ok(flight_schema) }).chain(partition_stream),
            );

        Ok(Response::new(flight_data))
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
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use daft_core::{
        prelude::{DataType, Field, Int64Array, Schema},
        series::IntoSeries,
    };
    use daft_micropartition::MicroPartition;
    use futures::TryStreamExt;

    use super::*;
    use crate::partition_store::InProgressFlightPartitionSet;

    fn make_schema() -> Arc<daft_schema::schema::Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]))
    }

    fn make_partition(partition_ref_id: u64) -> RegisteredFlightPartition {
        RegisteredFlightPartition {
            partition_ref_id,
            schema: make_schema(),
            file_path: None,
            num_rows: 0,
            size_bytes: 0,
        }
    }

    fn make_mp(values: &[i64]) -> MicroPartition {
        let rb = daft_recordbatch::RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_values("a", values.iter().copied()).into_series(),
        ])
        .unwrap();
        MicroPartition::new_loaded(make_schema(), Arc::new(vec![rb]), None)
    }

    fn make_temp_dir() -> String {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("daft-flight-server-test-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn clear_shuffle_removes_registered_partitions() {
        let server = ShuffleFlightServer::new();
        server
            .register_shuffle_partitions(11, vec![make_partition(101), make_partition(102)])
            .await
            .unwrap();

        assert!(
            server
                .get_shuffle_partitions(11, &[101, 102])
                .await
                .is_some()
        );
        assert!(server.clear_shuffle(11).await);
        assert!(!server.clear_shuffle(11).await);
        assert!(server.get_shuffle_partitions(11, &[101]).await.is_none());
    }

    #[tokio::test]
    async fn clear_shuffles_counts_removed_shuffles() {
        let server = ShuffleFlightServer::new();
        server
            .register_shuffle_partitions(11, vec![make_partition(101)])
            .await
            .unwrap();
        server
            .register_shuffle_partitions(12, vec![make_partition(201)])
            .await
            .unwrap();

        assert_eq!(server.clear_shuffles(&[11, 12, 99]).await, 2);
        assert!(server.get_shuffle_partitions(11, &[101]).await.is_none());
        assert!(server.get_shuffle_partitions(12, &[201]).await.is_none());
    }

    #[tokio::test]
    async fn read_local_partitions_reads_ipc_from_disk() {
        let temp_dir = make_temp_dir();
        let shuffle_id = 42;
        let input_id = 7;

        // Write real IPC files through the partition store.
        let partition_set = InProgressFlightPartitionSet::try_new(
            2,
            std::slice::from_ref(&temp_dir),
            input_id,
            shuffle_id,
            make_schema(),
            None,
        )
        .unwrap();
        partition_set
            .push_partitioned_data(vec![make_mp(&[10, 20, 30]), make_mp(&[40])])
            .await
            .unwrap();
        let registered = partition_set.close().await.unwrap();

        // Register them with the flight server.
        let server = ShuffleFlightServer::new();
        let ref_ids: Vec<u64> = registered.iter().map(|p| p.partition_ref_id).collect();
        server
            .register_shuffle_partitions(shuffle_id, registered)
            .await
            .unwrap();

        // Read back via the local disk path (no gRPC involved).
        let stream = server
            .read_local_partitions(shuffle_id, &ref_ids, make_schema())
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total_rows, 4);

        let all_values: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let col = b.get_column(0).i64().unwrap();
                (0..b.len()).map(move |i| col.get(i).unwrap())
            })
            .collect();
        assert_eq!(all_values, vec![10, 20, 30, 40]);
    }

    #[tokio::test]
    async fn read_local_partitions_skips_empty_partitions() {
        let temp_dir = make_temp_dir();
        let shuffle_id = 43;

        // Create partition set but only write to one partition.
        let partition_set = InProgressFlightPartitionSet::try_new(
            2,
            std::slice::from_ref(&temp_dir),
            1,
            shuffle_id,
            make_schema(),
            None,
        )
        .unwrap();
        partition_set
            .push_partitioned_data(vec![
                make_mp(&[100]),
                MicroPartition::empty(Some(make_schema())),
            ])
            .await
            .unwrap();
        let registered = partition_set.close().await.unwrap();

        let server = ShuffleFlightServer::new();
        let ref_ids: Vec<u64> = registered.iter().map(|p| p.partition_ref_id).collect();
        server
            .register_shuffle_partitions(shuffle_id, registered)
            .await
            .unwrap();

        let stream = server
            .read_local_partitions(shuffle_id, &ref_ids, make_schema())
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // Only the non-empty partition's data should appear.
        let total_rows: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total_rows, 1);
    }
}
