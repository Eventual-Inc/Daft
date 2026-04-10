use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::{JoinSet, combine_stream, get_compute_pool_num_threads, get_io_runtime};
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_local_plan::{FlightShufflePartitionRef, FlightShuffleReadInput, InputId};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{client::FlightClientManager, server::flight_server::ShuffleFlightServer};
use futures::{FutureExt, StreamExt, stream::BoxStream};
use tracing::instrument;

use super::source::{Source, SourceStream};
use crate::{
    channel::{Sender, UnboundedReceiver, create_channel},
    pipeline::{NodeName, PipelineMessage},
};

pub struct ShuffleReadSource {
    receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
    local_server: Option<(Arc<ShuffleFlightServer>, String)>,
    schema: SchemaRef,
    num_parallel_tasks: usize,
}

impl ShuffleReadSource {
    pub fn new(
        receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
        schema: SchemaRef,
        cfg: &DaftExecutionConfig,
        local_server: Option<(Arc<ShuffleFlightServer>, String)>,
    ) -> Self {
        let num_cpus = get_compute_pool_num_threads();
        let num_parallel_tasks = if cfg.scantask_max_parallel > 0 {
            cfg.scantask_max_parallel
        } else {
            num_cpus
        };

        Self {
            receiver,
            local_server,
            schema,
            num_parallel_tasks,
        }
    }

    fn spawn_flight_shuffle_processor(
        self,
        output_sender: Sender<PipelineMessage>,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let mut receiver = self.receiver;
        let num_parallel_tasks = self.num_parallel_tasks;
        let local_server = self.local_server;
        let schema = self.schema;

        let io_runtime = get_io_runtime(true);
        io_runtime.spawn(async move {
            let client_manager = FlightClientManager::new();
            let mut task_set = JoinSet::new();
            let mut pending_tasks: VecDeque<(InputId, FlightShuffleReadInput)> = VecDeque::new();
            let mut input_id_pending_counts: HashMap<InputId, usize> = HashMap::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || !task_set.is_empty() {
                while task_set.len() < num_parallel_tasks && let Some((input_id, input)) = pending_tasks.pop_front() {
                    task_set.spawn(forward_partition_refs(
                        client_manager.clone(),
                        local_server.clone(),
                        input.refs,
                        schema.clone(),
                        output_sender.clone(),
                        input_id,
                    ));
                }

                tokio::select! {
                    recv_result = receiver.recv(), if !receiver_exhausted => {
                        match recv_result {
                            Some((input_id, inputs)) if inputs.is_empty() => {
                                let empty = MicroPartition::empty(Some(schema.clone()));
                                if output_sender.send(PipelineMessage::Morsel {
                                    input_id,
                                    partition: empty,
                                }).await.is_err() {
                                    return Ok(());
                                }
                                if output_sender.send(PipelineMessage::Flush(input_id)).await.is_err() {
                                    return Ok(());
                                }
                            }
                            Some((input_id, inputs)) => {
                                let num_inputs = inputs.len();
                                *input_id_pending_counts.entry(input_id).or_insert(0) += num_inputs;
                                for input in inputs {
                                    pending_tasks.push_back((input_id, input));
                                }
                            }
                            None => {
                                receiver_exhausted = true;
                            }
                        }
                    }
                    Some(join_result) = task_set.join_next(), if !task_set.is_empty() => {
                        match join_result {
                            Ok(Ok(completed_input_id)) => {
                                let count = input_id_pending_counts.get_mut(&completed_input_id).expect("Input id should be present in input_id_pending_counts");
                                *count = count.saturating_sub(1);
                                if *count == 0 {
                                    input_id_pending_counts.remove(&completed_input_id);
                                    if output_sender.send(PipelineMessage::Flush(completed_input_id)).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                            Ok(Err(e)) => return Err(e),
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

async fn fetch_partition_refs(
    client_manager: FlightClientManager,
    local_server: Option<&(Arc<ShuffleFlightServer>, String)>,
    refs: &[FlightShufflePartitionRef],
    schema: SchemaRef,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let refs: Vec<_> = refs
        .iter()
        .filter(|partition_ref| partition_ref.num_rows > 0 && partition_ref.size_bytes > 0)
        .collect();

    if refs.is_empty() {
        return Ok(futures::stream::empty().boxed());
    }

    // Split refs into local (same node) and remote based on server address.
    let (local_refs, remote_refs): (Vec<_>, Vec<_>) = if let Some((_, local_address)) = local_server
    {
        refs.into_iter()
            .partition(|r| r.server_address == *local_address)
    } else {
        (vec![], refs)
    };

    let mut streams: Vec<BoxStream<'static, DaftResult<RecordBatch>>> = Vec::new();

    // Local path: read IPC files directly from disk, bypassing gRPC.
    if !local_refs.is_empty() {
        let (server, _) =
            local_server.expect("local_refs is non-empty only when local_server is Some");
        let mut refs_by_shuffle: HashMap<u64, Vec<u64>> = HashMap::new();
        for partition_ref in local_refs {
            refs_by_shuffle
                .entry(partition_ref.shuffle_id)
                .or_default()
                .push(partition_ref.partition_ref_id);
        }

        for (shuffle_id, partition_ref_ids) in refs_by_shuffle {
            let local_stream = server
                .read_local_partitions(shuffle_id, &partition_ref_ids, schema.clone())
                .await?;
            streams.push(local_stream);
        }
    }

    // Remote path: fetch over gRPC.
    if !remote_refs.is_empty() {
        let mut refs_by_server: HashMap<(u64, &str), Vec<u64>> = HashMap::new();
        for partition_ref in remote_refs {
            refs_by_server
                .entry((
                    partition_ref.shuffle_id,
                    partition_ref.server_address.as_str(),
                ))
                .or_default()
                .push(partition_ref.partition_ref_id);
        }
        let fetches = refs_by_server
            .into_iter()
            .map(|((shuffle_id, server_address), partition_ref_ids)| {
                let client_manager = client_manager.clone();
                let schema = schema.clone();
                let server_address = server_address.to_string();
                async move {
                    client_manager
                        .fetch_record_batches(
                            shuffle_id,
                            &server_address,
                            &partition_ref_ids,
                            schema,
                        )
                        .await
                }
            })
            .collect::<Vec<_>>();
        let remote_streams = futures::future::try_join_all(fetches).await?;
        streams.extend(remote_streams);
    }

    Ok(futures::stream::select_all(streams).boxed())
}

async fn forward_partition_refs(
    client_manager: FlightClientManager,
    local_server: Option<(Arc<ShuffleFlightServer>, String)>,
    refs: Vec<FlightShufflePartitionRef>,
    schema: SchemaRef,
    sender: Sender<PipelineMessage>,
    input_id: InputId,
) -> DaftResult<InputId> {
    let nonempty_ref_count = refs
        .iter()
        .filter(|partition_ref| partition_ref.num_rows > 0 && partition_ref.size_bytes > 0)
        .count();
    if nonempty_ref_count == 0 {
        if sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: MicroPartition::empty(Some(schema)),
            })
            .await
            .is_err()
        {
            return Ok(input_id);
        }
        return Ok(input_id);
    }

    let mut batches =
        fetch_partition_refs(client_manager, local_server.as_ref(), &refs, schema.clone()).await?;

    while let Some(batch) = batches.next().await {
        let batch = batch?;
        let partition = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![batch]), None);
        if sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition,
            })
            .await
            .is_err()
        {
            return Ok(input_id);
        }
    }

    Ok(input_id)
}

#[async_trait]
impl Source for ShuffleReadSource {
    fn name(&self) -> NodeName {
        "ShuffleRead".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::ScanTask
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["ShuffleRead".to_string()]
    }

    #[instrument(skip_all, name = "ShuffleReadSource::get_data")]
    fn get_data(
        self: Box<Self>,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(1);
        let processor_task = self.spawn_flight_shuffle_processor(output_sender);

        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use common_error::DaftResult;
    use daft_core::{
        prelude::{DataType, Field, Int64Array, Schema},
        series::IntoSeries,
    };
    use daft_shuffles::partition_store::InProgressFlightPartitionStore;
    use futures::TryStreamExt;

    use super::*;
    use crate::{channel::create_channel, pipeline::PipelineMessage};

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]))
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
        let path = std::env::temp_dir().join(format!("daft-shuffle-read-test-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path.to_string_lossy().to_string()
    }

    /// Helper: write data through the partition store, register with a server,
    /// and return the server + registered partition refs.
    async fn setup_local_server(
        values: &[&[i64]],
    ) -> (
        Arc<ShuffleFlightServer>,
        Vec<FlightShufflePartitionRef>,
        u64,
    ) {
        let temp_dir = make_temp_dir();
        let shuffle_id = 99;
        let input_id = 1;
        let local_address = "grpc://local:9999".to_string();

        let partition_set = InProgressFlightPartitionStore::try_new(
            input_id,
            values.len(),
            std::slice::from_ref(&temp_dir),
            shuffle_id,
            make_schema(),
            None,
        )
        .unwrap();
        let partitions: Vec<MicroPartition> = values
            .iter()
            .map(|vals| {
                if vals.is_empty() {
                    MicroPartition::empty(Some(make_schema()))
                } else {
                    make_mp(vals)
                }
            })
            .collect();
        partition_set
            .push_partitioned_data(partitions)
            .await
            .unwrap();
        let registered = partition_set.close().await.unwrap();

        let refs: Vec<FlightShufflePartitionRef> = registered
            .iter()
            .map(|p| FlightShufflePartitionRef {
                shuffle_id,
                server_address: local_address.clone(),
                partition_ref_id: p.partition_ref_id,
                num_rows: p.num_rows,
                size_bytes: p.size_bytes,
            })
            .collect();

        let server = Arc::new(ShuffleFlightServer::new());
        server
            .register_shuffle_partitions(shuffle_id, registered)
            .await
            .unwrap();

        (server, refs, shuffle_id)
    }

    #[tokio::test]
    async fn forward_partition_refs_emits_single_empty_morsel_when_all_refs_are_empty()
    -> DaftResult<()> {
        let schema = make_schema();
        let refs = vec![
            FlightShufflePartitionRef {
                shuffle_id: 11,
                server_address: "grpc://worker-a:1234".to_string(),
                partition_ref_id: 101,
                num_rows: 0,
                size_bytes: 100,
            },
            FlightShufflePartitionRef {
                shuffle_id: 11,
                server_address: "grpc://worker-b:1234".to_string(),
                partition_ref_id: 102,
                num_rows: 4,
                size_bytes: 0,
            },
        ];
        let (sender, mut receiver) = create_channel(4);

        let completed_input_id = forward_partition_refs(
            FlightClientManager::new(),
            None,
            refs,
            schema.clone(),
            sender.clone(),
            7,
        )
        .await?;
        drop(sender);

        assert_eq!(completed_input_id, 7);

        let Some(PipelineMessage::Morsel {
            input_id,
            partition,
        }) = receiver.recv().await
        else {
            panic!("expected empty morsel");
        };
        assert_eq!(input_id, 7);
        assert_eq!(partition.len(), 0);
        assert_eq!(partition.schema(), schema);
        assert!(receiver.recv().await.is_none());

        Ok(())
    }

    /// When local_server is set and refs match its address, data is read directly
    /// from disk. No gRPC server is running, so success proves the local path was used.
    #[tokio::test]
    async fn fetch_partition_refs_uses_local_disk_path() -> DaftResult<()> {
        let (server, refs, _) = setup_local_server(&[&[10, 20], &[30]]).await;
        let local_address = "grpc://local:9999".to_string();
        let local_server = (server, local_address);

        let stream = fetch_partition_refs(
            FlightClientManager::new(),
            Some(&local_server),
            &refs,
            make_schema(),
        )
        .await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total_rows, 3);

        let mut all_values: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let col = b.get_column(0).i64().unwrap();
                (0..b.len()).map(move |i| col.get(i).unwrap())
            })
            .collect();
        all_values.sort_unstable();
        assert_eq!(all_values, vec![10, 20, 30]);

        Ok(())
    }

    /// With local_server=None, the same refs are routed to gRPC. Since no gRPC
    /// server is listening, this must fail — proving that without a local_server
    /// the local disk path is not used.
    #[tokio::test]
    async fn fetch_partition_refs_without_local_server_goes_to_grpc() {
        let (_server, refs, _) = setup_local_server(&[&[10, 20]]).await;

        let stream = fetch_partition_refs(
            FlightClientManager::new(),
            None, // no local server → must use gRPC
            &refs,
            make_schema(),
        )
        .await;

        assert!(stream.is_err(), "expected gRPC connection error");
    }

    /// Refs whose address doesn't match the local server are routed to gRPC.
    /// Since no gRPC server is listening at that address, this must fail —
    /// proving the address comparison drives the routing decision.
    #[tokio::test]
    async fn fetch_partition_refs_mismatched_address_goes_to_grpc() {
        let (server, refs, _) = setup_local_server(&[&[10, 20]]).await;
        // local_server address doesn't match the refs' address
        let local_server = (server, "grpc://OTHER-HOST:7777".to_string());

        let stream = fetch_partition_refs(
            FlightClientManager::new(),
            Some(&local_server),
            &refs,
            make_schema(),
        )
        .await;

        assert!(stream.is_err(), "expected gRPC connection error");
    }

    /// Mixed refs: local refs are read from disk, remote refs go through gRPC
    /// to a real flight server. Both paths return correct data.
    #[tokio::test]
    async fn fetch_partition_refs_mixed_local_and_remote() -> DaftResult<()> {
        let (server, mut local_refs, _shuffle_id) = setup_local_server(&[&[10, 20], &[30]]).await;

        // Start a real gRPC server so remote refs have somewhere to connect.
        // start_server_loop uses blocking_recv internally, which panics inside
        // a tokio runtime context, so run on a plain OS thread.
        let server_clone = server.clone();
        let (conn_tx, conn_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            conn_tx
                .send(daft_shuffles::server::flight_server::start_server_loop(
                    "127.0.0.1",
                    server_clone,
                ))
                .unwrap();
        });
        let conn = conn_rx.recv().unwrap();
        let grpc_address = conn.shuffle_address();
        let local_address = "grpc://local:9999".to_string();

        // Re-tag one ref as "remote" by pointing it at the gRPC address.
        let remote_ref = FlightShufflePartitionRef {
            server_address: grpc_address,
            ..local_refs.pop().unwrap()
        };
        let mut all_refs = local_refs; // first ref stays local
        all_refs.push(remote_ref); // second ref goes through gRPC

        let local_server = (server, local_address);
        let stream = fetch_partition_refs(
            FlightClientManager::new(),
            Some(&local_server),
            &all_refs,
            make_schema(),
        )
        .await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total_rows, 3);

        let mut all_values: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let col = b.get_column(0).i64().unwrap();
                (0..b.len()).map(move |i| col.get(i).unwrap())
            })
            .collect();
        all_values.sort_unstable();
        assert_eq!(all_values, vec![10, 20, 30]);

        drop(conn);
        Ok(())
    }
}
