pub mod flight_client;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use arrow_flight::{Action, Ticket, client::FlightClient};
use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use tonic::transport::{Channel, Endpoint};

use crate::{
    client::flight_client::FlightRecordBatchStreamToDaftRecordBatchStream,
    server::flight_server::SEAL_SHUFFLE_ACTION,
};

/// Manages a per-server `tonic::transport::Channel` so concurrent `fetch_partition`
/// calls to the same server can issue `do_get`s in parallel over tonic's HTTP/2
/// multiplexed connection.
///
/// The previous design held an `Arc<Mutex<ShuffleFlightClient>>` per server: every
/// `fetch_partition` had to take the inner `Mutex` for the duration of
/// `client.do_get(ticket).await`, serializing request setup against every other
/// fetch on the same server. With shared `Channel`s and a fresh `FlightClient`
/// per request, do_gets fan out independently.
#[derive(Clone, Default)]
pub struct FlightClientManager {
    channels: Arc<RwLock<HashMap<String, Channel>>>,
}

impl FlightClientManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn fetch_partition(
        &self,
        shuffle_id: u64,
        server_address: &str,
        partition_ref_ids: &[u64],
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let channel = self.get_or_connect(server_address).await?;

        // Fresh FlightClient per request. Construction is essentially free (no I/O):
        // `FlightClient::new(channel)` wraps the channel in a tonic-generated client.
        let raw = FlightClient::new(channel)
            .into_inner()
            .max_decoding_message_size(usize::MAX);
        let mut client = FlightClient::new_from_inner(raw);

        let ticket = Ticket::new(format!(
            "{}:{}",
            shuffle_id,
            partition_ref_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));

        let stream = client.do_get(ticket).await.map_err(|e| {
            DaftError::External(
                format!(
                    "Error fetching partition refs {:?} from shuffle {} at {}. {}",
                    partition_ref_ids, shuffle_id, server_address, e
                )
                .into(),
            )
        })?;

        let daft_stream =
            FlightRecordBatchStreamToDaftRecordBatchStream::new(stream, schema).boxed();
        Ok(daft_stream)
    }

    /// Tell the server at `server_address` to consolidate all per-task entry groups
    /// for `shuffle_id` into one file per partition. Called once per server by the
    /// orchestrator after the producer stage drains and before consumer tasks run.
    /// Returns when the server has finished the consolidation (or errored).
    pub async fn seal_shuffle(&self, server_address: &str, shuffle_id: u64) -> DaftResult<()> {
        let channel = self.get_or_connect(server_address).await?;
        let raw = FlightClient::new(channel).into_inner();
        let mut client = FlightClient::new_from_inner(raw);
        let action = Action {
            r#type: SEAL_SHUFFLE_ACTION.to_string(),
            body: shuffle_id.to_le_bytes().to_vec().into(),
        };
        // The server emits a single empty Result on success. Drain the stream to
        // surface any error; we don't care about the body.
        let mut stream = client.do_action(action).await.map_err(|e| {
            DaftError::External(
                format!(
                    "seal_shuffle({}) at {} failed: {}",
                    shuffle_id, server_address, e
                )
                .into(),
            )
        })?;
        while let Some(msg) = stream.next().await {
            msg.map_err(|e| {
                DaftError::External(
                    format!(
                        "seal_shuffle({}) at {} stream error: {}",
                        shuffle_id, server_address, e
                    )
                    .into(),
                )
            })?;
        }
        Ok(())
    }

    /// Cheap hot-path: read-lock the map and clone the cached `Channel` (Arc inside).
    /// Slow path (first call per server): release the read lock, do the async connect,
    /// then take the write lock to insert. Two concurrent slow paths on the same
    /// server are tolerated — last writer wins, both end up with equivalent channels.
    async fn get_or_connect(&self, server_address: &str) -> DaftResult<Channel> {
        {
            let channels = self.channels.read().expect("channels lock poisoned");
            if let Some(ch) = channels.get(server_address) {
                return Ok(ch.clone());
            }
        }

        let endpoint = Endpoint::from_shared(server_address.to_string()).map_err(|e| {
            DaftError::External(format!("Failed to create endpoint: {:?}", e).into())
        })?;
        let channel = endpoint.connect().await.map_err(|e| {
            DaftError::External(format!("Failed to connect to endpoint: {:?}", e).into())
        })?;

        {
            let mut channels = self.channels.write().expect("channels lock poisoned");
            channels
                .entry(server_address.to_string())
                .or_insert_with(|| channel.clone());
        }
        Ok(channel)
    }
}
