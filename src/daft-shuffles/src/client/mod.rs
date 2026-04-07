pub mod flight_client;

use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::stream::BoxStream;
use tokio::sync::Mutex;

use crate::client::flight_client::ShuffleFlightClient;

#[derive(Clone)]
pub struct FlightClientManager {
    clients: Arc<Mutex<HashMap<String, Arc<Mutex<ShuffleFlightClient>>>>>,
}

impl FlightClientManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn fetch_record_batches(
        &self,
        shuffle_id: u64,
        server_address: &str,
        partition_ref_ids: &[u64],
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let client = {
            let mut clients = self.clients.lock().await;
            clients
                .entry(server_address.to_string())
                .or_insert_with(|| {
                    Arc::new(Mutex::new(ShuffleFlightClient::new(
                        server_address.to_string(),
                    )))
                })
                .clone()
        };

        client
            .lock()
            .await
            .get_record_batches(shuffle_id, partition_ref_ids, schema)
            .await
    }
}

impl Default for FlightClientManager {
    fn default() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
