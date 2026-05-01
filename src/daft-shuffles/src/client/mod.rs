pub mod flight_client;

use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::client::flight_client::ShuffleFlightClient;

#[derive(Clone)]
pub struct FlightClientManager {
    clients: Arc<Mutex<HashMap<String, Arc<Mutex<ShuffleFlightClient>>>>>,
}

impl FlightClientManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn fetch_partition(
        &self,
        shuffle_id: u64,
        server_address: &str,
        partition_ref_ids: &[Uuid],
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

        let stream = client
            .lock()
            .await
            .get_partition(shuffle_id, partition_ref_ids, schema)
            .await?
            .boxed();
        Ok(stream)
    }
}

impl Default for FlightClientManager {
    fn default() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
