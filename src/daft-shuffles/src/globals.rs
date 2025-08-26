use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use once_cell::sync::OnceCell;
use tokio::sync::RwLock;

use crate::{
    client::FlightClientManager,
    server::flight_server::FlightServerConnectionHandle,
    shuffle_cache::ShuffleCache,
};

// Global static flight server - one per process/worker
static FLIGHT_SERVER: OnceCell<Arc<FlightServerConnectionHandle>> = OnceCell::new();

// Global static flight client manager
static FLIGHT_CLIENT_MANAGER: OnceCell<Arc<RwLock<FlightClientManager>>> = OnceCell::new();

/// Initialize the global flight server
pub fn initialize_flight_server(ip: &str) -> DaftResult<()> {
    use crate::server::flight_server::start_flight_server;

    let server_handle = start_flight_server(ip)
        .map_err(|e| common_error::DaftError::InternalError(format!("Failed to start flight server: {}", e)))?;

    FLIGHT_SERVER.set(Arc::new(server_handle))
        .map_err(|_| common_error::DaftError::InternalError("Flight server already initialized".to_string()))?;

    Ok(())
}

/// Initialize the global flight client manager with minimal setup
pub fn initialize_flight_client_manager() -> DaftResult<()> {
    use daft_core::prelude::{DataType, Field, Schema};

    // Create a dummy schema - will be updated when actual operations happen
    let dummy_schema = Schema::new(vec![Field::new("dummy", DataType::Int64)]).unwrap();
    let client_manager = FlightClientManager::new(vec![], 4, Arc::new(dummy_schema));

    FLIGHT_CLIENT_MANAGER.set(Arc::new(RwLock::new(client_manager)))
        .map_err(|_| common_error::DaftError::InternalError("Flight client manager already initialized".to_string()))?;

    Ok(())
}

/// Initialize both flight server and client manager
pub fn initialize_flight_infrastructure(ip: &str) -> DaftResult<u16> {
    initialize_flight_server(ip)?;
    initialize_flight_client_manager()?;

    // Return the server port
    if let Some(server) = get_flight_server() {
        Ok(server.port())
    } else {
        Err(common_error::DaftError::InternalError("Flight server not found after initialization".to_string()))
    }
}

/// Get the global flight server handle
pub fn get_flight_server() -> Option<&'static Arc<FlightServerConnectionHandle>> {
    FLIGHT_SERVER.get()
}

/// Get the global flight client manager
pub fn get_flight_client_manager() -> Option<&'static Arc<RwLock<FlightClientManager>>> {
    FLIGHT_CLIENT_MANAGER.get()
}

/// Add a shuffle cache to the global flight server
pub async fn add_shuffle_cache_to_server(shuffle_id: String, shuffle_cache: Arc<ShuffleCache>) -> DaftResult<()> {
    if let Some(server) = get_flight_server() {
        server.add_shuffle_cache(shuffle_id, shuffle_cache).await;
        Ok(())
    } else {
        Err(common_error::DaftError::InternalError(
            "Flight server not initialized".to_string(),
        ))
    }
}

/// Remove a shuffle cache from the global flight server
pub async fn remove_shuffle_cache_from_server(shuffle_id: &str) -> DaftResult<()> {
    if let Some(server) = get_flight_server() {
        server.remove_shuffle_cache(shuffle_id).await;
        Ok(())
    } else {
        Err(common_error::DaftError::InternalError(
            "Flight server not initialized".to_string(),
        ))
    }
}

/// Add addresses to the global flight client manager
pub async fn add_addresses_to_client_manager(addresses: Vec<String>) -> DaftResult<()> {
    if let Some(client_manager_lock) = get_flight_client_manager() {
        let mut client_manager = client_manager_lock.write().await;
        client_manager.add_addresses(addresses).await?;
        Ok(())
    } else {
        Err(common_error::DaftError::InternalError(
            "Flight client manager not initialized".to_string(),
        ))
    }
}

/// Fetch a partition using the global flight client manager
pub async fn fetch_partition_from_clients(
    shuffle_id: &str,
    partition_idx: usize,
) -> DaftResult<Arc<daft_micropartition::MicroPartition>> {
    if let Some(client_manager_lock) = get_flight_client_manager() {
        let client_manager = client_manager_lock.read().await;
        client_manager.fetch_partition(shuffle_id, partition_idx).await
    } else {
        Err(common_error::DaftError::InternalError(
            "Flight client manager not initialized".to_string(),
        ))
    }
}
