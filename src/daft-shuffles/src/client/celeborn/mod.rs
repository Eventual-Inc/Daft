//! Celeborn shuffle client abstraction and implementations.
//!
//! See [`client::CelebornClient`] for the trait definition and
//! [`ffi::ShuffleCelebornClient`] for the implementation behind it.

use std::sync::Arc;

use common_error::DaftResult;

mod client;
mod ffi;
#[cfg(test)]
mod integration_tests;

pub use client::{CelebornClient, CelebornClientConfig, PartitionDataStream};
pub use ffi::ShuffleCelebornClient;

/// Create a connected Celeborn client from connection-level configuration.
///
/// Returns a real FFI-backed [`ShuffleCelebornClient`] that connects to
/// the Celeborn LifecycleManager.
///
/// # Arguments
/// * `config` - Connection-level Celeborn configuration (lm_host, lm_port,
///   app_id, and native `celeborn.*` properties).
pub fn connect_celeborn_client(
    config: &CelebornClientConfig,
) -> DaftResult<Arc<dyn CelebornClient>> {
    let client = ShuffleCelebornClient::connect(config)?;
    Ok(Arc::new(client))
}
