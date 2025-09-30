//! Access to token credentials through various means
//!
//! Supported means currently include:
//! * The environment
//! * Azure CLI credentials cache
//! * Managed identity
//! * Client secret
mod app_service_managed_identity_credential;
#[cfg(not(target_arch = "wasm32"))]
mod azure_cli_credentials;
#[cfg(feature = "azureauth_cli")]
#[cfg(not(target_arch = "wasm32"))]
mod azureauth_cli_credentials;
mod cache;
#[cfg(feature = "client_certificate")]
mod client_certificate_credentials;
mod client_secret_credentials;
mod default_credentials;
mod environment_credentials;
mod imds_managed_identity_credentials;
mod options;
mod specific_azure_credential;
mod virtual_machine_managed_identity_credential;
mod workload_identity_credentials;

pub use app_service_managed_identity_credential::*;
#[cfg(not(target_arch = "wasm32"))]
pub use azure_cli_credentials::*;
#[cfg(feature = "azureauth_cli")]
#[cfg(not(target_arch = "wasm32"))]
pub use azureauth_cli_credentials::*;
#[cfg(feature = "client_certificate")]
pub use client_certificate_credentials::*;
pub use client_secret_credentials::*;
pub use default_credentials::*;
pub use environment_credentials::*;
pub(crate) use imds_managed_identity_credentials::*;
pub use options::*;
pub use specific_azure_credential::*;
pub use virtual_machine_managed_identity_credential::*;
pub use workload_identity_credentials::*;
