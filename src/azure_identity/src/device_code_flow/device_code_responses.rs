use std::fmt;

use azure_core::auth::Secret;
use serde::Deserialize;

/// Error response returned from the device code flow.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DeviceCodeErrorResponse {
    /// Name of the error.
    pub error: String,
    /// Description of the error.
    pub error_description: String,
    /// Uri to get more information on this error.
    pub error_uri: String,
}

impl std::error::Error for DeviceCodeErrorResponse {}

impl fmt::Display for DeviceCodeErrorResponse {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}. {}", self.error, self.error_description)
    }
}

/// A successful token response.
#[derive(Debug, Clone, Deserialize)]
pub struct DeviceCodeAuthorization {
    /// Always `Bearer`.
    pub token_type: String,
    /// The scopes the access token is valid for.
    /// Format: Space separated strings
    pub scope: String,
    /// Number of seconds the included access token is valid for.
    pub expires_in: u64,
    /// Issued for the scopes that were requested.
    /// Format: Opaque string
    access_token: Secret,
    /// Issued if the original scope parameter included offline_access.
    /// Format: JWT
    refresh_token: Option<Secret>,
    /// Issued if the original scope parameter included the openid scope.
    /// Format: Opaque string
    id_token: Option<Secret>,
}

impl DeviceCodeAuthorization {
    /// Get the access token
    pub fn access_token(&self) -> &Secret {
        &self.access_token
    }
    /// Get the refresh token
    pub fn refresh_token(&self) -> Option<&Secret> {
        self.refresh_token.as_ref()
    }
    /// Get the id token
    pub fn id_token(&self) -> Option<&Secret> {
        self.id_token.as_ref()
    }
}
