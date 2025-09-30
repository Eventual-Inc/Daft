use std::sync::Arc;

use azure_core::error::{ErrorKind, ResultExt};
use url::Url;

use crate::env::Env;

const AZURE_AUTHORITY_HOST_ENV_KEY: &str = "AZURE_AUTHORITY_HOST";
const AZURE_PUBLIC_CLOUD: &str = "https://login.microsoftonline.com";

/// Provides options to configure how the Identity library makes authentication
/// requests to Azure Active Directory.
#[derive(Debug, Clone)]
pub struct TokenCredentialOptions {
    env: Env,
    http_client: Arc<dyn azure_core::HttpClient>,
    authority_host: String,
}

/// The default token credential options.
/// The authority host is taken from the `AZURE_AUTHORITY_HOST` environment variable if set and a valid URL.
/// If not, the default authority host is `https://login.microsoftonline.com` for the Azure public cloud.
impl Default for TokenCredentialOptions {
    fn default() -> Self {
        let env = Env::default();
        let authority_host = env
            .var(AZURE_AUTHORITY_HOST_ENV_KEY)
            .unwrap_or_else(|_| AZURE_PUBLIC_CLOUD.to_owned());
        Self {
            env: Env::default(),
            http_client: azure_core::new_http_client(),
            authority_host,
        }
    }
}

impl TokenCredentialOptions {
    #[cfg(test)]
    pub(crate) fn new(env: Env, http_client: Arc<dyn azure_core::HttpClient>) -> Self {
        Self {
            env,
            http_client,
            authority_host: AZURE_PUBLIC_CLOUD.to_owned(),
        }
    }
    /// Set the authority host for authentication requests.
    pub fn set_authority_host(&mut self, authority_host: String) {
        self.authority_host = authority_host;
    }

    /// The authority host to use for authentication requests.  The default is
    /// `https://login.microsoftonline.com`.
    pub fn authority_host(&self) -> azure_core::Result<Url> {
        Url::parse(&self.authority_host).with_context(ErrorKind::DataConversion, || {
            format!("invalid authority host URL {}", &self.authority_host)
        })
    }

    pub fn http_client(&self) -> Arc<dyn azure_core::HttpClient> {
        self.http_client.clone()
    }

    pub(crate) fn env(&self) -> &Env {
        &self.env
    }
}

impl From<Arc<dyn azure_core::HttpClient>> for TokenCredentialOptions {
    fn from(http_client: Arc<dyn azure_core::HttpClient>) -> Self {
        Self {
            http_client,
            ..Default::default()
        }
    }
}
