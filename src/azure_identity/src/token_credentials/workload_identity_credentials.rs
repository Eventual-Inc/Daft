use std::{str, sync::Arc, time::Duration};

use azure_core::{
    Error, HttpClient,
    auth::{AccessToken, Secret, TokenCredential},
    error::{ErrorKind, ResultExt},
};
use time::OffsetDateTime;
use url::Url;

use crate::{
    TokenCredentialOptions, federated_credentials_flow, token_credentials::cache::TokenCache,
};

const AZURE_TENANT_ID_ENV_KEY: &str = "AZURE_TENANT_ID";
const AZURE_CLIENT_ID_ENV_KEY: &str = "AZURE_CLIENT_ID";
const AZURE_FEDERATED_TOKEN_FILE: &str = "AZURE_FEDERATED_TOKEN_FILE";
const AZURE_FEDERATED_TOKEN: &str = "AZURE_FEDERATED_TOKEN";

/// Enables authentication to Azure Active Directory using a client secret that was generated for an App Registration.
///
/// More information on how to configure a client secret can be found here:
/// <https://docs.microsoft.com/azure/active-directory/develop/quickstart-configure-app-access-web-apis#add-credentials-to-your-web-application>

#[derive(Debug)]
pub struct WorkloadIdentityCredential {
    http_client: Arc<dyn HttpClient>,
    authority_host: Url,
    tenant_id: String,
    client_id: String,
    token: Secret,
    cache: TokenCache,
}

impl WorkloadIdentityCredential {
    /// Create a new `WorkloadIdentityCredential`
    pub fn new<T>(
        http_client: Arc<dyn HttpClient>,
        authority_host: Url,
        tenant_id: String,
        client_id: String,
        token: T,
    ) -> Self
    where
        T: Into<Secret>,
    {
        Self {
            http_client,
            authority_host,
            tenant_id,
            client_id,
            token: token.into(),
            cache: TokenCache::new(),
        }
    }

    pub fn create(
        options: impl Into<TokenCredentialOptions>,
    ) -> azure_core::Result<WorkloadIdentityCredential> {
        let options = options.into();
        let http_client = options.http_client();
        let authority_host = options.authority_host()?;
        let env = options.env();
        let tenant_id =
            env.var(AZURE_TENANT_ID_ENV_KEY)
                .with_context(ErrorKind::Credential, || {
                    format!(
                        "working identity credential requires {} environment variable",
                        AZURE_TENANT_ID_ENV_KEY
                    )
                })?;
        let client_id =
            env.var(AZURE_CLIENT_ID_ENV_KEY)
                .with_context(ErrorKind::Credential, || {
                    format!(
                        "working identity credential requires {} environment variable",
                        AZURE_CLIENT_ID_ENV_KEY
                    )
                })?;

        if let Ok(token) = env
            .var(AZURE_FEDERATED_TOKEN)
            .map_kind(ErrorKind::Credential)
        {
            return Ok(WorkloadIdentityCredential::new(
                http_client,
                authority_host,
                tenant_id,
                client_id,
                token,
            ));
        }

        if let Ok(token_file) = env
            .var(AZURE_FEDERATED_TOKEN_FILE)
            .map_kind(ErrorKind::Credential)
        {
            let token = std::fs::read_to_string(token_file.clone()).with_context(
                ErrorKind::Credential,
                || {
                    format!(
                        "failed to read federated token from file {}",
                        token_file.as_str()
                    )
                },
            )?;
            return Ok(WorkloadIdentityCredential::new(
                http_client,
                authority_host,
                tenant_id,
                client_id,
                token,
            ));
        }

        Err(Error::with_message(ErrorKind::Credential, || {
            format!(
                "working identity credential requires {AZURE_FEDERATED_TOKEN} or {AZURE_FEDERATED_TOKEN_FILE} environment variables"
            )
        }))
    }

    async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        let res: AccessToken = federated_credentials_flow::perform(
            self.http_client.clone(),
            &self.client_id,
            self.token.secret(),
            scopes,
            &self.tenant_id,
            &self.authority_host,
        )
        .await
        .map(|r| {
            AccessToken::new(
                r.access_token().clone(),
                OffsetDateTime::now_utc() + Duration::from_secs(r.expires_in),
            )
        })
        .context(ErrorKind::Credential, "request token error")?;
        Ok(res)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TokenCredential for WorkloadIdentityCredential {
    async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        self.cache.get_token(scopes, self.get_token(scopes)).await
    }

    async fn clear_cache(&self) -> azure_core::Result<()> {
        self.cache.clear().await
    }
}
