use azure_core::{
    Url,
    auth::{AccessToken, TokenCredential},
    error::{ErrorKind, ResultExt},
    headers::HeaderName,
};

use crate::{ImdsId, ImdsManagedIdentityCredential, TokenCredentialOptions};

const ENDPOINT_ENV: &str = "IDENTITY_ENDPOINT";
const API_VERSION: &str = "2019-08-01";
const SECRET_HEADER: HeaderName = HeaderName::from_static("x-identity-header");
const SECRET_ENV: &str = "IDENTITY_HEADER";

#[derive(Debug)]
pub struct AppServiceManagedIdentityCredential {
    credential: ImdsManagedIdentityCredential,
}

impl AppServiceManagedIdentityCredential {
    pub fn create(options: impl Into<TokenCredentialOptions>) -> azure_core::Result<Self> {
        let options = options.into();
        let env = options.env();
        let endpoint = &env
            .var(ENDPOINT_ENV)
            .with_context(ErrorKind::Credential, || {
                format!(
                    "app service credential requires {} environment variable",
                    ENDPOINT_ENV
                )
            })?;
        let endpoint = Url::parse(endpoint).with_context(ErrorKind::Credential, || {
            format!(
                "app service credential {} environment variable must be a valid URL, but is '{endpoint}'",
                ENDPOINT_ENV
            )
        })?;
        Ok(Self {
            credential: ImdsManagedIdentityCredential::new(
                options,
                endpoint,
                API_VERSION,
                SECRET_HEADER,
                SECRET_ENV,
                ImdsId::SystemAssigned,
            ),
        })
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TokenCredential for AppServiceManagedIdentityCredential {
    async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        self.credential.get_token(scopes).await
    }

    async fn clear_cache(&self) -> azure_core::Result<()> {
        self.credential.clear_cache().await
    }
}
