use azure_core::{
    Url,
    auth::{AccessToken, TokenCredential},
    headers::HeaderName,
};

use crate::{ImdsId, ImdsManagedIdentityCredential, TokenCredentialOptions};

const ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
const API_VERSION: &str = "2019-08-01";
const SECRET_HEADER: HeaderName = HeaderName::from_static("x-identity-header");
const SECRET_ENV: &str = "IDENTITY_HEADER";

#[derive(Debug)]
pub struct VirtualMachineManagedIdentityCredential {
    credential: ImdsManagedIdentityCredential,
}

impl VirtualMachineManagedIdentityCredential {
    pub fn new(options: impl Into<TokenCredentialOptions>) -> Self {
        let endpoint = Url::parse(ENDPOINT).unwrap(); // valid url constant
        Self {
            credential: ImdsManagedIdentityCredential::new(
                options,
                endpoint,
                API_VERSION,
                SECRET_HEADER,
                SECRET_ENV,
                ImdsId::SystemAssigned,
            ),
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TokenCredential for VirtualMachineManagedIdentityCredential {
    async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        self.credential.get_token(scopes).await
    }

    async fn clear_cache(&self) -> azure_core::Result<()> {
        self.credential.clear_cache().await
    }
}
