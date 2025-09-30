use azure_core::{
    auth::{AccessToken, TokenCredential},
    error::{Error, ErrorKind},
};

#[cfg(feature = "client_certificate")]
pub use crate::token_credentials::ClientCertificateCredential;
use crate::token_credentials::{
    ClientSecretCredential, TokenCredentialOptions, WorkloadIdentityCredential,
};

#[derive(Debug)]
pub(crate) enum EnvironmentCredentialKind {
    ClientSecret(ClientSecretCredential),
    WorkloadIdentity(WorkloadIdentityCredential),
    #[cfg(feature = "client_certificate")]
    ClientCertificate(ClientCertificateCredential),
}

/// Enables authentication with Workflows Identity if either `AZURE_FEDERATED_TOKEN` or `AZURE_FEDERATED_TOKEN_FILE` is set,
/// otherwise enables authentication to Azure Active Directory using client secret, or a username and password.
///
///
/// Details configured in the following environment variables:
///
/// | Variable                            | Description                                      |
/// |-------------------------------------|--------------------------------------------------|
/// | `AZURE_TENANT_ID`                   | The Azure Active Directory tenant(directory) ID. |
/// | `AZURE_CLIENT_ID`                   | The client(application) ID of an App Registration in the tenant. |
/// | `AZURE_CLIENT_SECRET`               | A client secret that was generated for the App Registration. |
/// | `AZURE_FEDERATED_TOKEN_FILE`        | Path to an federated token file. Variable is present in pods with aks workload identities. |  // codespell:ignore aks
///
/// This credential ultimately uses a `WorkloadIdentityCredential` or a`ClientSecretCredential` to perform the authentication using
/// these details.
/// Please consult the documentation of that class for more details.
#[derive(Debug)]
pub struct EnvironmentCredential {
    source: EnvironmentCredentialKind,
}

impl EnvironmentCredential {
    pub fn create(
        options: impl Into<TokenCredentialOptions>,
    ) -> azure_core::Result<EnvironmentCredential> {
        let options = options.into();
        if let Ok(credential) = WorkloadIdentityCredential::create(options.clone()) {
            return Ok(Self {
                source: EnvironmentCredentialKind::WorkloadIdentity(credential),
            });
        }
        if let Ok(credential) = ClientSecretCredential::create(options.clone()) {
            return Ok(Self {
                source: EnvironmentCredentialKind::ClientSecret(credential),
            });
        }
        #[cfg(feature = "client_certificate")]
        if let Ok(credential) = ClientCertificateCredential::create(options.clone()) {
            return Ok(Self {
                source: EnvironmentCredentialKind::ClientCertificate(credential),
            });
        }
        Err(Error::message(
            ErrorKind::Credential,
            "no valid environment credential providers",
        ))
    }

    #[cfg(test)]
    pub(crate) fn source(&self) -> &EnvironmentCredentialKind {
        &self.source
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TokenCredential for EnvironmentCredential {
    async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        match &self.source {
            EnvironmentCredentialKind::ClientSecret(credential) => {
                credential.get_token(scopes).await
            }
            EnvironmentCredentialKind::WorkloadIdentity(credential) => {
                credential.get_token(scopes).await
            }
            #[cfg(feature = "client_certificate")]
            EnvironmentCredentialKind::ClientCertificate(credential) => {
                credential.get_token(scopes).await
            }
        }
    }

    async fn clear_cache(&self) -> azure_core::Result<()> {
        match &self.source {
            EnvironmentCredentialKind::ClientSecret(credential) => credential.clear_cache().await,
            EnvironmentCredentialKind::WorkloadIdentity(credential) => {
                credential.clear_cache().await
            }
            #[cfg(feature = "client_certificate")]
            EnvironmentCredentialKind::ClientCertificate(credential) => {
                credential.clear_cache().await
            }
        }
    }
}
