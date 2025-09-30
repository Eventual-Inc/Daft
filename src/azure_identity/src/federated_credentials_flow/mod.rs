//! Authorize using the OAuth 2.0 client credentials flow with federated credentials.
//!
//! ```no_run
//! use azure_core::{authority_hosts::AZURE_PUBLIC_CLOUD, Url};
//! use azure_identity::{federated_credentials_flow};
//!
//! use std::env;
//! use std::error::Error;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     let client_id =
//!         env::var("CLIENT_ID").expect("Missing CLIENT_ID environment variable.");
//!     let token = env::var("FEDERATED_TOKEN").expect("Missing FEDERATED_TOKEN environment variable.");
//!     let tenant_id = env::var("TENANT_ID").expect("Missing TENANT_ID environment variable.");
//!     let subscription_id =
//!         env::var("SUBSCRIPTION_ID").expect("Missing SUBSCRIPTION_ID environment variable.");
//!
//!     let http_client = azure_core::new_http_client();
//!     // This will give you the final token to use in authorization.
//!     let token = federated_credentials_flow::perform(
//!         http_client.clone(),
//!         &client_id,
//!         &token,
//!         &["https://management.azure.com/"],
//!         &tenant_id,
//!         &AZURE_PUBLIC_CLOUD,
//!     )
//!     .await?;
//!     Ok(())
//! }
//! ```
//!
//! You can learn more about this authorization flow [here](https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow#third-case-access-token-request-with-a-federated-credential).

mod login_response;

use std::sync::Arc;

use azure_core::{
    HttpClient, Method, Request, Url, content_type,
    error::{ErrorKind, ResultExt},
    headers,
};
use login_response::LoginResponse;
use tracing::{debug, error};
use url::form_urlencoded;

/// Perform the client credentials flow
pub async fn perform(
    http_client: Arc<dyn HttpClient>,
    client_id: &str,
    client_assertion: &str,
    scopes: &[&str],
    tenant_id: &str,
    host: &Url,
) -> azure_core::Result<LoginResponse> {
    let encoded: String = form_urlencoded::Serializer::new(String::new())
        .append_pair("client_id", client_id)
        .append_pair("scope", &scopes.join(" "))
        .append_pair(
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        )
        .append_pair("client_assertion", client_assertion)
        .append_pair("grant_type", "client_credentials")
        .finish();

    let url = host
        .join(&format!("/{tenant_id}/oauth2/v2.0/token"))
        .with_context(ErrorKind::DataConversion, || {
            format!("The supplied tenant id could not be url encoded: {tenant_id}")
        })?;

    let mut req = Request::new(url, Method::Post);
    req.insert_header(
        headers::CONTENT_TYPE,
        content_type::APPLICATION_X_WWW_FORM_URLENCODED,
    );
    req.set_body(encoded);
    let rsp = http_client.execute_request(&req).await?;
    let rsp_status = rsp.status();
    debug!("rsp_status == {:?}", rsp_status);
    if rsp_status.is_success() {
        rsp.json().await
    } else {
        let (rsp_status, rsp_headers, rsp_body) = rsp.deconstruct();
        let rsp_body = rsp_body.collect().await?;
        let text = std::str::from_utf8(&rsp_body)?;
        error!("rsp_body == {:?}", text);
        Err(ErrorKind::http_response_from_parts(rsp_status, &rsp_headers, &rsp_body).into_error())
    }
}
