//! Authentication helpers for the Azure Blob source, built on the official
//! GA `azure_core` / `azure_identity` / `azure_storage_blob` SDKs.
//!
//! The GA SDK only supports Entra ID (`TokenCredential`) auth natively, so this
//! module fills the gaps that the deprecated `azure_storage` 0.21 crates used
//! to cover:
//! - [`StaticBearerCredential`]: wraps a user-supplied bearer token (e.g.
//!   Fabric/OneLake tokens from `notebookutils.credentials.getToken`).
//! - [`SharedKeyAuthorizationPolicy`]: signs each request with a storage
//!   account access key (SharedKey), ported from the old
//!   `azure_storage::authorization_policy`.
//! - [`SasTokenPolicy`]: appends a shared access signature's query parameters
//!   to each request.

use std::sync::Arc;

use async_trait::async_trait;
use azure_core::{
    Result,
    credentials::{AccessToken, Secret, TokenCredential, TokenRequestOptions},
    hmac::hmac_sha256,
    http::{
        Context, Method, Request, Url,
        headers::{AUTHORIZATION, HeaderName, Headers, MS_DATE},
        policies::{Policy, PolicyResult},
    },
    time::OffsetDateTime,
};

// Common HTTP header names used in the SharedKey string-to-sign.
// (The GA `azure_core` no longer exports constants for all of these.)
const CONTENT_ENCODING: HeaderName = HeaderName::from_static("content-encoding");
const CONTENT_LANGUAGE: HeaderName = HeaderName::from_static("content-language");
const CONTENT_LENGTH: HeaderName = HeaderName::from_static("content-length");
const CONTENT_MD5: HeaderName = HeaderName::from_static("content-md5");
const CONTENT_TYPE: HeaderName = HeaderName::from_static("content-type");
const DATE: HeaderName = HeaderName::from_static("date");
const IF_MODIFIED_SINCE: HeaderName = HeaderName::from_static("if-modified-since");
const IF_MATCH: HeaderName = HeaderName::from_static("if-match");
const IF_NONE_MATCH: HeaderName = HeaderName::from_static("if-none-match");
const IF_UNMODIFIED_SINCE: HeaderName = HeaderName::from_static("if-unmodified-since");
const RANGE: HeaderName = HeaderName::from_static("range");

/// A [`TokenCredential`] that returns a fixed, user-supplied bearer token.
///
/// This is used for `AzureConfig.bearer_token`, notably by Microsoft Fabric /
/// OneLake users who mint tokens via `notebookutils.credentials.getToken`.
#[derive(Debug)]
pub(crate) struct StaticBearerCredential(Secret);

impl StaticBearerCredential {
    pub(crate) fn new(token: String) -> Arc<Self> {
        Arc::new(Self(Secret::new(token)))
    }
}

#[async_trait]
impl TokenCredential for StaticBearerCredential {
    async fn get_token(
        &self,
        _scopes: &[&str],
        _options: Option<TokenRequestOptions<'_>>,
    ) -> Result<AccessToken> {
        // The token is static so the expiry is nominal; the bearer auth policy
        // will simply keep reusing it.
        Ok(AccessToken {
            token: self.0.clone(),
            expires_on: OffsetDateTime::now_utc() + azure_core::time::Duration::minutes(30),
        })
    }
}

/// A per-try pipeline [`Policy`] that signs requests with an Azure Storage
/// account access key ("SharedKey" authorization).
///
/// The GA SDK does not support SharedKey auth by design (Azure discourages
/// account keys), but Daft's `AzureConfig.access_key` and local azurite
/// testing rely on it. The signing algorithm is ported from the deprecated
/// `azure_storage` crate and costs one local HMAC-SHA256 per request.
#[derive(Debug, Clone)]
pub(crate) struct SharedKeyAuthorizationPolicy {
    account: String,
    key: Secret,
}

impl SharedKeyAuthorizationPolicy {
    pub(crate) fn new(account: String, key: String) -> Arc<Self> {
        Arc::new(Self {
            account,
            key: Secret::new(key),
        })
    }
}

#[async_trait]
impl Policy for SharedKeyAuthorizationPolicy {
    async fn send(
        &self,
        ctx: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        assert!(
            !next.is_empty(),
            "SharedKeyAuthorizationPolicy cannot be the last policy of a pipeline"
        );

        // If the request already carries a SAS signature, don't double-sign.
        if !request.url().query_pairs().any(|(k, _)| &*k == "sig") {
            // SharedKey requires a date header. The GA pipeline does not set
            // `x-ms-date` itself (bearer auth does not need it), so set it here,
            // inside the per-try policy, to keep it fresh across retries.
            let now = azure_core::time::to_rfc7231(&OffsetDateTime::now_utc());
            request.insert_header(MS_DATE, now);

            let auth = generate_authorization(
                request.headers(),
                request.url(),
                &request.method(),
                &self.account,
                &self.key,
            )?;
            request.insert_header(AUTHORIZATION, auth);
        }

        next[0].send(ctx, request, &next[1..]).await
    }
}

/// A per-try pipeline [`Policy`] that appends a shared access signature's
/// query parameters to each request URL.
#[derive(Debug, Clone)]
pub(crate) struct SasTokenPolicy {
    pairs: Vec<(String, String)>,
}

impl SasTokenPolicy {
    pub(crate) fn new(sas_token: &str) -> Arc<Self> {
        // Accept both `?sv=...&sig=...` and `sv=...&sig=...` forms.
        let sas_token = sas_token.trim_start_matches('?');
        let pairs = url::form_urlencoded::parse(sas_token.as_bytes())
            .into_owned()
            .collect();
        Arc::new(Self { pairs })
    }
}

#[async_trait]
impl Policy for SasTokenPolicy {
    async fn send(
        &self,
        ctx: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        // Ensure the signature param is not already present.
        if !request.url().query_pairs().any(|(k, _)| &*k == "sig") {
            request
                .url_mut()
                .query_pairs_mut()
                .extend_pairs(self.pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        }
        next[0].send(ctx, request, &next[1..]).await
    }
}

fn generate_authorization(
    h: &Headers,
    u: &Url,
    method: &Method,
    account: &str,
    key: &Secret,
) -> Result<String> {
    let str_to_sign = string_to_sign(h, u, method, account);
    let auth = hmac_sha256(&str_to_sign, key)?;
    Ok(format!("SharedKey {account}:{auth}"))
}

fn add_if_exists<'a>(h: &'a Headers, key: &HeaderName) -> &'a str {
    h.get_optional_str(key).unwrap_or_default()
}

/// <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>
fn string_to_sign(h: &Headers, u: &Url, method: &Method, account: &str) -> String {
    // Content length must only be specified if != 0. This is valid from 2015-02-21.
    let content_length = h
        .get_optional_str(&CONTENT_LENGTH)
        .filter(|&v| v != "0")
        .unwrap_or_default();
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}{}",
        method.as_ref(),
        add_if_exists(h, &CONTENT_ENCODING),
        add_if_exists(h, &CONTENT_LANGUAGE),
        content_length,
        add_if_exists(h, &CONTENT_MD5),
        add_if_exists(h, &CONTENT_TYPE),
        add_if_exists(h, &DATE),
        add_if_exists(h, &IF_MODIFIED_SINCE),
        add_if_exists(h, &IF_MATCH),
        add_if_exists(h, &IF_NONE_MATCH),
        add_if_exists(h, &IF_UNMODIFIED_SINCE),
        add_if_exists(h, &RANGE),
        canonicalize_header(h),
        canonicalized_resource(account, u)
    )
}

fn canonicalize_header(headers: &Headers) -> String {
    let mut names = headers
        .iter()
        .filter_map(|(k, _)| (k.as_str().starts_with("x-ms")).then_some(k))
        .collect::<Vec<_>>();
    names.sort_unstable();

    let mut result = String::new();

    for header_name in names {
        let value = headers.get_optional_str(header_name).unwrap();
        let name = header_name.as_str();
        result = format!("{result}{name}:{value}\n");
    }
    result
}

fn canonicalized_resource(account: &str, uri: &Url) -> String {
    let mut can_res: String = String::new();
    can_res += "/";
    can_res += account;

    for p in uri.path_segments().into_iter().flatten() {
        can_res.push('/');
        can_res.push_str(p);
    }
    can_res += "\n";

    // Query parameters.
    let query_pairs = uri.query_pairs();
    {
        let mut qps: Vec<String> = Vec::new();
        for (q, _) in query_pairs {
            if !(qps.iter().any(|x| x == &*q)) {
                qps.push(q.into_owned());
            }
        }

        qps.sort();

        for qparam in qps {
            // Find correct parameter.
            let ret = lexy_sort(query_pairs, &qparam);

            can_res = can_res + &qparam.to_lowercase() + ":";

            for (i, item) in ret.iter().enumerate() {
                if i > 0 {
                    can_res += ",";
                }
                can_res += item;
            }

            can_res += "\n";
        }
    };

    can_res[0..can_res.len() - 1].to_owned()
}

fn lexy_sort<'a>(
    vec: impl Iterator<Item = (std::borrow::Cow<'a, str>, std::borrow::Cow<'a, str>)> + 'a,
    query_param: &str,
) -> Vec<std::borrow::Cow<'a, str>> {
    let mut values = vec
        .filter(|(k, _)| *k == query_param)
        .map(|(_, v)| v)
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_to_sign_canonicalizes_request() {
        let mut headers = Headers::new();
        headers.insert(MS_DATE, "Wed, 05 Aug 2026 20:00:00 GMT");
        headers.insert(
            HeaderName::from_static("x-ms-version"),
            "2025-11-05".to_string(),
        );
        let url = Url::parse(
            "https://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=list",
        )
        .unwrap();

        let str_to_sign = string_to_sign(&headers, &url, &Method::Get, "myaccount");
        assert_eq!(
            str_to_sign,
            "GET\n\n\n\n\n\n\n\n\n\n\n\n\
             x-ms-date:Wed, 05 Aug 2026 20:00:00 GMT\n\
             x-ms-version:2025-11-05\n\
             /myaccount/mycontainer\n\
             comp:list\n\
             restype:container"
        );
    }

    #[test]
    fn generate_authorization_has_shared_key_format() {
        let mut headers = Headers::new();
        headers.insert(MS_DATE, "Wed, 05 Aug 2026 20:00:00 GMT");
        let url =
            Url::parse("https://myaccount.blob.core.windows.net/mycontainer/some/blob").unwrap();
        // Base64-encoded key, as provided by Azure.
        let key = Secret::new(azure_core::base64::encode("supersecretkey"));

        let auth = generate_authorization(&headers, &url, &Method::Get, "myaccount", &key).unwrap();
        assert!(auth.starts_with("SharedKey myaccount:"), "got: {auth}");
        // The signature is base64, so it should decode.
        let sig = auth.strip_prefix("SharedKey myaccount:").unwrap();
        assert!(azure_core::base64::decode(sig).is_ok());
    }

    #[tokio::test]
    async fn static_bearer_credential_returns_token() {
        let cred = StaticBearerCredential::new("my-fabric-token".to_string());
        let token = cred
            .get_token(&["https://storage.azure.com/.default"], None)
            .await
            .unwrap();
        assert_eq!(token.token.secret(), "my-fabric-token");
    }

    #[test]
    fn sas_token_policy_parses_with_and_without_question_mark() {
        for sas in [
            "sv=2022-11-02&ss=b&sig=abc123",
            "?sv=2022-11-02&ss=b&sig=abc123",
        ] {
            let policy = SasTokenPolicy::new(sas);
            assert_eq!(
                policy.pairs,
                vec![
                    ("sv".to_string(), "2022-11-02".to_string()),
                    ("ss".to_string(), "b".to_string()),
                    ("sig".to_string(), "abc123".to_string()),
                ]
            );
        }
    }
}
