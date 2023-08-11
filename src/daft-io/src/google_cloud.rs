use std::sync::Arc;

use google_cloud_storage::client::ClientConfig;

use google_cloud_storage::client::Client;
use google_cloud_storage::http::objects::get::GetObjectRequest;
pub(crate) struct GCSSource {
    client: Client
}

impl GCSSource {
    pub async fn get_client() -> super::Result<Arc<Self>> {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let client = Client::new(config);
        Ok(GCSSource {
            client
        }.into())
    }

    async fn get(&self, uri: &str) {

        let parsed = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;
        let container = match parsed.host_str() {
            Some(s) => Ok(s),
            None => Err(Error::InvalidUrl {
                path: uri.into(),
                source: url::ParseError::EmptyHost,
            }),
        }?;
        let key = parsed.path();



        let req = GetObjectRequest {

        };
        self.client.get_object(req)
    }
}


