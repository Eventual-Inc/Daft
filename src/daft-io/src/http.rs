use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use reqwest::header::RANGE;
use snafu::{IntoError, ResultExt, Snafu};

use super::object_io::{GetResult, ObjectSource};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to connect to {}: {}", path, source))]
    UnableToConnect {
        path: String,
        source: reqwest::Error,
    },

    #[snafu(display("Unable to open {}: {}", path, source))]
    UnableToOpenFile {
        path: String,
        source: reqwest::Error,
    },

    #[snafu(display("Unable to read data from {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: reqwest::Error,
    },

    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },
}

pub(crate) struct HttpSource {
    client: reqwest::Client,
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToOpenFile { path, source } => match source.status().map(|v| v.as_u16()) {
                Some(404) | Some(410) => super::Error::NotFound {
                    path,
                    source: source.into(),
                },
                None | Some(_) => super::Error::UnableToOpenFile {
                    path,
                    source: source.into(),
                },
            },
            _ => super::Error::Generic {
                store: super::SourceType::Http,
                source: error.into(),
            },
        }
    }
}

lazy_static! {
    static ref HTTP_CLIENT: Arc<HttpSource> = HttpSource {
        client: reqwest::ClientBuilder::default().build().unwrap(),
    }
    .into();
}

impl HttpSource {
    pub async fn get_client() -> super::Result<Arc<Self>> {
        Ok(HTTP_CLIENT.clone())
    }
}

#[async_trait]
impl ObjectSource for HttpSource {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult> {
        let request = self.client.get(uri);
        let request = match range {
            None => request,
            Some(range) => request.header(
                RANGE,
                format!("bytes={}-{}", range.start, range.end.saturating_sub(1)),
            ),
        };

        let response = request
            .send()
            .await
            .context(UnableToConnectSnafu::<String> { path: uri.into() })?;
        let response = response
            .error_for_status()
            .context(UnableToOpenFileSnafu::<String> { path: uri.into() })?;
        let size_bytes = response.content_length().map(|s| s as usize);
        let stream = response.bytes_stream();
        let owned_string = uri.to_owned();
        let stream = stream.map_err(move |e| {
            UnableToReadBytesSnafu::<String> {
                path: owned_string.clone(),
            }
            .into_error(e)
            .into()
        });
        Ok(GetResult::Stream(stream.boxed(), size_bytes))
    }
}
