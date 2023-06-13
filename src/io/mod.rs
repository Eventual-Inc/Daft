mod http;
mod object_io;
mod s3_like;

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use tokio::task::JoinError;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, Utf8Array},
    error::{DaftError, DaftResult},
    io::s3_like::S3LikeSource,
};

use self::{http::HttpSource, object_io::GetResult};

impl From<url::ParseError> for DaftError {
    fn from(error: url::ParseError) -> Self {
        DaftError::IoError(error.into())
    }
}

impl From<JoinError> for DaftError {
    fn from(error: JoinError) -> Self {
        DaftError::IoError(error.into())
    }
}

#[derive(Clone)]
struct MegaClient {
    pub s3_client: S3LikeSource,
}

async fn single_url_get(input: String, client: Arc<S3LikeSource>) -> anyhow::Result<GetResult> {
    use crate::io::object_io::ObjectSource;
    let parsed = url::Url::parse(input.as_str())?;
    match parsed.scheme() {
        "https" | "http" => HttpSource {}.get(input).await,
        "s3" => client.get(input).await,
        // return a DaftIoError instead
        v => panic!("protocol {v} not supported for url: {input}"),
    }
}

async fn single_url_download(
    index: usize,
    input: Option<String>,
    client: Arc<S3LikeSource>,
    raise_error_on_failure: bool,
) -> DaftResult<Option<bytes::Bytes>> {
    let value = if let Some(input) = input {
        let response = single_url_get(input, client).await;
        let res = match response {
            Ok(res) => res.bytes().await,
            Err(err) => Err(err.into()),
        };
        Some(res)
    } else {
        None
    };

    let final_value = match value {
        Some(Ok(bytes)) => Ok(Some(bytes)),
        Some(Err(err)) => match raise_error_on_failure {
            true => Err(err),
            false => {
                log::warn!(
                    "Error occurred during url_download at index: {index} {}",
                    err
                );
                Ok(None)
            }
        },
        None => Ok(None),
    };
    final_value
}

// lazy_static! {
//     /// This is an example for using doc comment attributes
//     static ref CLIENT: MegaClient = MegaClient {
//         s3_client: S3LikeSource::new()
//     };
// }

pub fn url_download<S: ToString, I: Iterator<Item = Option<S>>>(
    name: &str,
    urls: I,
    max_connections: usize,
    raise_error_on_failure: bool,
) -> DaftResult<BinaryArray> {
    if max_connections == 0 {
        return Err(DaftError::ValueError(
            "max_connections for url_download must be non-zero".into(),
        ));
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let client = Arc::new(rt.block_on(async move { S3LikeSource::new().await }));
    let fetches = futures::stream::iter(urls.enumerate().map(|(i, url)| {
        let owned_url = url.map(|s| s.to_string());
        let client_arc = client.clone();
        tokio::spawn(async move {
            (
                i,
                single_url_download(i, owned_url, client_arc, raise_error_on_failure).await,
            )
        })
    }))
    .buffer_unordered(max_connections)
    .then(async move |r| match r {
        Ok((i, Ok(v))) => Ok((i, v)),
        Ok((i, Err(error))) => Err(error),
        Err(error) => Err(error.into()),
    });

    let collect_future = fetches.try_collect::<Vec<_>>();
    let mut results = rt.block_on(async move { collect_future.await })?;

    results.sort_by_key(|k| k.0);
    let mut offsets: Vec<i64> = Vec::with_capacity(results.len() + 1);
    offsets.push(0);
    let mut valid = Vec::with_capacity(results.len());
    valid.reserve(results.len());

    let cap_needed: usize = results
        .iter()
        .filter_map(|f| f.1.as_ref().and_then(|f| Some(f.len())))
        .sum();
    let mut data = Vec::with_capacity(cap_needed);
    for (_, b) in results.into_iter() {
        match b {
            Some(b) => {
                data.extend(b.as_ref());
                offsets.push(b.len() as i64 + offsets.last().unwrap());
                valid.push(true);
            }
            None => {
                offsets.push(*offsets.last().unwrap());
                valid.push(false);
            }
        }
    }
    BinaryArray::try_from((name, data, offsets))?.with_validity(valid.as_slice())
}

impl Utf8Array {
    pub fn url_download(
        &self,
        max_connections: usize,
        raise_error_on_failure: bool,
    ) -> DaftResult<BinaryArray> {
        let urls = self.as_arrow().iter();
        url_download(self.name(), urls, max_connections, raise_error_on_failure)
    }
}
