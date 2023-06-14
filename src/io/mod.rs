mod http;
mod object_io;
mod s3_like;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use futures::{StreamExt, TryStreamExt};

use tokio::task::JoinError;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, Utf8Array},
    error::{DaftError, DaftResult},
    io::s3_like::S3LikeSource,
};

use self::{
    http::HttpSource,
    object_io::{GetResult, ObjectSource},
};

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

lazy_static! {
    static ref OBJ_SRC_MAP: RwLock<HashMap<String, Arc<dyn ObjectSource>>> =
        RwLock::new(HashMap::new());
}

async fn get_source(scheme: &str) -> DaftResult<Arc<dyn ObjectSource>> {
    {
        if let Some(source) = OBJ_SRC_MAP.read().unwrap().get(scheme) {
            return Ok(source.clone());
        }
    }

    let new_source: Arc<dyn ObjectSource> = match scheme {
        "https" | "http" => Ok(Arc::new(HttpSource::new().await) as Arc<dyn ObjectSource>),
        "s3" => Ok(Arc::new(S3LikeSource::new().await) as Arc<dyn ObjectSource>),
        _ => Err(DaftError::ValueError(format!(
            "{scheme} not supported for IO!"
        ))),
    }?;

    let mut w_handle = OBJ_SRC_MAP.write().unwrap();
    if w_handle.get(scheme).is_none() {
        w_handle.insert(scheme.to_string(), new_source.clone());
    }
    Ok(new_source)
}

async fn single_url_get(input: String) -> DaftResult<GetResult> {
    let parsed = url::Url::parse(input.as_str())?;

    let source = get_source(parsed.scheme()).await?;
    source.get(input).await
}

async fn single_url_download(
    index: usize,
    input: Option<String>,
    raise_error_on_failure: bool,
) -> DaftResult<Option<bytes::Bytes>> {
    let value = if let Some(input) = input {
        let response = single_url_get(input).await;
        let res = match response {
            Ok(res) => res.bytes().await,
            Err(err) => Err(err),
        };
        Some(res)
    } else {
        None
    };

    match value {
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
    }
}

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
    let now = std::time::Instant::now();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let elap = now.elapsed().as_nanos();
    log::warn!("time to create rt: {elap}");

    let fetches = futures::stream::iter(urls.enumerate().map(|(i, url)| {
        let owned_url = url.map(|s| s.to_string());
        tokio::spawn(async move {
            (
                i,
                single_url_download(i, owned_url, raise_error_on_failure).await,
            )
        })
    }))
    .buffer_unordered(max_connections)
    .then(async move |r| match r {
        Ok((i, Ok(v))) => Ok((i, v)),
        Ok((_i, Err(error))) => Err(error),
        Err(error) => Err(error.into()),
    });

    let collect_future = fetches.try_collect::<Vec<_>>();
    let mut results = rt.block_on(collect_future)?;

    results.sort_by_key(|k| k.0);
    let mut offsets: Vec<i64> = Vec::with_capacity(results.len() + 1);
    offsets.push(0);
    let mut valid = Vec::with_capacity(results.len());
    valid.reserve(results.len());

    let cap_needed: usize = results
        .iter()
        .filter_map(|f| f.1.as_ref().map(|f| f.len()))
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
