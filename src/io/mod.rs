mod http;
mod local;
mod object_io;
mod s3_like;

use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, RwLock},
};

use futures::{StreamExt, TryStreamExt};

use tokio::{runtime::Runtime, task::JoinError};
use url::ParseError;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, Utf8Array},
    error::{DaftError, DaftResult},
    io::s3_like::S3LikeSource,
};

use self::{
    http::HttpSource,
    local::LocalSource,
    object_io::{GetResult, ObjectSource},
};

impl From<url::ParseError> for DaftError {
    fn from(error: url::ParseError) -> Self {
        DaftError::External(error.into())
    }
}

impl From<JoinError> for DaftError {
    fn from(error: JoinError) -> Self {
        DaftError::IoError(error.into())
    }
}

lazy_static! {
    static ref OBJ_SRC_MAP: RwLock<HashMap<SourceType, Arc<dyn ObjectSource>>> =
        RwLock::new(HashMap::new());
    static ref RT: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
}

async fn get_source(source_type: SourceType) -> DaftResult<Arc<dyn ObjectSource>> {
    {
        if let Some(source) = OBJ_SRC_MAP.read().unwrap().get(&source_type) {
            return Ok(source.clone());
        }
    }

    let new_source: Arc<dyn ObjectSource> = match source_type {
        SourceType::File => Arc::new(LocalSource::new().await) as Arc<dyn ObjectSource>,
        SourceType::Http => Arc::new(HttpSource::new().await) as Arc<dyn ObjectSource>,
        SourceType::S3 => Arc::new(S3LikeSource::new().await) as Arc<dyn ObjectSource>,
    };

    let mut w_handle = OBJ_SRC_MAP.write().unwrap();
    if w_handle.get(&source_type).is_none() {
        w_handle.insert(source_type, new_source.clone());
    }
    Ok(new_source)
}

#[derive(Debug, Hash, PartialEq, std::cmp::Eq)]
enum SourceType {
    File,
    Http,
    S3,
}

fn parse_url<'a>(input: &'a str) -> DaftResult<(SourceType, Cow<'a, str>)> {
    let mut fixed_input = Cow::Borrowed(input);

    let url = match url::Url::parse(input) {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => {
            fixed_input = Cow::Owned(format!("file://{input}"));
            url::Url::parse(fixed_input.as_ref())
        }
        Err(err) => Err(err),
    }?;

    let scheme = url.scheme().to_lowercase();
    match scheme.as_ref() {
        "file" => Ok((SourceType::File, fixed_input)),
        "http" | "https" => Ok((SourceType::Http, fixed_input)),
        "s3" => Ok((SourceType::S3, fixed_input)),
        _ => Err(DaftError::ValueError(format!(
            "{scheme} not supported for IO!"
        ))),
    }
}

async fn single_url_get(input: String) -> DaftResult<GetResult> {
    let (scheme, path) = parse_url(&input)?;
    let source = get_source(scheme).await?;
    source.get(path.as_ref()).await
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
                    "Error occurred during url_download at index: {index} {} (falling back to Null)",
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
    let rt = &RT;
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
