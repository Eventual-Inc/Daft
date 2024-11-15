use std::{borrow::Cow, collections::HashMap, ops::Range, sync::Arc};

use common_error::DaftResult;
use common_file_formats::FileFormat;
use common_io_config::IOConfig;
use futures::stream::BoxStream;
use lazy_static::lazy_static;
use snafu::ResultExt;

use crate::{object_source::{FileMetadata, GetResult, ObjectSource, StreamingRetryParams}, stats::{IOStatsContext, IOStatsRef}, SourceType};


#[derive(Default)]
pub struct IOClient {
    source_type_to_store: tokio::sync::RwLock<HashMap<SourceType, Arc<dyn ObjectSource>>>,
    config: Arc<IOConfig>,
}

impl IOClient {
    pub fn new(config: Arc<IOConfig>) -> crate::Result<Self> {
        Ok(Self {
            source_type_to_store: tokio::sync::RwLock::new(HashMap::new()),
            config,
        })
    }
    pub fn parse_url(input: &str) -> crate::Result<(SourceType, Cow<'_, str>)> {
        let mut fixed_input = Cow::Borrowed(input);
        // handle tilde `~` expansion
        if input.starts_with("~/") {
            return home::home_dir()
                .and_then(|home_dir| {
                    let expanded = home_dir.join(&input[2..]);
                    let input = expanded.to_str()?;
    
                    Some((SourceType::File, Cow::Owned(format!("file://{input}"))))
                })
                .ok_or_else(|| crate::Error::InvalidArgument {
                    msg: "Could not convert expanded path to string".to_string(),
                });
        }
    
        let url = match url::Url::parse(input) {
            Ok(url) => Ok(url),
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                fixed_input = Cow::Owned(format!("file://{input}"));
    
                url::Url::parse(fixed_input.as_ref())
            }
            Err(err) => Err(err),
        }
        .context(crate::InvalidUrlSnafu { path: input })?;
    
        let scheme = url.scheme().to_lowercase();
        match scheme.as_ref() {
            "file" => Ok((SourceType::File, fixed_input)),
            "http" | "https" => Ok((SourceType::Http, fixed_input)),
            "s3" | "s3a" => Ok((SourceType::S3, fixed_input)),
            "az" | "abfs" | "abfss" => Ok((SourceType::AzureBlob, fixed_input)),
            "gcs" | "gs" => Ok((SourceType::GCS, fixed_input)),
            "hf" => Ok((SourceType::HF, fixed_input)),
            #[cfg(target_env = "msvc")]
            _ if scheme.len() == 1 && ("a" <= scheme.as_str() && (scheme.as_str() <= "z")) => {
                Ok((SourceType::File, Cow::Owned(format!("file://{input}"))))
            }
            _ => Err(crate::Error::NotImplementedSource { store: scheme }),
        }
    }
    
    async fn get_source(&self, input: &str) -> crate::Result<Arc<dyn ObjectSource>> {
        let (source_type, path) = Self::parse_url(input)?;

        {
            if let Some(client) = self.source_type_to_store.read().await.get(&source_type) {
                return Ok(client.clone());
            }
        }
        let mut w_handle = self.source_type_to_store.write().await;

        if let Some(client) = w_handle.get(&source_type) {
            return Ok(client.clone());
        }

        match source_type {
            _ => todo!("{}", source_type)
            // SourceType::File => LocalSource::get_client().await? as Arc<dyn ObjectSource>,
            // SourceType::Http => {
            //     HttpSource::get_client(&self.config.http).await? as Arc<dyn ObjectSource>
            // }
            // SourceType::S3 => {
            //     S3LikeSource::get_client(&self.config.s3).await? as Arc<dyn ObjectSource>
            // }
            // SourceType::AzureBlob => {
            //     AzureBlobSource::get_client(&self.config.azure, &path).await?
            //         as Arc<dyn ObjectSource>
            // }

            // SourceType::GCS => {
            //     GCSSource::get_client(&self.config.gcs).await? as Arc<dyn ObjectSource>
            // }
            // SourceType::HF => {
            //     HFSource::get_client(&self.config.http).await? as Arc<dyn ObjectSource>
            // }
        }

        // if w_handle.get(&source_type).is_none() {
        //     w_handle.insert(source_type, new_source.clone());
        // }
        // Ok(new_source)
    }

    pub async fn glob(
        &self,
        input: String,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
        file_format: Option<FileFormat>,
    ) -> crate::Result<BoxStream<'static, crate::Result<FileMetadata>>> {
        let source = self.get_source(&input).await?;
        let files = source
            .glob(
                input.as_str(),
                fanout_limit,
                page_size,
                limit,
                io_stats,
                file_format,
            )
            .await?;
        Ok(files)
    }

    pub async fn single_url_get(
        &self,
        input: String,
        range: Option<Range<usize>>,
        io_stats: Option<IOStatsRef>,
    ) -> crate::Result<GetResult> {
        let (_, path) = Self::parse_url(&input)?;
        let source = self.get_source(&input).await?;
        let get_result = source
            .get(path.as_ref(), range.clone(), io_stats.clone())
            .await?;
        Ok(get_result.with_retry(StreamingRetryParams::new(source, input, range, io_stats)))
    }

    pub async fn single_url_put(
        &self,
        dest: &str,
        data: bytes::Bytes,
        io_stats: Option<IOStatsRef>,
    ) -> crate::Result<()> {
        let (_, path) = Self::parse_url(dest)?;
        let source = self.get_source(dest).await?;
        source.put(path.as_ref(), data, io_stats.clone()).await
    }

    pub async fn single_url_get_size(
        &self,
        input: String,
        io_stats: Option<IOStatsRef>,
    ) -> crate::Result<usize> {
        let (_, path) = Self::parse_url(&input)?;
        let source = self.get_source(&input).await?;
        source.get_size(path.as_ref(), io_stats).await
    }

    pub async fn single_url_download(
        &self,
        index: usize,
        input: Option<String>,
        raise_error_on_failure: bool,
        io_stats: Option<IOStatsRef>,
    ) -> crate::Result<Option<bytes::Bytes>> {
        let value = if let Some(input) = input {
            let response = self.single_url_get(input, None, io_stats).await;
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
            Some(Err(err)) => {
                if raise_error_on_failure {
                    Err(err)
                } else {
                    log::warn!(
                    "Error occurred during url_download at index: {index} {} (falling back to Null)",
                    err
                );
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    pub async fn single_url_upload(
        &self,
        index: usize,
        dest: String,
        data: Option<bytes::Bytes>,
        io_stats: Option<IOStatsRef>,
    ) -> crate::Result<Option<String>> {
        let value = if let Some(data) = data {
            let response = self.single_url_put(dest.as_str(), data, io_stats).await;
            Some(response)
        } else {
            None
        };

        match value {
            Some(Ok(())) => Ok(Some(dest)),
            Some(Err(err)) => {
                log::warn!(
                    "Error occurred during file upload at index: {index} {} (falling back to Null)",
                    err
                );
                Err(err)
            }
            None => Ok(None),
        }
    }
}
type CacheKey = (bool, Arc<IOConfig>);

lazy_static! {
    static ref CLIENT_CACHE: std::sync::RwLock<HashMap<CacheKey, Arc<IOClient>>> =
        std::sync::RwLock::new(HashMap::new());
}

pub fn get_io_client(multi_thread: bool, config: Arc<IOConfig>) -> DaftResult<Arc<IOClient>> {
    let read_handle = CLIENT_CACHE.read().unwrap();
    let key = (multi_thread, config.clone());
    if let Some(client) = read_handle.get(&key) {
        Ok(client.clone())
    } else {
        drop(read_handle);

        let mut w_handle = CLIENT_CACHE.write().unwrap();
        if let Some(client) = w_handle.get(&key) {
            Ok(client.clone())
        } else {
            let client = Arc::new(IOClient::new(config)?);
            w_handle.insert(key, client.clone());
            Ok(client)
        }
    }
}