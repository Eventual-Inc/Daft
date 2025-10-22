use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use common_file_formats::FileFormat;
use common_io_config::{IOConfig, ObfuscatedString, UnityConfig};
use futures::stream::BoxStream;
use itertools::Itertools;
use pyo3::{intern, prelude::*};
use snafu::ResultExt;

use crate::{
    IOClient, InvalidUrlSnafu, SourceType,
    object_io::{FileMetadata, GetResult, LSResult, ObjectSource},
    range::GetRange,
    stats::IOStatsRef,
};

fn invalid_unity_path(path: &str) -> crate::Error {
    crate::Error::NotFound {
        path: path.to_string(),
        source: Box::new(DaftError::ValueError(format!(
            "Expected Unity Catalog volume path to be in the form `vol+dbfs:/Volumes/catalog/schema/volume/path`, instead found: {}",
            path
        ))),
    }
}

pub struct UnitySource {
    /// A `daft.unity_catalog.UnityCatalog` instance
    unity_catalog: pyo3::Py<pyo3::PyAny>,
    /// map of volume name to io client and storage location
    cached_sources: tokio::sync::RwLock<HashMap<String, Arc<ClientAndLocation>>>,
}

struct ClientAndLocation {
    io_client: IOClient,
    storage_location: String,
}

impl UnitySource {
    pub async fn get_client(config: &UnityConfig) -> super::Result<Arc<Self>> {
        let Some(endpoint) = &config.endpoint else {
            return Err(super::Error::UnableToCreateClient {
                store: SourceType::Unity,
                source: Box::new(DaftError::ValueError(
                    "UnityConfig.endpoint must be provided to create a UnitySource".to_string(),
                )),
            });
        };

        let unity_catalog = Python::attach(|py| {
            Ok::<_, PyErr>(
                py.import(intern!(py, "daft.unity_catalog"))?
                    .getattr(intern!(py, "UnityCatalog"))?
                    .call1((
                        endpoint,
                        config.token.as_ref().map(ObfuscatedString::as_string),
                    ))?
                    .unbind(),
            )
        })
        .map_err(|e| super::Error::UnableToCreateClient {
            store: SourceType::Unity,
            source: Box::new(e),
        })?;

        let cached_sources = tokio::sync::RwLock::new(HashMap::new());

        Ok(Arc::new(Self {
            unity_catalog,
            cached_sources,
        }))
    }

    async fn get_or_create_io_client(&self, name: &str) -> super::Result<Arc<ClientAndLocation>> {
        {
            if let Some(client) = self.cached_sources.read().await.get(name) {
                return Ok(client.clone());
            }
        }

        let mut w_handle = self.cached_sources.write().await;
        if let Some(client) = w_handle.get(name) {
            return Ok(client.clone());
        }

        let (io_config, storage_location) = Python::attach(|py| {
            let volume = self
                .unity_catalog
                .bind(py)
                .call_method1(intern!(py, "load_volume"), (name,))?;

            let py_io_config = volume.getattr(intern!(py, "io_config"))?;
            let io_config = if py_io_config.is_none() {
                IOConfig::default()
            } else {
                py_io_config
                    .extract::<common_io_config::python::IOConfig>()?
                    .config
            };

            let storage_location = volume
                .getattr(intern!(py, "volume_info"))?
                .getattr(intern!(py, "storage_location"))?
                .extract::<String>()?;

            Ok::<_, PyErr>((io_config, storage_location))
        })
        .map_err(|e| super::Error::UnableToLoadCredentials {
            store: SourceType::Unity,
            source: Box::new(e),
        })?;

        let io_client = IOClient::new(Arc::new(io_config))?;
        let client = Arc::new(ClientAndLocation {
            io_client,
            storage_location,
        });

        w_handle.insert(name.to_string(), client.clone());
        Ok(client)
    }

    async fn volume_path_to_source_and_url(
        &self,
        path: &str,
    ) -> super::Result<(Arc<dyn ObjectSource>, String)> {
        let url = url::Url::parse(path).context(InvalidUrlSnafu { path })?;

        let mut segments = url
            .path_segments()
            .ok_or_else(|| invalid_unity_path(path))?;

        if segments.next() != Some("Volumes") {
            return Err(invalid_unity_path(path));
        }

        let catalog_name = segments.next().ok_or_else(|| invalid_unity_path(path))?;
        let schema_name = segments.next().ok_or_else(|| invalid_unity_path(path))?;
        let volume_name = segments.next().ok_or_else(|| invalid_unity_path(path))?;
        let volume_path = segments.join("/");

        let combined_name = format!("{catalog_name}.{schema_name}.{volume_name}");

        let client = self.get_or_create_io_client(&combined_name).await?;

        let source_path = format!("{}/{}", client.storage_location, volume_path);
        let source = client.io_client.get_source(&source_path).await?;

        Ok((source, source_path))
    }
}

#[async_trait]
impl ObjectSource for UnitySource {
    async fn supports_range(&self, _: &str) -> super::Result<bool> {
        Ok(true)
    }

    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let (source, source_uri) = self.volume_path_to_source_and_url(uri).await?;
        source.get(&source_uri, range, io_stats).await
    }

    async fn put(&self, uri: &str, data: Bytes, io_stats: Option<IOStatsRef>) -> super::Result<()> {
        let (source, source_uri) = self.volume_path_to_source_and_url(uri).await?;
        source.put(&source_uri, data, io_stats).await
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        let (source, source_uri) = self.volume_path_to_source_and_url(uri).await?;
        source.get_size(&source_uri, io_stats).await
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
        file_format: Option<FileFormat>,
    ) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>> {
        let (source, source_glob_path) = self.volume_path_to_source_and_url(glob_path).await?;
        source
            .glob(
                &source_glob_path,
                fanout_limit,
                page_size,
                limit,
                io_stats,
                file_format,
            )
            .await
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult> {
        let (source, source_path) = self.volume_path_to_source_and_url(path).await?;
        source
            .ls(&source_path, posix, continuation_token, page_size, io_stats)
            .await
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}
