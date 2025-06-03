use std::{collections::HashMap, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use common_file_formats::FileFormat;
use common_io_config::unity::UnityCatalog;
use futures::stream::BoxStream;
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    object_io::{FileMetadata, GetResult, LSResult, ObjectSource},
    stats::IOStatsRef,
    IOClient, InvalidUrlSnafu, SourceType,
};

fn invalid_unity_path(path: &str) -> crate::Error {
    crate::Error::NotFound {
        path: path.to_string(),
        source: Box::new(DaftError::ValueError(format!(
            "Expected Unity Catalog volume path to be in the form `dbfs:/Volumes/catalog/schema/volume/path`, instead found: {}",
            path
        ))),
    }
}

pub struct UnitySource {
    catalog: Arc<UnityCatalog>,
    /// map of volume name to source and storage location
    cached_sources: tokio::sync::RwLock<HashMap<String, ClientWithLocation>>,
}

#[derive(Clone)]
struct ClientWithLocation {
    io_client: Arc<IOClient>,
    storage_location: String,
}

impl UnitySource {
    pub fn get_client(catalog: Arc<UnityCatalog>) -> Arc<Self> {
        Arc::new(Self {
            catalog,
            cached_sources: tokio::sync::RwLock::new(HashMap::new()),
        })
    }

    async fn get_or_create_io_client(&self, name: &str) -> super::Result<ClientWithLocation> {
        {
            if let Some(client) = self.cached_sources.read().await.get(name) {
                return Ok(client.clone());
            }
        }

        let mut w_handle = self.cached_sources.write().await;
        if let Some(client) = w_handle.get(name) {
            return Ok(client.clone());
        }

        let volume = self
            .catalog
            .load_volume(name)
            .map_err(|e| crate::Error::Generic {
                store: SourceType::Unity,
                source: Box::new(e),
            })?;

        let io_config = volume.io_config.unwrap_or_default();
        let io_client = Arc::new(IOClient::new(io_config)?);

        let client = ClientWithLocation {
            io_client,
            storage_location: volume.storage_location,
        };

        w_handle.insert(name.to_string(), client.clone());
        Ok(client)
    }

    async fn volume_path_to_source_and_url(
        &self,
        path: &str,
    ) -> super::Result<(Arc<dyn ObjectSource>, String)> {
        let url = url::Url::parse(path).context(InvalidUrlSnafu { path })?;

        debug_assert_eq!(
            url.scheme().to_lowercase(),
            "dbfs",
            "UnitySource should only be used for paths with scheme `dbfs`."
        );

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

        let ClientWithLocation {
            io_client,
            storage_location,
        } = self.get_or_create_io_client(&combined_name).await?;

        let source_path = format!("{}/{}", storage_location, volume_path);
        let source = io_client.get_source(&source_path).await?;

        Ok((source, source_path))
    }
}

#[async_trait]
impl ObjectSource for UnitySource {
    async fn get(
        &self,
        uri: &str,
        range: Option<Range<usize>>,
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
}
