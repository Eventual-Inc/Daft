use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use common_file_formats::FileFormat;
use common_io_config::{GravitinoConfig, IOConfig};
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

fn invalid_gravitino_path(path: &str) -> crate::Error {
    crate::Error::NotFound {
        path: path.to_string(),
        source: Box::new(DaftError::ValueError(format!(
            "Expected Gravitino fileset path to be in the form `gvfs://fileset/catalog/schema/fileset/path`, instead found: {}",
            path
        ))),
    }
}

pub struct GravitinoSource {
    /// A `daft.gravitino.GravitinoClient` instance
    gravitino_client: pyo3::Py<pyo3::PyAny>,
    /// map of fileset name to io client and storage location
    cached_sources: tokio::sync::RwLock<HashMap<String, Arc<ClientAndLocation>>>,
}

struct ClientAndLocation {
    io_client: IOClient,
    storage_location: String,
}

impl GravitinoSource {
    pub async fn get_client(config: &GravitinoConfig) -> super::Result<Arc<Self>> {
        let Some(endpoint) = &config.endpoint else {
            return Err(super::Error::UnableToCreateClient {
                store: SourceType::Gravitino,
                source: Box::new(DaftError::ValueError(
                    "GravitinoConfig.endpoint must be provided to create a GravitinoSource"
                        .to_string(),
                )),
            });
        };

        let Some(metalake_name) = &config.metalake_name else {
            return Err(super::Error::UnableToCreateClient {
                store: SourceType::Gravitino,
                source: Box::new(DaftError::ValueError(
                    "GravitinoConfig.metalake_name must be provided to create a GravitinoSource"
                        .to_string(),
                )),
            });
        };

        let gravitino_client = Python::attach(|py| {
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("auth_type", config.auth_type.as_deref().unwrap_or("simple"))?;
            if let Some(username) = &config.username {
                kwargs.set_item("username", username)?;
            }
            if let Some(password) = &config.password {
                kwargs.set_item("password", password.as_string())?;
            }
            if let Some(token) = &config.token {
                kwargs.set_item("token", token.as_string())?;
            }

            Ok::<_, PyErr>(
                py.import(intern!(py, "daft.gravitino.gravitino_catalog"))?
                    .getattr(intern!(py, "GravitinoClient"))?
                    .call((endpoint, metalake_name), Some(&kwargs))?
                    .unbind(),
            )
        })
        .map_err(|e| super::Error::UnableToCreateClient {
            store: SourceType::Gravitino,
            source: Box::new(e),
        })?;

        let cached_sources = tokio::sync::RwLock::new(HashMap::new());

        Ok(Arc::new(Self {
            gravitino_client,
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
            let fileset = self
                .gravitino_client
                .bind(py)
                .call_method1(intern!(py, "load_fileset"), (name,))?;

            let py_io_config = fileset.getattr(intern!(py, "io_config"))?;
            let io_config = if py_io_config.is_none() {
                IOConfig::default()
            } else {
                py_io_config
                    .extract::<common_io_config::python::IOConfig>()?
                    .config
            };

            let storage_location = fileset
                .getattr(intern!(py, "fileset_info"))?
                .getattr(intern!(py, "storage_location"))?
                .extract::<String>()?;

            Ok::<_, PyErr>((io_config, storage_location))
        })
        .map_err(|e| super::Error::UnableToLoadCredentials {
            store: SourceType::Gravitino,
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

    async fn fileset_path_to_source_and_url(
        &self,
        path: &str,
    ) -> super::Result<(Arc<dyn ObjectSource>, String)> {
        let url = url::Url::parse(path).context(InvalidUrlSnafu { path })?;

        // Check that the scheme is gvfs and host is fileset
        if url.scheme() != "gvfs" {
            return Err(invalid_gravitino_path(path));
        }

        if url.host_str() != Some("fileset") {
            return Err(invalid_gravitino_path(path));
        }

        let mut segments = url
            .path_segments()
            .ok_or_else(|| invalid_gravitino_path(path))?;

        let catalog_name = segments
            .next()
            .ok_or_else(|| invalid_gravitino_path(path))?;
        let schema_name = segments
            .next()
            .ok_or_else(|| invalid_gravitino_path(path))?;
        let fileset_name = segments
            .next()
            .ok_or_else(|| invalid_gravitino_path(path))?;
        let fileset_path = segments.join("/");

        let combined_name = format!("{catalog_name}.{schema_name}.{fileset_name}");

        let client = self.get_or_create_io_client(&combined_name).await?;

        let source_path = if fileset_path.is_empty() {
            client.storage_location.clone()
        } else {
            format!("{}/{}", client.storage_location, fileset_path)
        };

        let source = client.io_client.get_source(&source_path).await?;

        Ok((source, source_path))
    }
}

#[async_trait]
impl ObjectSource for GravitinoSource {
    async fn supports_range(&self, _: &str) -> super::Result<bool> {
        Ok(true)
    }

    async fn create_multipart_writer(
        self: Arc<Self>,
        uri: &str,
    ) -> super::Result<Option<Box<dyn crate::multipart::MultipartWriter>>> {
        let (source, source_uri) = self.fileset_path_to_source_and_url(uri).await?;
        source.create_multipart_writer(&source_uri).await
    }

    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let (source, source_uri) = self.fileset_path_to_source_and_url(uri).await?;
        source.get(&source_uri, range, io_stats).await
    }

    async fn put(&self, uri: &str, data: Bytes, io_stats: Option<IOStatsRef>) -> super::Result<()> {
        let (source, source_uri) = self.fileset_path_to_source_and_url(uri).await?;
        source.put(&source_uri, data, io_stats).await
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        let (source, source_uri) = self.fileset_path_to_source_and_url(uri).await?;
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
        let (source, source_glob_path) = self.fileset_path_to_source_and_url(glob_path).await?;

        // Extract the gvfs prefix and storage location for path transformation
        // We need to extract the base path without the glob pattern
        // For example: gvfs://fileset/catalog/schema/fileset/**/*.parquet -> gvfs://fileset/catalog/schema/fileset/
        let gvfs_base_path = if glob_path.contains("**") {
            // Find the position before the glob pattern
            if let Some(pos) = glob_path.find("**") {
                glob_path[..pos].to_string()
            } else {
                glob_path.to_string()
            }
        } else if let Some(pos) = glob_path.rfind('/') {
            glob_path[..=pos].to_string() // Include the trailing slash
        } else {
            glob_path.to_string()
        };

        // Get the storage location base path to replace
        let storage_base_path = if source_glob_path.contains("**") {
            // Find the position before the glob pattern
            if let Some(pos) = source_glob_path.find("**") {
                source_glob_path[..pos].to_string()
            } else {
                source_glob_path.clone()
            }
        } else if let Some(pos) = source_glob_path.rfind('/') {
            source_glob_path[..=pos].to_string() // Include the trailing slash
        } else {
            source_glob_path.clone()
        };

        let underlying_stream = source
            .glob(
                &source_glob_path,
                fanout_limit,
                page_size,
                limit,
                io_stats,
                file_format,
            )
            .await?;

        // Transform the file paths from storage format back to gvfs format
        use futures::StreamExt;
        let transformed_stream = underlying_stream.map(move |result| {
            result.map(|mut file_metadata| {
                // Replace the storage path prefix with the gvfs prefix
                if file_metadata.filepath.starts_with(&storage_base_path) {
                    let relative_path = &file_metadata.filepath[storage_base_path.len()..];
                    file_metadata.filepath = format!("{}{}", gvfs_base_path, relative_path);
                }
                file_metadata
            })
        });

        Ok(Box::pin(transformed_stream))
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult> {
        let (source, source_path) = self.fileset_path_to_source_and_url(path).await?;
        source
            .ls(&source_path, posix, continuation_token, page_size, io_stats)
            .await
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

#[cfg(test)]
mod tests {
    use common_io_config::GravitinoConfig;

    use super::*;

    // Tests for invalid_gravitino_path function
    #[test]
    fn test_invalid_gravitino_path_error_message() {
        let path = "invalid://path";
        let error = invalid_gravitino_path(path);

        match error {
            crate::Error::NotFound {
                path: error_path,
                source,
            } => {
                assert_eq!(error_path, path);
                assert!(
                    source
                        .to_string()
                        .contains("Expected Gravitino fileset path")
                );
            }
            _ => panic!("Expected NotFound error"),
        }
    }

    #[test]
    fn test_invalid_gravitino_path_with_various_formats() {
        let test_cases = vec![
            "http://example.com",
            "s3://bucket/key",
            "gvfs://wrong/path",
            "gvfs://fileset",
            "gvfs://fileset/",
            "gvfs://fileset/catalog",
        ];

        for path in test_cases {
            let error = invalid_gravitino_path(path);
            assert!(matches!(error, crate::Error::NotFound { .. }));
        }
    }

    // Tests for GravitinoSource::get_client validation
    #[cfg(feature = "python")]
    #[tokio::test]
    async fn test_get_client_missing_endpoint() {
        let config = GravitinoConfig {
            endpoint: None,
            metalake_name: Some("test_metalake".to_string()),
            auth_type: None,
            username: None,
            password: None,
            token: None,
        };

        let result = GravitinoSource::get_client(&config).await;
        assert!(result.is_err());

        match result {
            Err(crate::Error::UnableToCreateClient { store, .. }) => {
                assert_eq!(store, SourceType::Gravitino);
            }
            _ => panic!("Expected UnableToCreateClient error"),
        }
    }

    #[cfg(feature = "python")]
    #[tokio::test]
    async fn test_get_client_missing_metalake_name() {
        let config = GravitinoConfig {
            endpoint: Some("http://localhost:8090".to_string()),
            metalake_name: None,
            auth_type: None,
            username: None,
            password: None,
            token: None,
        };

        let result = GravitinoSource::get_client(&config).await;
        assert!(result.is_err());

        match result {
            Err(crate::Error::UnableToCreateClient { store, .. }) => {
                assert_eq!(store, SourceType::Gravitino);
            }
            _ => panic!("Expected UnableToCreateClient error"),
        }
    }

    #[cfg(feature = "python")]
    #[tokio::test]
    async fn test_get_client_missing_both_required_fields() {
        let config = GravitinoConfig {
            endpoint: None,
            metalake_name: None,
            auth_type: None,
            username: None,
            password: None,
            token: None,
        };

        let result = GravitinoSource::get_client(&config).await;
        assert!(result.is_err());
    }

    // Tests for URL parsing and validation
    #[test]
    fn test_gravitino_path_parsing_invalid_scheme() {
        // Test with wrong scheme
        let paths = vec![
            "http://fileset/catalog/schema/fileset/path",
            "s3://fileset/catalog/schema/fileset/path",
            "file://fileset/catalog/schema/fileset/path",
        ];

        for path in paths {
            let url = url::Url::parse(path).unwrap();
            assert_ne!(url.scheme(), "gvfs");
        }
    }

    #[test]
    fn test_gravitino_path_parsing_invalid_host() {
        // Test with wrong host
        let paths = vec![
            "gvfs://bucket/catalog/schema/fileset/path",
            "gvfs://table/catalog/schema/fileset/path",
            "gvfs://catalog/catalog/schema/fileset/path",
        ];

        for path in paths {
            let url = url::Url::parse(path).unwrap();
            assert_ne!(url.host_str(), Some("fileset"));
        }
    }

    #[test]
    fn test_gravitino_path_parsing_valid_format() {
        let path = "gvfs://fileset/catalog/schema/fileset/path/to/file.parquet";
        let url = url::Url::parse(path).unwrap();

        assert_eq!(url.scheme(), "gvfs");
        assert_eq!(url.host_str(), Some("fileset"));

        let segments: Vec<&str> = url.path_segments().unwrap().collect();
        assert_eq!(segments.len(), 5);
        assert_eq!(segments[0], "catalog");
        assert_eq!(segments[1], "schema");
        assert_eq!(segments[2], "fileset");
        assert_eq!(segments[3], "path");
        assert_eq!(segments[4], "to");
    }

    #[test]
    fn test_gravitino_path_parsing_minimal_valid_format() {
        let path = "gvfs://fileset/catalog/schema/fileset";
        let url = url::Url::parse(path).unwrap();

        assert_eq!(url.scheme(), "gvfs");
        assert_eq!(url.host_str(), Some("fileset"));

        let segments: Vec<&str> = url.path_segments().unwrap().collect();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0], "catalog");
        assert_eq!(segments[1], "schema");
        assert_eq!(segments[2], "fileset");
    }

    #[test]
    fn test_gravitino_path_parsing_with_glob_pattern() {
        let path = "gvfs://fileset/catalog/schema/fileset/**/*.parquet";
        let url = url::Url::parse(path).unwrap();

        assert_eq!(url.scheme(), "gvfs");
        assert_eq!(url.host_str(), Some("fileset"));

        let segments: Vec<&str> = url.path_segments().unwrap().collect();
        assert!(segments.len() >= 3);
        assert_eq!(segments[0], "catalog");
        assert_eq!(segments[1], "schema");
        assert_eq!(segments[2], "fileset");
    }

    #[test]
    fn test_gravitino_path_segments_extraction() {
        let path = "gvfs://fileset/my_catalog/my_schema/my_fileset/data/file.parquet";
        let url = url::Url::parse(path).unwrap();

        let mut segments = url.path_segments().unwrap();

        let catalog_name = segments.next().unwrap();
        let schema_name = segments.next().unwrap();
        let fileset_name = segments.next().unwrap();
        let remaining: Vec<&str> = segments.collect();

        assert_eq!(catalog_name, "my_catalog");
        assert_eq!(schema_name, "my_schema");
        assert_eq!(fileset_name, "my_fileset");
        assert_eq!(remaining, vec!["data", "file.parquet"]);

        let combined_name = format!("{}.{}.{}", catalog_name, schema_name, fileset_name);
        assert_eq!(combined_name, "my_catalog.my_schema.my_fileset");
    }

    #[test]
    fn test_gravitino_path_without_file_path() {
        let path = "gvfs://fileset/catalog/schema/fileset";
        let url = url::Url::parse(path).unwrap();

        let mut segments = url.path_segments().unwrap();

        let catalog_name = segments.next().unwrap();
        let schema_name = segments.next().unwrap();
        let fileset_name = segments.next().unwrap();
        let remaining: Vec<&str> = segments.collect();

        assert_eq!(catalog_name, "catalog");
        assert_eq!(schema_name, "schema");
        assert_eq!(fileset_name, "fileset");
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_gravitino_path_with_trailing_slash() {
        let path = "gvfs://fileset/catalog/schema/fileset/";
        let url = url::Url::parse(path).unwrap();

        assert_eq!(url.scheme(), "gvfs");
        assert_eq!(url.host_str(), Some("fileset"));

        let segments: Vec<&str> = url
            .path_segments()
            .unwrap()
            .filter(|s| !s.is_empty())
            .collect();
        assert_eq!(segments.len(), 3);
    }

    // Tests for glob path base extraction logic
    #[test]
    fn test_glob_path_base_extraction_with_double_star() {
        let glob_path = "gvfs://fileset/catalog/schema/fileset/**/*.parquet";

        let gvfs_base_path = if glob_path.contains("**") {
            if let Some(pos) = glob_path.find("**") {
                glob_path[..pos].to_string()
            } else {
                glob_path.to_string()
            }
        } else {
            glob_path.to_string()
        };

        assert_eq!(gvfs_base_path, "gvfs://fileset/catalog/schema/fileset/");
    }

    #[test]
    fn test_glob_path_base_extraction_with_single_star() {
        let glob_path = "gvfs://fileset/catalog/schema/fileset/*.parquet";

        let gvfs_base_path = if glob_path.contains("**") {
            if let Some(pos) = glob_path.find("**") {
                glob_path[..pos].to_string()
            } else {
                glob_path.to_string()
            }
        } else if let Some(pos) = glob_path.rfind('/') {
            glob_path[..=pos].to_string()
        } else {
            glob_path.to_string()
        };

        assert_eq!(gvfs_base_path, "gvfs://fileset/catalog/schema/fileset/");
    }

    #[test]
    fn test_glob_path_base_extraction_no_glob() {
        let glob_path = "gvfs://fileset/catalog/schema/fileset/file.parquet";

        let gvfs_base_path = if glob_path.contains("**") {
            if let Some(pos) = glob_path.find("**") {
                glob_path[..pos].to_string()
            } else {
                glob_path.to_string()
            }
        } else if let Some(pos) = glob_path.rfind('/') {
            glob_path[..=pos].to_string()
        } else {
            glob_path.to_string()
        };

        assert_eq!(gvfs_base_path, "gvfs://fileset/catalog/schema/fileset/");
    }

    #[test]
    fn test_glob_path_base_extraction_nested_path() {
        let glob_path = "gvfs://fileset/catalog/schema/fileset/year=2023/month=01/**/*.parquet";

        let gvfs_base_path = if let Some(pos) = glob_path.find("**") {
            glob_path[..pos].to_string()
        } else {
            glob_path.to_string()
        };

        assert_eq!(
            gvfs_base_path,
            "gvfs://fileset/catalog/schema/fileset/year=2023/month=01/"
        );
    }

    // Tests for storage path transformation logic
    #[test]
    fn test_storage_path_transformation() {
        let storage_base_path = "s3://bucket/path/";
        let gvfs_base_path = "gvfs://fileset/catalog/schema/fileset/";
        let file_path = "s3://bucket/path/data/file.parquet";

        if file_path.starts_with(storage_base_path) {
            let relative_path = &file_path[storage_base_path.len()..];
            let transformed_path = format!("{}{}", gvfs_base_path, relative_path);
            assert_eq!(
                transformed_path,
                "gvfs://fileset/catalog/schema/fileset/data/file.parquet"
            );
        }
    }

    #[test]
    fn test_storage_path_transformation_nested() {
        let storage_base_path = "s3://bucket/prefix/";
        let gvfs_base_path = "gvfs://fileset/cat/sch/fs/";
        let file_path = "s3://bucket/prefix/year=2023/month=01/data.parquet";

        if file_path.starts_with(storage_base_path) {
            let relative_path = &file_path[storage_base_path.len()..];
            let transformed_path = format!("{}{}", gvfs_base_path, relative_path);
            assert_eq!(
                transformed_path,
                "gvfs://fileset/cat/sch/fs/year=2023/month=01/data.parquet"
            );
        }
    }

    #[test]
    fn test_storage_path_transformation_no_match() {
        let storage_base_path = "s3://bucket/path/";
        let file_path = "s3://other-bucket/data/file.parquet";

        // Should not transform if prefix doesn't match
        assert!(!file_path.starts_with(storage_base_path));
    }

    // Tests for path segment validation
    #[test]
    fn test_path_segments_insufficient_components() {
        // Test paths with insufficient components
        let insufficient_paths = vec![
            "gvfs://fileset/",
            "gvfs://fileset/catalog",
            "gvfs://fileset/catalog/schema",
        ];

        for path in insufficient_paths {
            let url = url::Url::parse(path).unwrap();
            let segments: Vec<&str> = url.path_segments().unwrap().collect();
            assert!(segments.len() < 3 || segments.iter().any(|s| s.is_empty()));
        }
    }

    #[test]
    fn test_path_segments_with_empty_components() {
        let path = "gvfs://fileset/catalog//fileset/file.parquet";
        let url = url::Url::parse(path).unwrap();
        let segments: Vec<&str> = url.path_segments().unwrap().collect();

        // URL parsing normalizes empty segments
        assert!(segments.contains(&""));
    }

    // Tests for combined name format
    #[test]
    fn test_combined_name_format() {
        let catalog = "my_catalog";
        let schema = "my_schema";
        let fileset = "my_fileset";

        let combined = format!("{}.{}.{}", catalog, schema, fileset);
        assert_eq!(combined, "my_catalog.my_schema.my_fileset");
    }

    #[test]
    fn test_combined_name_format_with_special_characters() {
        // Test with special characters
        let catalog = "catalog-with-dash";
        let schema = "schema_with_underscore";
        let fileset = "fileset.with.dots";

        let combined = format!("{}.{}.{}", catalog, schema, fileset);
        assert_eq!(
            combined,
            "catalog-with-dash.schema_with_underscore.fileset.with.dots"
        );
    }

    #[test]
    fn test_combined_name_format_with_numbers() {
        let catalog = "catalog123";
        let schema = "schema456";
        let fileset = "fileset789";

        let combined = format!("{}.{}.{}", catalog, schema, fileset);
        assert_eq!(combined, "catalog123.schema456.fileset789");
    }

    // Tests for itertools join usage
    #[test]
    fn test_path_segments_join() {
        let segments = vec!["path", "to", "file.parquet"];
        let joined = segments.iter().join("/");
        assert_eq!(joined, "path/to/file.parquet");
    }

    #[test]
    fn test_path_segments_join_empty() {
        let segments: Vec<&str> = vec![];
        let joined = segments.iter().join("/");
        assert_eq!(joined, "");
    }

    #[test]
    fn test_path_segments_join_single() {
        let segments = vec!["file.parquet"];
        let joined = segments.iter().join("/");
        assert_eq!(joined, "file.parquet");
    }
}
