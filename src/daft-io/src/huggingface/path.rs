use std::{fmt::Display, str::FromStr};

use uuid::Uuid;

use crate::huggingface::error::Error;

#[derive(Clone, Debug, PartialEq)]
pub(super) enum HFRepoType {
    Models,
    Buckets,
    Datasets,
    Spaces,
}

impl FromStr for HFRepoType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "models" | "model" => Ok(Self::Models),
            "buckets" | "bucket" => Ok(Self::Buckets),
            "datasets" | "dataset" => Ok(Self::Datasets),
            "spaces" | "space" => Ok(Self::Spaces),
            _ => Err(Error::InvalidPath {
                path: s.to_string(),
            }),
        }
    }
}

impl HFRepoType {
    pub(super) fn as_str(&self) -> &str {
        match self {
            Self::Models => "models",
            Self::Buckets => "buckets",
            Self::Datasets => "datasets",
            Self::Spaces => "spaces",
        }
    }
}

impl Display for HFRepoType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HFPathParts {
    pub repo_type: HFRepoType,
    pub repository: String,
    pub revision: String,
    pub path: String,
}

impl HFPathParts {
    pub(super) fn resolve_url(&self) -> String {
        match self.repo_type {
            HFRepoType::Buckets => format!(
                "https://huggingface.co/buckets/{}/resolve/{}",
                self.repository, self.path
            ),
            HFRepoType::Models => format!(
                "https://huggingface.co/{}/resolve/{}/{}",
                self.repository, self.revision, self.path
            ),
            HFRepoType::Spaces => {
                format!(
                    "https://huggingface.co/spaces/{}/resolve/{}/{}",
                    self.repository, self.revision, self.path
                )
            }
            HFRepoType::Datasets => format!(
                "https://huggingface.co/datasets/{}/resolve/{}/{}",
                self.repository, self.revision, self.path,
            ),
        }
    }

    /// Parse `https://huggingface.co/.../resolve/...` URLs.
    pub(super) fn from_resolve_url(url: &str) -> Option<Self> {
        let parsed = url::Url::parse(url).ok()?;
        if parsed.host_str()? != "huggingface.co" {
            return None;
        }
        let path = parsed.path().trim_start_matches('/');

        let (repo_type_str, rest) = path.split_once('/')?;
        let repo_type = repo_type_str.parse().ok()?;

        let resolve_marker = "/resolve/";
        let resolve_idx = rest.find(resolve_marker)?;
        let repository = rest[..resolve_idx].to_string();
        let after_resolve = &rest[resolve_idx + resolve_marker.len()..];
        if after_resolve.is_empty() {
            return None;
        }

        let (revision, path) = if repo_type == HFRepoType::Buckets {
            ("main".to_string(), after_resolve.to_string())
        } else {
            let (revision, file_path) = after_resolve.split_once('/')?;
            if file_path.is_empty() {
                return None;
            }
            (revision.to_string(), file_path.to_string())
        };

        Some(Self {
            repo_type,
            repository,
            revision,
            path,
        })
    }
}

/// Extract [`HFPathParts`] from an `hf://` or Hugging Face HTTPS resolve URL.
pub(super) fn hf_path_parts_from_uri(uri: &str) -> Result<Option<HFPathParts>, Error> {
    let path = uri.parse::<HFPath>()?;
    match path {
        HFPath::Hf(parts) if !parts.path.is_empty() => Ok(Some(parts)),
        HFPath::Http(url) => Ok(HFPathParts::from_resolve_url(&url)),
        _ => Ok(None),
    }
}

impl FromStr for HFPathParts {
    type Err = Error;
    /// Extracts path components from a hugging face path:
    /// `hf:// [datasets | spaces] / {username} / {reponame} @ {revision} / {path from root}`
    fn from_str(uri: &str) -> Result<Self, Self::Err> {
        // hf:// [datasets] / {username} / {reponame} @ {revision} / {path from root}
        //       !>
        if !uri.starts_with("hf://") {
            return Err(Error::InvalidPath {
                path: uri.to_string(),
            });
        }
        (|| {
            let uri = &uri[5..];

            // [datasets] / {username} / {reponame} @ {revision} / {path from root}
            // ^--------^   !>
            let (repo_type_str, uri) = uri.split_once('/')?;
            // {username} / {reponame} @ {revision} / {path from root}
            // ^--------^   !>
            let (username, uri) = uri.split_once('/')?;
            // {reponame} @ {revision} / {path from root}
            // ^--------^   !>
            let (repository, uri) = if let Some((repo, uri)) = uri.split_once('/') {
                (repo, uri)
            } else {
                return Some(Self {
                    repo_type: repo_type_str.parse().ok()?,
                    repository: format!("{username}/{uri}"),
                    revision: "main".to_string(),
                    path: String::new(),
                });
            };

            // {revision} / {path from root}
            // ^--------^   !>
            let (repository, revision) = if let Some((repo, rev)) = repository.split_once('@') {
                (repo, rev.to_string())
            } else {
                (repository, "main".to_string())
            };

            // {username}/{reponame}
            let repository = format!("{username}/{repository}");
            // {path from root}
            // ^--------------^
            let path = uri.to_string().trim_end_matches('/').to_string();

            Some(Self {
                repo_type: repo_type_str.parse().ok()?,
                repository,
                revision,
                path,
            })
        })()
        .ok_or_else(|| Error::InvalidPath {
            path: uri.to_string(),
        })
    }
}

impl std::fmt::Display for HFPathParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hf://{REPO_TYPE}/{REPOSITORY}/{PATH}",
            REPO_TYPE = self.repo_type,
            REPOSITORY = self.repository,
            PATH = self.path
        )
    }
}

pub(crate) enum HFPath {
    Http(String),
    Hf(HFPathParts),
}

impl FromStr for HFPath {
    type Err = Error;

    fn from_str(uri: &str) -> Result<Self, Self::Err> {
        if uri.starts_with("http://") || uri.starts_with("https://") {
            Ok(Self::Http(uri.to_string()))
        } else {
            uri.parse().map(Self::Hf)
        }
    }
}

impl HFPath {
    // There is a bug within huggingface apis that is incorrectly caching files
    // https://github.com/huggingface/datasets/issues/7685
    //
    // So to bypass this, we add a unique parameter to the url to prevent CDN caching.
    pub(super) fn get_file_uri(&self, cache_bust: bool) -> String {
        let base = match self {
            Self::Http(base) => base.clone(),
            Self::Hf(parts) => parts.resolve_url(),
        };
        if cache_bust {
            let cachebuster = Uuid::new_v4();
            let cachebuster = cachebuster.to_string();
            if base.contains('?') {
                format!("{base}&cachebust={cachebuster}")
            } else {
                format!("{base}?cachebust={cachebuster}")
            }
        } else {
            base
        }
    }

    pub(super) fn get_api_uri(&self) -> String {
        match self {
            Self::Http(path) => path.clone(),
            Self::Hf(parts) => {
                if parts.repo_type == HFRepoType::Buckets {
                    let path = format!("/{}", parts.path.trim_start_matches('/'));
                    format!(
                        "https://huggingface.co/api/buckets/{REPOSITORY}/tree{PATH}",
                        REPOSITORY = parts.repository,
                        PATH = path,
                    )
                } else {
                    // "https://huggingface.co/api/ [datasets] / {username} / {reponame} / tree / {revision} / {path from root}"
                    format!(
                        "https://huggingface.co/api/{REPO_TYPE}/{REPOSITORY}/tree/{REVISION}/{PATH}",
                        REPO_TYPE = parts.repo_type,
                        REPOSITORY = parts.repository,
                        REVISION = parts.revision,
                        PATH = parts.path,
                    )
                }
            }
        }
    }

    pub(super) fn get_parquet_api_uri(&self) -> String {
        match self {
            Self::Http(path) => path.clone(),
            Self::Hf(parts) => format!(
                "https://huggingface.co/api/{REPO_TYPE}/{REPOSITORY}/parquet",
                REPO_TYPE = parts.repo_type,
                REPOSITORY = parts.repository,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use super::{HFPathParts, HFRepoType};

    #[test]
    fn test_parse_hf_parts() -> DaftResult<()> {
        let uri = "hf://datasets/wikimedia/wikipedia/20231101.ab/*.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            repo_type: HFRepoType::Datasets,
            repository: "wikimedia/wikipedia".to_string(),
            revision: "main".to_string(),
            path: "20231101.ab/*.parquet".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_parse_hf_parts_with_revision() -> DaftResult<()> {
        let uri = "hf://datasets/wikimedia/wikipedia@dev/20231101.ab/*.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            repo_type: HFRepoType::Datasets,
            repository: "wikimedia/wikipedia".to_string(),
            revision: "dev".to_string(),
            path: "20231101.ab/*.parquet".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_parse_hf_parts_with_exact_path() -> DaftResult<()> {
        let uri = "hf://datasets/user/repo@dev/config/my_file.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            repo_type: HFRepoType::Datasets,
            repository: "user/repo".to_string(),
            revision: "dev".to_string(),
            path: "config/my_file.parquet".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_parse_hf_parts_with_wildcard() -> DaftResult<()> {
        let uri = "hf://datasets/wikimedia/wikipedia/**/*.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            repo_type: HFRepoType::Datasets,
            repository: "wikimedia/wikipedia".to_string(),
            revision: "main".to_string(),
            path: "**/*.parquet".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_from_resolve_url_datasets() {
        let url =
            "https://huggingface.co/datasets/google/FACTS-grounding-public/resolve/main/README.md";
        let parts = HFPathParts::from_resolve_url(url).unwrap();
        assert_eq!(parts.repo_type, HFRepoType::Datasets);
        assert_eq!(parts.repository, "google/FACTS-grounding-public");
        assert_eq!(parts.revision, "main");
        assert_eq!(parts.path, "README.md");
    }

    #[test]
    fn test_from_resolve_url_models() {
        let url = "https://huggingface.co/Qwen/Qwen2.5-0.5B/resolve/main/config.json";
        let parts = HFPathParts::from_resolve_url(url).unwrap();
        assert_eq!(parts.repo_type, HFRepoType::Models);
        assert_eq!(parts.repository, "Qwen/Qwen2.5-0.5B");
        assert_eq!(parts.revision, "main");
        assert_eq!(parts.path, "config.json");
    }

    #[test]
    fn test_parse_hf_parts_bucket() -> DaftResult<()> {
        let uri = "hf://buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2026-17/file.warc.gz";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            repo_type: HFRepoType::Buckets,
            repository: "commoncrawl/commoncrawl".to_string(),
            revision: "main".to_string(),
            path: "crawl-data/CC-MAIN-2026-17/file.warc.gz".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_resolve_url_buckets_omits_revision() {
        let parts = HFPathParts {
            repo_type: HFRepoType::Buckets,
            repository: "commoncrawl/commoncrawl".to_string(),
            revision: "main".to_string(),
            path: "crawl-data/CC-MAIN-2026-17/file.warc.gz".to_string(),
        };
        assert_eq!(
            parts.resolve_url(),
            "https://huggingface.co/buckets/commoncrawl/commoncrawl/resolve/crawl-data/CC-MAIN-2026-17/file.warc.gz"
        );
    }

    #[test]
    fn test_from_resolve_url_buckets() {
        let url = "https://huggingface.co/buckets/commoncrawl/commoncrawl/resolve/crawl-data/CC-MAIN-2026-17/file.warc.gz";
        let parts = HFPathParts::from_resolve_url(url).unwrap();
        assert_eq!(parts.repo_type, HFRepoType::Buckets);
        assert_eq!(parts.repository, "commoncrawl/commoncrawl");
        assert_eq!(parts.path, "crawl-data/CC-MAIN-2026-17/file.warc.gz");
    }

    #[test]
    fn test_resolve_url_datasets_omits_duplicate_prefix() {
        let parts = HFPathParts {
            repo_type: HFRepoType::Datasets,
            repository: "open-world-agents/D2E-480p".to_string(),
            revision: "main".to_string(),
            path: "PEAK/recording.mcap".to_string(),
        };
        assert_eq!(
            parts.resolve_url(),
            "https://huggingface.co/datasets/open-world-agents/D2E-480p/resolve/main/PEAK/recording.mcap"
        );
    }

    #[test]
    fn test_resolve_url_models_omits_prefix() {
        let parts = HFPathParts {
            repo_type: HFRepoType::Models,
            repository: "Qwen/Qwen2.5".to_string(),
            revision: "main".to_string(),
            path: "config.json".to_string(),
        };
        assert_eq!(
            parts.resolve_url(),
            "https://huggingface.co/Qwen/Qwen2.5/resolve/main/config.json"
        );
    }
}
