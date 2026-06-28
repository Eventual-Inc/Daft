use std::str::FromStr;

use uuid::Uuid;

use crate::huggingface::error::Error;


#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HFPathParts {
    pub bucket: String,
    pub repository: String,
    pub revision: String,
    pub path: String,
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
            let (bucket, uri) = uri.split_once('/')?;
            // {username} / {reponame} @ {revision} / {path from root}
            // ^--------^   !>
            let (username, uri) = uri.split_once('/')?;
            // {reponame} @ {revision} / {path from root}
            // ^--------^   !>
            let (repository, uri) = if let Some((repo, uri)) = uri.split_once('/') {
                (repo, uri)
            } else {
                return Some(Self {
                    bucket: bucket.to_string(),
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
                bucket: bucket.to_string(),
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
            "hf://{BUCKET}/{REPOSITORY}/{PATH}",
            BUCKET = self.bucket,
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
            Self::Hf(parts) => {
                format!(
                    "https://huggingface.co/{BUCKET}/{REPOSITORY}/resolve/{REVISION}/{PATH}",
                    BUCKET = parts.bucket,
                    REPOSITORY = parts.repository,
                    REVISION = parts.revision,
                    PATH = parts.path,
                )
            }
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
                // "https://huggingface.co/api/ [datasets] / {username} / {reponame} / tree / {revision} / {path from root}"
                format!(
                    "https://huggingface.co/api/{BUCKET}/{REPOSITORY}/tree/{REVISION}/{PATH}",
                    BUCKET = parts.bucket,
                    REPOSITORY = parts.repository,
                    REVISION = parts.revision,
                    PATH = parts.path,
                )
            }
        }
    }

    pub(super) fn get_parquet_api_uri(&self) -> String {
        match self {
            Self::Http(path) => path.clone(),
            Self::Hf(parts) => format!(
                "https://huggingface.co/api/{BUCKET}/{REPOSITORY}/parquet",
                BUCKET = parts.bucket,
                REPOSITORY = parts.repository,
            ),
        }
    }
}


#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use super::HFPathParts;

    #[test]
    fn test_parse_hf_parts() -> DaftResult<()> {
        let uri = "hf://datasets/wikimedia/wikipedia/20231101.ab/*.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            bucket: "datasets".to_string(),
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
            bucket: "datasets".to_string(),
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
            bucket: "datasets".to_string(),
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
            bucket: "datasets".to_string(),
            repository: "wikimedia/wikipedia".to_string(),
            revision: "main".to_string(),
            path: "**/*.parquet".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }
}
