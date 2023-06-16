use std::path::PathBuf;

use super::object_io::{GetResult, ObjectSource};
use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use url::ParseError;

pub struct LocalSource {}

#[derive(Debug, Snafu)]
enum Error {
    // #[snafu(display("Generic {} error: {:?}", store, source))]
    // Generic {
    //     store: &'static str,
    //     source: DynError,
    // },

    // #[snafu(display("Object at location {} not found: {:?}", path, source))]
    // NotFound {
    //     path: String,
    //     source: DynError,
    // },

    // #[snafu(display("Invalid Argument: {:?}", msg))]
    // InvalidArgument{msg: String},

    // #[snafu(display("Unable to open file {}: {}", path, s3::error::DisplayErrorContext(source)))]
    // UnableToOpenFile {
    //     path: String,
    //     source: SdkError<GetObjectError, Response>,
    // },

    // #[snafu(display("Unable to read data from file {}: {}", path, source))]
    // UnableToReadBytes {
    //     path: String,
    //     source: ByteStreamError,
    // },
    #[snafu(display("Unable to parse URL \"{}\"", url.to_string_lossy()))]
    InvalidUrl { url: PathBuf, source: ParseError },

    #[snafu(display("Unable to convert URL \"{}\" to path", path))]
    InvalidFilePath { path: String },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        super::Error::Generic {
            store: "local",
            source: error.into(),
        }
    }
}

impl LocalSource {
    pub async fn new() -> Self {
        LocalSource {}
    }
}

#[async_trait]
impl ObjectSource for LocalSource {
    async fn get(&self, uri: &str) -> super::Result<GetResult> {
        let path = url::Url::parse(uri).context(InvalidUrlSnafu { url: uri })?;
        let file_path = match path.to_file_path() {
            Ok(f) => Ok(f),
            Err(_err) => Err(Error::InvalidFilePath {
                path: path.to_string(),
            }),
        }?;
        Ok(GetResult::File(file_path))
    }
}
