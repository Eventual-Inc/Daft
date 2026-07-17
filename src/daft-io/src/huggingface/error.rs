use std::{num::ParseIntError, string::FromUtf8Error};

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum Error {
    #[snafu(display("Unable to connect to {}: {}", path, source))]
    UnableToConnect {
        path: String,
        source: reqwest_middleware::Error,
    },

    #[snafu(display("Unable to open {}: {}", path, source))]
    UnableToOpenFile {
        path: String,
        source: reqwest_middleware::reqwest::Error,
    },

    #[snafu(display("Unable to determine size of {}", path))]
    UnableToDetermineSize { path: String },

    #[snafu(display("Unable to read data from {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: reqwest_middleware::reqwest::Error,
    },

    #[snafu(display(
        "Unable to parse data as Utf8 while reading header for file: {path}. {source}"
    ))]
    UnableToParseUtf8Header { path: String, source: FromUtf8Error },

    #[snafu(display(
        "Unable to parse data as Integer while reading header for file: {path}. {source}"
    ))]
    UnableToParseInteger { path: String, source: ParseIntError },

    #[snafu(display("Invalid path: {}", path))]
    InvalidPath { path: String },

    #[snafu(display(r"
Implicit Parquet conversion not supported for private datasets.
You can use glob patterns, or request a specific file to access your dataset instead.
Example:
    instead of `hf://datasets/username/dataset_name`, use `hf://datasets/username/dataset_name/file_name.parquet`
    or `hf://datasets/username/dataset_name/*.parquet
"))]
    PrivateDataset,

    #[snafu(display("Unauthorized access to dataset, please check your credentials."))]
    Unauthorized,

    #[snafu(display(
        "Unauthorized response (HTTP 401) from Hugging Face bucket '{repository}'.
Hugging Face returns 401 both for private buckets and for buckets that do not exist.
Note that storage buckets are separate from datasets: a dataset named '{repository}' is not automatically available as a bucket, so check that https://huggingface.co/buckets/{repository} exists.
To read a dataset, use 'hf://datasets/{repository}/...' instead.
If the bucket exists but is private, provide a token via `HuggingFaceConfig(token=...)`."
    ))]
    BucketUnauthorized { repository: String },

    #[snafu(display("Xet error while accessing {}: {}", path, message))]
    XetOperationFailed { path: String, message: String },
}
