pub mod download;
pub mod upload;
use std::sync::Arc;

use daft_dsl::{
    functions::{FunctionModule, ScalarFunction},
    lit, ExprRef,
};
use download::{UrlDownload, UrlDownloadArgs};
use upload::{UrlUpload, UrlUploadArgs};

/// Creates a `url_download` ExprRef from the positional and optional named arguments.
#[must_use]
pub fn download(input: ExprRef, args: Option<UrlDownloadArgs>) -> ExprRef {
    let args = if let Some(UrlDownloadArgs {
        max_connections,
        raise_error_on_failure,
        multi_thread,
        io_config,
    }) = args
    {
        vec![
            input,
            lit(max_connections as u64),
            lit(raise_error_on_failure),
            lit(multi_thread),
            lit(Arc::unwrap_or_clone(io_config)),
        ]
    } else {
        vec![input]
    };

    ScalarFunction::new(UrlDownload, args).into()
}

/// Creates a `url_upload` ExprRef from the positional and optional named arguments.
#[must_use]
pub fn upload(input: ExprRef, location: ExprRef, args: Option<UrlUploadArgs>) -> ExprRef {
    let args = if let Some(UrlUploadArgs {
        max_connections,
        raise_error_on_failure,
        multi_thread,
        is_single_folder,
        io_config,
    }) = args
    {
        vec![
            input,
            location,
            lit(max_connections as u64),
            lit(raise_error_on_failure),
            lit(multi_thread),
            lit(is_single_folder),
            lit(Arc::unwrap_or_clone(io_config)),
        ]
    } else {
        vec![input, location]
    };

    ScalarFunction::new(UrlUpload, args).into()
}

pub struct UriFunctions;

impl FunctionModule for UriFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(UrlDownload);
        parent.add_fn(UrlUpload);
    }
}
