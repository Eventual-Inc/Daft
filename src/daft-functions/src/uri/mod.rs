pub mod download;
pub mod upload;

use daft_dsl::{
    functions::{FunctionModule, ScalarFunction},
    make_literal, ExprRef,
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
            make_literal(max_connections)
                .expect("value is serializable")
                .into(),
            make_literal(raise_error_on_failure)
                .expect("value is serializable")
                .into(),
            make_literal(multi_thread)
                .expect("value is serializable")
                .into(),
            make_literal(io_config)
                .expect("value is serializable")
                .into(),
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
            make_literal(max_connections)
                .expect("value is serializable")
                .into(),
            make_literal(raise_error_on_failure)
                .expect("value is serializable")
                .into(),
            make_literal(multi_thread)
                .expect("value is serializable")
                .into(),
            make_literal(is_single_folder)
                .expect("value is serializable")
                .into(),
            make_literal(io_config)
                .expect("value is serializable")
                .into(),
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
