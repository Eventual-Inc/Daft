pub mod download;
pub mod upload;

use daft_dsl::{functions::ScalarFunction, ExprRef};
use download::UrlDownloadArgs;
use upload::UrlUploadArgs;

/// Creates a `url_download` ExprRef from the positional and optional named arguments.
#[must_use]
pub fn download(input: ExprRef, args: Option<UrlDownloadArgs>) -> ExprRef {
    ScalarFunction::new(args.unwrap_or_default(), vec![input]).into()
}

/// Creates a `url_upload` ExprRef from the positional and optional named arguments.
#[must_use]
pub fn upload(input: ExprRef, location: ExprRef, args: Option<UrlUploadArgs>) -> ExprRef {
    ScalarFunction::new(args.unwrap_or_default(), vec![input, location]).into()
}
