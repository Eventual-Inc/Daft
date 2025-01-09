mod download;
mod upload;

use common_io_config::IOConfig;
use daft_dsl::{functions::ScalarFunction, ExprRef};
use download::DownloadFunction;
use upload::UploadFunction;

#[must_use]
pub fn download(
    input: ExprRef,
    max_connections: usize,
    raise_error_on_failure: bool,
    multi_thread: bool,
    config: Option<IOConfig>,
) -> ExprRef {
    ScalarFunction::new(
        DownloadFunction {
            max_connections,
            raise_error_on_failure,
            multi_thread,
            config: config.unwrap_or_default().into(),
        },
        vec![input],
    )
    .into()
}

#[must_use]
pub fn upload(
    input: ExprRef,
    location: ExprRef,
    max_connections: usize,
    raise_error_on_failure: bool,
    multi_thread: bool,
    is_single_folder: bool,
    config: Option<IOConfig>,
) -> ExprRef {
    ScalarFunction::new(
        UploadFunction {
            max_connections,
            raise_error_on_failure,
            multi_thread,
            is_single_folder,
            config: config.unwrap_or_default().into(),
        },
        vec![input, location],
    )
    .into()
}
