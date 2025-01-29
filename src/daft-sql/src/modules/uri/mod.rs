use super::SQLModule;
use crate::functions::SQLFunctions;

mod url_download;
mod url_upload;

pub struct SQLModuleUri;

impl SQLModule for SQLModuleUri {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("url_download", url_download::SqlUrlDownload);
        parent.add_fn("url_upload", url_upload::SqlUrlUpload);
    }
}
