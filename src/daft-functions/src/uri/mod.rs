pub mod download;
pub mod upload;

use daft_dsl::functions::FunctionModule;
use download::UrlDownload;
use upload::UrlUpload;

pub struct UriFunctions;

impl FunctionModule for UriFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(UrlDownload);
        parent.add_fn(UrlUpload);
    }
}
