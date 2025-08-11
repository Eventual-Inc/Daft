pub mod download;
pub mod parse;
pub mod upload;

use daft_dsl::functions::FunctionModule;
use download::UrlDownload;
pub use download::UrlDownloadArgs;
use parse::UrlParse;
use upload::UrlUpload;
pub use upload::UrlUploadArgs;

pub struct UriFunctions;

impl FunctionModule for UriFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(UrlDownload);
        parent.add_fn(UrlParse);
        parent.add_fn(UrlUpload);
    }
}
