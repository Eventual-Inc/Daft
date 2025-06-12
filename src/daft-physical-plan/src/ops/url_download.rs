use daft_dsl::ExprRef;
use daft_functions_uri::UrlDownloadArgs;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UrlDownload {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub args: UrlDownloadArgs<ExprRef>,
    pub output_column: String,
}

impl UrlDownload {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        args: UrlDownloadArgs<ExprRef>,
        output_column: String,
    ) -> Self {
        Self {
            input,
            args,
            output_column,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("UrlDownload: {:?}", self.args));
        res
    }
}

crate::impl_default_tree_display!(UrlDownload);
