use daft_dsl::ExprRef;
use daft_functions_uri::UrlUploadArgs;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UrlUpload {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub args: UrlUploadArgs<ExprRef>,
    pub output_column: String,
}

impl UrlUpload {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        args: UrlUploadArgs<ExprRef>,
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
        res.push(format!("UrlUpload: {:?}", self.args));
        res
    }
}

crate::impl_default_tree_display!(UrlUpload);
