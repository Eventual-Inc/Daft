use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;

use crate::source_info::SourceInfo;

pub struct Source {
    /// The schema of the output of this node (the source data schema).
    /// May be a subset of the source data schema; executors should push down this projection if possible.
    pub schema: SchemaRef,

    /// Information about the source data location.
    pub source_info: Box<SourceInfo>,

    /// Optional filters to apply to the source data.
    pub filters: Vec<ExprRef>,
    /// Optional number of rows to read.
    pub limit: Option<usize>,
}
