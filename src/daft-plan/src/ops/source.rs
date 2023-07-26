use daft_dsl::ExprRef;

use crate::plan_schema::PlanSchema;
use crate::source_info::SourceInfo;

pub struct Source {
    /// The schema of the output.
    /// May be a subset of the source data schema.
    pub schema: Box<PlanSchema>,

    /// Information about the source data location.
    pub source_info: Box<dyn SourceInfo>,

    /// Optional number of partitions to return.
    pub into_partitions: Option<usize>,
    /// Optional filters to apply to the source data.
    pub filters: Vec<ExprRef>,
    /// Optional number of rows to read
    pub limit: Option<usize>,
}
