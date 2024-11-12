use std::{any::Any, fmt::Debug, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::DisplayAs;
use common_error::DaftResult;
use common_file_formats::FileFormatConfig;
use daft_schema::schema::SchemaRef;

use crate::Pushdowns;

#[typetag::serde(tag = "type")]
pub trait ScanTaskLike: Debug + DisplayAs + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn dyn_eq(&self, other: &dyn ScanTaskLike) -> bool;
    #[must_use]
    fn materialized_schema(&self) -> SchemaRef;
    #[must_use]
    fn num_rows(&self) -> Option<usize>;
    #[must_use]
    fn approx_num_rows(&self, config: Option<&DaftExecutionConfig>) -> Option<f64>;
    #[must_use]
    fn upper_bound_rows(&self) -> Option<usize>;
    #[must_use]
    fn size_bytes_on_disk(&self) -> Option<usize>;
    #[must_use]
    fn estimate_in_memory_size_bytes(&self, config: Option<&DaftExecutionConfig>) -> Option<usize>;
    #[must_use]
    fn file_format_config(&self) -> Arc<FileFormatConfig>;
    #[must_use]
    fn pushdowns(&self) -> &Pushdowns;
    #[must_use]
    fn schema(&self) -> SchemaRef;
}

pub type ScanTaskLikeRef = Arc<dyn ScanTaskLike>;

impl PartialEq for dyn ScanTaskLike + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other)
    }
}

pub type BoxScanTaskLikeIter = Box<dyn Iterator<Item = DaftResult<Arc<dyn ScanTaskLike>>>>;
