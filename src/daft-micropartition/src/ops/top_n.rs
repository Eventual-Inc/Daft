use common_error::DaftResult;
use daft_dsl::ExprRef;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn top_n(
        &self,
        sort_keys: &[ExprRef],
        descending: &[bool],
        nulls_first: &[bool],
        limit: usize,
    ) -> DaftResult<Self> {
        self.sort(sort_keys, descending, nulls_first)?.head(limit)
    }
}
