use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    #[deprecated(note = "name-referenced columns")]
    /// Casts a `MicroPartition` to a schema.
    ///
    /// Note: this method is deprecated because it maps fields by name, which will not work for schemas with duplicate field names.
    /// It should only be used for scans, and once we support reading files with duplicate column names, we should remove this function.
    pub fn cast_to_schema(&self, schema: SchemaRef) -> DaftResult<Self> {
        let pruned_statistics = self
            .statistics
            .as_ref()
            .map(|stats| {
                #[allow(deprecated)]
                stats.cast_to_schema(&schema)
            })
            .transpose()?;

        Ok(Self::new_loaded(
            schema.clone(),
            Arc::new(
                self.chunks
                    .iter()
                    .map(|tbl| {
                        #[allow(deprecated)]
                        tbl.cast_to_schema(schema.as_ref())
                    })
                    .collect::<DaftResult<Vec<_>>>()?,
            ),
            pruned_statistics,
        ))
    }
}
