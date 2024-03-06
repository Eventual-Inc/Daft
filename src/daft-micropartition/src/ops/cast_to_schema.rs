use std::{collections::HashMap, ops::Deref, sync::Arc};

use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use daft_dsl::Expr;
use daft_scan::ScanTask;

use crate::micropartition::{MicroPartition, TableState};

impl MicroPartition {
    pub fn cast_to_schema(&self, schema: SchemaRef) -> DaftResult<Self> {
        self.cast_to_schema_with_fill(schema, None)
    }

    pub fn cast_to_schema_with_fill(
        &self,
        schema: SchemaRef,
        fill_map: Option<&HashMap<&str, Expr>>,
    ) -> DaftResult<Self> {
        let schema_owned = schema.clone();
        let pruned_statistics = self
            .statistics
            .as_ref()
            .map(|stats| stats.cast_to_schema_with_fill(schema_owned, fill_map))
            .transpose()?;

        let guard = self.state.lock().unwrap();
        match guard.deref() {
            // Replace schema if Unloaded, which should be applied when data is lazily loaded
            TableState::Unloaded(scan_task) => {
                let maybe_new_scan_task = if scan_task.schema == schema {
                    scan_task.clone()
                } else {
                    Arc::new(ScanTask::new(
                        scan_task.sources.clone(),
                        scan_task.file_format_config.clone(),
                        schema,
                        scan_task.storage_config.clone(),
                        scan_task.pushdowns.clone(),
                    ))
                };
                Ok(MicroPartition::new_unloaded(
                    maybe_new_scan_task,
                    self.metadata.clone(),
                    pruned_statistics.expect("Unloaded MicroPartition should have statistics"),
                ))
            }
            // If Tables are already loaded, we map `Table::cast_to_schema` on each Table
            TableState::Loaded(tables) => Ok(MicroPartition::new_loaded(
                schema.clone(),
                Arc::new(
                    tables
                        .iter()
                        .map(|tbl| tbl.cast_to_schema_with_fill(schema.as_ref(), fill_map))
                        .collect::<DaftResult<Vec<_>>>()?,
                ),
                pruned_statistics,
            )),
        }
    }
}
